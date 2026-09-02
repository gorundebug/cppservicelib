/*
 * Service-wide lifecycle registry.
 * Go analog: runtime.ServiceApp.
 */
#pragma once

#include <array>
#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <userver/engine/async.hpp>
#include <userver/engine/deadline.hpp>
#include <userver/engine/future_status.hpp>
#include <userver/engine/task/cancel.hpp>
#include <userver/engine/task/task_with_result.hpp>
#include <userver/engine/wait_all_checked.hpp>

#include <servicelib/runtime/context.hpp>
#include <servicelib/runtime/environment.hpp>
#include <servicelib/runtime/pool/delaypool.hpp>
#include <servicelib/runtime/pool/prioritytaskpool.hpp>
#include <servicelib/runtime/pool/taskpool.hpp>

namespace servicelib {

enum class ServiceComponentKind : std::size_t {
  kDataSource,
  kDataSink,
  kStorage,
  kDelayPool,
  kTaskPool,
  kPriorityTaskPool,
  kComponent,
  kCount,
};

// Owns registered runtime objects and applies the same lifecycle boundary as
// Go ServiceApp. Service-specific generated code constructs the graph and
// registers objects; it never starts individual endpoints or pools.
class ServiceLifecycle final {
 public:
  ServiceLifecycle() = default;
  ServiceLifecycle(const ServiceLifecycle&) = delete;
  ServiceLifecycle& operator=(const ServiceLifecycle&) = delete;

  template <typename T>
  void add(ServiceComponentKind kind, std::shared_ptr<T> component,
           metrics::Metrics* telemetryMetrics = nullptr,
           log::Logger* telemetryLogger = nullptr) {
    if (!component) {
      throw std::invalid_argument("registered service component is null");
    }
    if (state_ != State::kCreated) {
      throw std::logic_error(
          "service components must be registered before start");
    }

    auto& entries = entries_[index(kind)];
    const void* identity = component.get();
    for (const auto& entry : entries) {
      if (entry.identity == identity) {
        throw std::logic_error("service component is already registered");
      }
    }
    auto onStopTimeout = makeStopTimeoutCallback(
        kind, *component, entries.size(), telemetryMetrics, telemetryLogger);
    entries.push_back(Entry{
        .name = componentName(kind, *component, entries.size()),
        .identity = identity,
        .owner = component,
        .start = [component](
                     Context context) { component->start(std::move(context)); },
        .stop = [component](
                    Context context) { component->stop(std::move(context)); },
        .onStopTimeout = std::move(onStopTimeout)});
  }

  void start(Context context) {
    if (state_ != State::kCreated) {
      throw std::logic_error("service lifecycle is already started");
    }

    state_ = State::kStarting;
    try {
      for (const auto kind : kStartOrder) {
        for (auto& entry : entries_[index(kind)]) {
          entry.start(context);
          started_.push_back(&entry);
        }
      }
      state_ = State::kRunning;
    } catch (...) {
      const auto error = std::current_exception();
      stopStarted(context);
      state_ = State::kStopped;
      std::rethrow_exception(error);
    }
  }

  void stop(Context context,
            log::Logger& logger = log::NoopLogger::instance()) {
    stopBeforeGraphDrain(context, logger);
    stopAfterGraphDrain(context, logger);
  }

  void stopBeforeGraphDrain(
      Context context, log::Logger& logger = log::NoopLogger::instance()) {
    if (state_ == State::kCreated) {
      state_ = State::kStopped;
      clear();
      return;
    }
    if (state_ == State::kStopped) return;
    if (state_ != State::kRunning) {
      throw std::logic_error("service lifecycle transition is in progress");
    }

    state_ = State::kStopping;
    // Close and drain every root producer before touching the graph
    // executors. A source that was accepted before shutdown may still enqueue
    // work into a task pool while its stop() is draining.
    std::vector<Entry*> admission;
    appendReverse(admission,
                  entries_[index(ServiceComponentKind::kDataSource)]);
    appendReverse(admission,
                  entries_[index(ServiceComponentKind::kComponent)]);
    stopPhase(context, logger, admission);

    // Pools and timers are graph-work producers. Drain them completely before
    // StreamExecutionEnvironment checks the input/parallel counters; otherwise
    // a pool task may create a ParallelCall after the counter was observed at
    // zero.
    std::vector<Entry*> executors;
    appendReverse(executors,
                  entries_[index(ServiceComponentKind::kPriorityTaskPool)]);
    appendReverse(executors,
                  entries_[index(ServiceComponentKind::kTaskPool)]);
    appendReverse(executors,
                  entries_[index(ServiceComponentKind::kDelayPool)]);
    appendReverse(executors, entries_[index(ServiceComponentKind::kStorage)]);
    stopPhase(context, logger, executors);
  }

  void stopAfterGraphDrain(
      Context context, log::Logger& logger = log::NoopLogger::instance()) {
    if (state_ == State::kStopped) return;
    if (state_ != State::kStopping) {
      throw std::logic_error(
          "service lifecycle graph drain phase has not started");
    }
    std::vector<Entry*> sinks;
    appendReverse(sinks, entries_[index(ServiceComponentKind::kDataSink)]);
    stopPhase(context, logger, sinks);

    state_ = State::kStopped;
    clear();
  }

 private:
  enum class State { kCreated, kStarting, kRunning, kStopping, kStopped };

  struct Entry final {
    std::string name;
    const void* identity{};
    std::shared_ptr<void> owner;
    std::function<void(Context)> start;
    std::function<void(Context)> stop;
    std::function<void()> onStopTimeout;
  };

  static constexpr std::size_t index(ServiceComponentKind kind) noexcept {
    return static_cast<std::size_t>(kind);
  }

  static constexpr std::string_view kindName(
      ServiceComponentKind kind) noexcept {
    switch (kind) {
      case ServiceComponentKind::kDataSource:
        return "datasource";
      case ServiceComponentKind::kDataSink:
        return "datasink";
      case ServiceComponentKind::kStorage:
        return "storage";
      case ServiceComponentKind::kDelayPool:
        return "delay_pool";
      case ServiceComponentKind::kTaskPool:
        return "task_pool";
      case ServiceComponentKind::kPriorityTaskPool:
        return "priority_task_pool";
      case ServiceComponentKind::kComponent:
        return "component";
      case ServiceComponentKind::kCount:
        break;
    }
    return "unknown";
  }

  template <typename T>
  static std::string componentName(ServiceComponentKind kind,
                                   const T& component, std::size_t index) {
    const auto prefix = std::string(kindName(kind)) + ":";
    if constexpr (requires { component.getName(); }) {
      return prefix + std::string(component.getName());
    } else if constexpr (requires { component.id(); }) {
      return prefix + std::to_string(component.id());
    } else {
      return prefix + std::to_string(index);
    }
  }

  template <typename T>
  static std::string connectorName(const T& component, std::size_t index) {
    if constexpr (requires { component.getName(); }) {
      return std::string(component.getName());
    } else if constexpr (requires { component.config().name; }) {
      return std::string(component.config().name);
    } else if constexpr (requires { component.id(); }) {
      return std::to_string(component.id());
    } else {
      return std::to_string(index);
    }
  }

  template <typename T>
  static std::function<void()> makeStopTimeoutCallback(
      ServiceComponentKind kind, const T& component, std::size_t index,
      metrics::Metrics* telemetryMetrics, log::Logger* telemetryLogger) {
    if (!telemetryMetrics || !telemetryLogger ||
        (kind != ServiceComponentKind::kDataSource &&
         kind != ServiceComponentKind::kDataSink)) {
      return {};
    }

    const bool isSource = kind == ServiceComponentKind::kDataSource;
    auto connector = connectorName(component, index);
    auto scope = telemetryMetrics->scope(
        isSource ? "datasource_connector" : "datasink_connector",
        {{"connector", connector}});
    auto counter = scope->counter(
        "events_total",
        isSource ? "Total number of events in data source connector"
                 : "Total number of events in data sink connector",
        {{"event", "stop_timeout"}});
    auto sharedCounter =
        std::shared_ptr<metrics::Int64Counter>(std::move(counter));

    return [isSource, connector = std::move(connector), telemetryLogger,
            counter = std::move(sharedCounter)]() noexcept {
      try {
        telemetryLogger->warn(isSource ? "data source stopped by timeout"
                                       : "data sink stopped by timeout",
                              {log::Field::Str("name", connector)});
      } catch (...) {
        // Telemetry must never interrupt lifecycle cleanup.
      }
      try {
        counter->inc();
      } catch (...) {
        // Telemetry must never interrupt lifecycle cleanup.
      }
    };
  }

  static void appendReverse(std::vector<Entry*>& target,
                            std::vector<Entry>& entries) {
    for (auto it = entries.rbegin(); it != entries.rend(); ++it) {
      target.push_back(&*it);
    }
  }

  static void logFailure(log::Logger& logger, std::string_view message,
                         const Entry& entry,
                         std::string_view error = {}) noexcept {
    try {
      if (error.empty()) {
        logger.warn(message, {log::Field::Str("resource", entry.name)});
      } else {
        logger.warn(message, {log::Field::Str("resource", entry.name),
                              log::Field::Err(error)});
      }
    } catch (...) {
      // Telemetry must never interrupt lifecycle cleanup.
    }
  }

  static void stopPhase(Context context, log::Logger& logger,
                        const std::vector<Entry*>& entries) {
    if (entries.empty()) return;

    userver::engine::TaskCancellationBlocker cancellationBlocker;
    std::vector<userver::engine::TaskWithResult<std::exception_ptr>> tasks;
    tasks.reserve(entries.size());
    for (auto* entry : entries) {
      auto stop = entry->stop;
      auto owner = entry->owner;
      tasks.push_back(userver::engine::AsyncNoTracing(
          [stop = std::move(stop), owner = std::move(owner),
           context]() mutable -> std::exception_ptr {
            static_cast<void>(owner);
            try {
              stop(std::move(context));
              return {};
            } catch (...) {
              return std::current_exception();
            }
          }));
    }

    userver::engine::FutureStatus status =
        userver::engine::FutureStatus::kReady;
    if (context.deadline()) {
      status = userver::engine::WaitAllCheckedUntil(
          userver::engine::Deadline::FromTimePoint(*context.deadline()), tasks);
    } else {
      userver::engine::WaitAllChecked(tasks);
    }

    if (status != userver::engine::FutureStatus::kReady) {
      for (std::size_t i = 0; i < tasks.size(); ++i) {
        if (!tasks[i].IsFinished()) {
          if (entries[i]->onStopTimeout) {
            entries[i]->onStopTimeout();
          } else {
            logFailure(logger, "service shutdown operation timed out",
                       *entries[i]);
          }
        }
      }
    }

    for (std::size_t i = 0; i < tasks.size(); ++i) {
      if (!tasks[i].IsFinished()) {
        // The service-wide shutdown deadline is an upper bound, not a
        // diagnostic followed by an unbounded join. The task owns both the
        // stop callable and its component until it completes, so detaching it
        // cannot observe the cleared lifecycle registry.
        userver::engine::DetachUnscopedUnsafe(
            std::move(tasks[i]).AsTask());
        continue;
      }
      const auto error = tasks[i].Get();
      if (!error) continue;
      try {
        std::rethrow_exception(error);
      } catch (const std::exception& ex) {
        logFailure(logger, "service shutdown operation failed", *entries[i],
                   ex.what());
      } catch (...) {
        logFailure(logger, "service shutdown operation failed", *entries[i],
                   "unknown exception");
      }
    }
  }

  void stopStarted(Context context) noexcept {
    for (auto it = started_.rbegin(); it != started_.rend(); ++it) {
      try {
        (*it)->stop(context);
      } catch (...) {
      }
    }
    clear();
  }

  void clear() noexcept {
    started_.clear();
    for (auto& entries : entries_) entries.clear();
  }

  inline static constexpr std::array kStartOrder{
      ServiceComponentKind::kStorage,
      ServiceComponentKind::kDelayPool,
      ServiceComponentKind::kTaskPool,
      ServiceComponentKind::kPriorityTaskPool,
      ServiceComponentKind::kComponent,
      ServiceComponentKind::kDataSink,
      ServiceComponentKind::kDataSource};

  // Stop admission first. Sinks stop last so already accepted source work can
  // still flush results, matching Go ServiceApp's two-phase shutdown.
  inline static constexpr std::array kStopOrder{
      ServiceComponentKind::kDataSource,       ServiceComponentKind::kComponent,
      ServiceComponentKind::kPriorityTaskPool, ServiceComponentKind::kTaskPool,
      ServiceComponentKind::kDelayPool,        ServiceComponentKind::kStorage,
      ServiceComponentKind::kDataSink};

  std::array<std::vector<Entry>,
             static_cast<std::size_t>(ServiceComponentKind::kCount)>
      entries_;
  std::vector<Entry*> started_;
  State state_{State::kCreated};
};

// The stream execution environment contains only graph/runtime mechanics.
// ServiceApp adds the service-wide ownership and lifecycle boundary represented
// by runtime.ServiceApp in Go. Generated services derive from this class.
template <typename TService, typename TDataTypeFactory>
class ServiceApp
    : public ServiceExecutionEnvironment<TService, TDataTypeFactory>,
      public status::Provider {
 public:
  using ExecutionEnvironment =
      servicelib::ServiceExecutionEnvironment<TService, TDataTypeFactory>;

  pool::ITaskPool* getTaskPool(const std::string& name) override {
    ensureConfiguredPools();
    const auto found = taskPools_.find(name);
    return found == taskPools_.end() ? nullptr : found->second.get();
  }

  pool::IPriorityTaskPool* getPriorityTaskPool(
      const std::string& name) override {
    ensureConfiguredPools();
    const auto found = priorityTaskPools_.find(name);
    return found == priorityTaskPools_.end() ? nullptr : found->second.get();
  }

  void delay(Context context, pool::IDelayPool::Duration duration,
             std::function<void()> task) override {
    if (!delayPool_) {
      throw std::logic_error("service delay pool is not registered");
    }
    delayPool_->delay(std::move(context), duration, std::move(task));
  }

  template <typename T>
  void registerDataSource(std::shared_ptr<T> source) {
    lifecycle_.add(ServiceComponentKind::kDataSource, std::move(source),
                   &this->getMetrics(), &this->getLogger());
  }

  template <typename T>
  void registerDataSink(std::shared_ptr<T> sink) {
    lifecycle_.add(ServiceComponentKind::kDataSink, std::move(sink),
                   &this->getMetrics(), &this->getLogger());
  }

  template <typename T>
  void registerStorage(std::shared_ptr<T> storage) {
    lifecycle_.add(ServiceComponentKind::kStorage, std::move(storage));
  }

  void registerDelayPool(std::shared_ptr<pool::IDelayPool> pool) {
    if (delayPool_) throw std::logic_error("delay pool is already registered");
    delayPool_ = pool;
    lifecycle_.add(ServiceComponentKind::kDelayPool, std::move(pool));
  }

  void registerTaskPool(std::shared_ptr<pool::ITaskPool> pool) {
    if (!pool) throw std::invalid_argument("task pool is null");
    const auto name = pool->getName();
    if (!taskPools_.emplace(name, pool).second) {
      throw std::logic_error("duplicate task pool: " + name);
    }
    lifecycle_.add(ServiceComponentKind::kTaskPool, std::move(pool));
  }

  void registerPriorityTaskPool(std::shared_ptr<pool::IPriorityTaskPool> pool) {
    if (!pool) throw std::invalid_argument("priority task pool is null");
    const auto name = pool->getName();
    if (!priorityTaskPools_.emplace(name, pool).second) {
      throw std::logic_error("duplicate priority task pool: " + name);
    }
    lifecycle_.add(ServiceComponentKind::kPriorityTaskPool, std::move(pool));
  }

  template <typename T>
  void addComponent(std::shared_ptr<T> component) {
    lifecycle_.add(ServiceComponentKind::kComponent, std::move(component));
  }

  void start(Context context = {}) {
    if (running_) throw std::logic_error("service is already started");

    ensureConfiguredPools();
    this->startExecutionRuntime();
    try {
      status::Registry::Register(*this);
      lifecycle_.start(std::move(context));
      running_ = true;
    } catch (...) {
      status::Registry::Unregister(*this);
      this->stopExecutionRuntime();
      releaseOwnedRuntimeObjects();
      throw;
    }
  }

  void stop(Context context = {}) {
    if (!running_) return;
    running_ = false;
    status::Registry::Unregister(*this);

    std::exception_ptr lifecycleError;
    try {
      if (const auto service = this->getServiceConfigSnapshot();
          service && service->shutdownTimeout > 0) {
        context = context.bounded(
            std::chrono::milliseconds{service->shutdownTimeout});
      }
      lifecycle_.stopBeforeGraphDrain(context, this->getLogger());
    } catch (...) {
      lifecycleError = std::current_exception();
    }
    const bool graphDrained = this->drainExecutionRuntime(context);
    if (!graphDrained) {
      try {
        this->getLogger().warn("service graph drain timed out");
      } catch (...) {
      }
    }
    try {
      lifecycle_.stopAfterGraphDrain(context, this->getLogger());
    } catch (...) {
      if (!lifecycleError) lifecycleError = std::current_exception();
    }
    if (graphDrained) {
      this->releaseExecutionRuntime();
    } else {
      // In-flight work still owns graph callbacks. Preserve their targets
      // after the service-wide shutdown deadline instead of freeing them.
      this->abandonExecutionRuntime();
    }
    releaseOwnedRuntimeObjects();
    if (lifecycleError) std::rethrow_exception(lifecycleError);
  }

  bool isRunning() const noexcept { return running_; }

  std::string networkDataJson() const override {
    return this->makeStatusNetworkDataJson();
  }

  std::string graphYaml() const override { return this->makeStatusGraphYaml(); }

 protected:
  ServiceApp() = default;
  ~ServiceApp() = default;

 private:
  void ensureConfiguredPools() {
    if (!delayPool_) {
      registerDelayPool(
          std::shared_ptr<pool::IDelayPool>(pool::makeDelayPool(*this)));
    }

    const auto runtimeConfig = this->getRuntimeConfigSnapshot();
    if (!runtimeConfig) {
      throw std::logic_error("runtime config is not published");
    }

    const auto ensureCallSemantics =
        [this](const config::CallSemanticsGroup& semantics) {
          if (semantics.taskPool.has_value()) {
            const auto& name = semantics.taskPool->poolName;
            if (name.empty()) {
              throw std::invalid_argument(
                  "task pool call semantics requires poolName");
            }
            if (!taskPools_.contains(name)) {
              registerTaskPool(std::shared_ptr<pool::ITaskPool>(
                  pool::makeTaskPool(name, *this)));
            }
          }

          if (semantics.priorityTaskPool.has_value()) {
            const auto& name = semantics.priorityTaskPool->poolName;
            if (name.empty()) {
              throw std::invalid_argument(
                  "priority task pool call semantics requires poolName");
            }
            if (!priorityTaskPools_.contains(name)) {
              registerPriorityTaskPool(std::shared_ptr<pool::IPriorityTaskPool>(
                  pool::makePriorityTaskPool(name, *this)));
            }
          }
        };

    if (const auto service = this->getServiceConfigSnapshot();
        service && service->defaultCallSemantics.has_value()) {
      ensureCallSemantics(*service->defaultCallSemantics);
    }
    for (const auto* link : runtimeConfig->GetConfig().GetLinks()) {
      if (link && link->callSemantics.has_value()) {
        ensureCallSemantics(*link->callSemantics);
      }
    }
  }

  void releaseOwnedRuntimeObjects() noexcept {
    delayPool_.reset();
    taskPools_.clear();
    priorityTaskPools_.clear();
  }

  ServiceLifecycle lifecycle_;
  std::shared_ptr<pool::IDelayPool> delayPool_;
  std::unordered_map<std::string, std::shared_ptr<pool::ITaskPool>> taskPools_;
  std::unordered_map<std::string, std::shared_ptr<pool::IPriorityTaskPool>>
      priorityTaskPools_;
  bool running_{false};
};

}  // namespace servicelib

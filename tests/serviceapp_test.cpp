#include <algorithm>
#include <atomic>
#include <chrono>
#include <concepts>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <userver/engine/sleep.hpp>
#include <userver/utest/utest.hpp>

#include <servicelib/runtime/serviceapp.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

namespace {

struct ServiceDataTypes final {
  template <typename>
  struct DataType {};
};

class Service final : public servicelib::ServiceApp<Service, ServiceDataTypes> {
};

class PriorityPoolConfig final : public servicelib::config::IConfig {
 public:
  PriorityPoolConfig()
      : pool_{.name = "Default Pool", .executorsCount = 1},
        link_{.from = 1,
              .to = 2,
              .callSemantics = servicelib::config::MakeCallSemanticsGroup(
                  servicelib::api::CallSemantics::kPriorityTaskPool,
                  "Default Pool", 1)} {}

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams()
      const override {
    return {};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {};
  }
  std::vector<const servicelib::config::PoolConfig*> GetPools()
      const override {
    return {&pool_};
  }
  std::vector<const servicelib::config::LinkConfig*> GetLinks()
      const override {
    return {&link_};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override {
    return {};
  }
  std::vector<const servicelib::config::TypeConfig*> GetTypes()
      const override {
    return {};
  }

 private:
  servicelib::config::PoolConfig pool_;
  servicelib::config::LinkConfig link_;
};

static_assert(std::derived_from<servicelib::IRuntimeEnvironment,
                                servicelib::IServiceEnvironment>);
static_assert(std::derived_from<Service, servicelib::IRuntimeEnvironment>);
static_assert(std::derived_from<Service, servicelib::IServiceEnvironment>);
static_assert(
    std::derived_from<Service, servicelib::ServiceExecutionEnvironment<
                                   Service, ServiceDataTypes>>);

struct EventLog final {
  void add(std::string event) {
    std::lock_guard lock(mutex);
    events.push_back(std::move(event));
  }

  std::vector<std::string> snapshot() const {
    std::lock_guard lock(mutex);
    return events;
  }

  mutable std::mutex mutex;
  std::vector<std::string> events;
};

struct Component final {
  std::string name;
  EventLog* events{};
  bool failStart{};
  bool failStop{};
  std::chrono::milliseconds stopDelay{};
  std::atomic<bool>* stopped{};

  void start(servicelib::Context) {
    events->add(name + ":start");
    if (failStart) throw std::runtime_error("start failed");
  }

  void stop(servicelib::Context) {
    if (stopDelay > std::chrono::milliseconds::zero()) {
      userver::engine::SleepFor(stopDelay);
    }
    events->add(name + ":stop");
    if (stopped) stopped->store(true);
    if (failStop) throw std::runtime_error("stop failed");
  }
};

struct RecordingLogger final : servicelib::log::Logger {
  struct Record final {
    std::string message;
    std::string resource;
    std::string error;
  };

  void debug(std::string_view,
             std::initializer_list<servicelib::log::Field>) override {}
  void info(std::string_view,
            std::initializer_list<servicelib::log::Field>) override {}
  void error(std::string_view,
             std::initializer_list<servicelib::log::Field>) override {}
  void warn(std::string_view message,
            std::initializer_list<servicelib::log::Field> fields) override {
    Record record{.message = std::string(message), .resource = {}, .error = {}};
    for (const auto& field : fields) {
      if (field.key() == "resource") {
        record.resource = field.stringValue();
      } else if (field.key() == "error") {
        record.error = field.stringValue();
      }
    }
    records.push_back(std::move(record));
  }

  std::vector<Record> records;
};

UTEST(ServiceLifecycle, StartsAndStopsInServiceAppOrder) {
  EventLog events;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kStorage,
                std::make_shared<Component>("storage", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kTaskPool,
                std::make_shared<Component>("task", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kPriorityTaskPool,
                std::make_shared<Component>("priority", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kComponent,
                std::make_shared<Component>("component", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSink,
                std::make_shared<Component>("sink", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDelayPool,
                std::make_shared<Component>("delay", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource,
                std::make_shared<Component>("source", &events));

  lifecycle.start(servicelib::Context{});
  lifecycle.stopBeforeGraphDrain(servicelib::Context{});

  const auto beforeGraphDrain = events.snapshot();
  EXPECT_EQ(std::find(beforeGraphDrain.begin(), beforeGraphDrain.end(),
                      "sink:stop"),
            beforeGraphDrain.end());

  lifecycle.stopAfterGraphDrain(servicelib::Context{});

  const auto recorded = events.snapshot();
  ASSERT_EQ(recorded.size(), 14);
  EXPECT_EQ(
      std::vector(recorded.begin(), recorded.begin() + 7),
      (std::vector<std::string>{"storage:start", "delay:start", "task:start",
                                "priority:start", "component:start",
                                "sink:start", "source:start"}));
  EXPECT_EQ(recorded.back(), "sink:stop");
  EXPECT_NE(std::find(recorded.begin() + 7, recorded.end(), "source:stop"),
            recorded.end());
  EXPECT_NE(std::find(recorded.begin() + 7, recorded.end(), "delay:stop"),
            recorded.end());
  EXPECT_LT(std::find(recorded.begin() + 7, recorded.end(), "source:stop"),
            std::find(recorded.begin() + 7, recorded.end(), "delay:stop"));
}

UTEST(ServiceLifecycle, RollsBackAlreadyStartedComponents) {
  EventLog events;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource,
                std::make_shared<Component>("source", &events, true));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSink,
                std::make_shared<Component>("sink", &events));

  EXPECT_THROW(lifecycle.start(servicelib::Context{}), std::runtime_error);
  EXPECT_EQ(
      events.snapshot(),
      (std::vector<std::string>{"sink:start", "source:start", "source:stop", "sink:stop"}));
}

UTEST(ServiceLifecycle, StopFailureDoesNotSkipOtherResources) {
  EventLog events;
  RecordingLogger logger;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kComponent,
                std::make_shared<Component>("healthy", &events));
  auto failing = std::make_shared<Component>("failing", &events, false, true);
  lifecycle.add(servicelib::ServiceComponentKind::kComponent, failing);

  lifecycle.start(servicelib::Context{});
  EXPECT_THROW(lifecycle.stop(servicelib::Context{}, logger), std::runtime_error);
  EXPECT_FALSE(lifecycle.isStopped());

  const auto recorded = events.snapshot();
  EXPECT_NE(std::find(recorded.begin(), recorded.end(), "healthy:stop"),
            recorded.end());
  EXPECT_NE(std::find(recorded.begin(), recorded.end(), "failing:stop"),
            recorded.end());
  ASSERT_EQ(logger.records.size(), 1);
  EXPECT_EQ(logger.records.front().message,
            "service shutdown operation failed");
  EXPECT_EQ(logger.records.front().resource, "component:1");
  EXPECT_EQ(logger.records.front().error, "stop failed");
  failing->failStop = false;
  lifecycle.stop(servicelib::Context{}, logger);
  EXPECT_TRUE(lifecycle.isStopped());
  const auto retried = events.snapshot();
  EXPECT_EQ(std::count(retried.begin(), retried.end(), "healthy:stop"), 1);
  EXPECT_EQ(std::count(retried.begin(), retried.end(), "failing:stop"), 2);
}

UTEST(ServiceLifecycle, DeadlineIsUpperBoundForWholeShutdown) {
  EventLog events;
  RecordingLogger logger;
  std::atomic<bool> stopped{false};
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(
      servicelib::ServiceComponentKind::kComponent,
      std::make_shared<Component>("slow", &events, false, false,
                                  std::chrono::milliseconds{30}, &stopped));

  lifecycle.start(servicelib::Context{});
  const auto started = std::chrono::steady_clock::now();
  EXPECT_THROW(lifecycle.stop(
      servicelib::Context{}.bounded(std::chrono::milliseconds{1}), logger),
      std::runtime_error);

  EXPECT_LT(std::chrono::steady_clock::now() - started,
            std::chrono::milliseconds{20});
  EXPECT_FALSE(stopped.load());
  ASSERT_EQ(logger.records.size(), 1);
  EXPECT_EQ(logger.records.front().message,
            "service shutdown operation timed out");
  EXPECT_EQ(logger.records.front().resource, "component:0");
  userver::engine::SleepFor(std::chrono::milliseconds{40});
  EXPECT_TRUE(stopped.load());
  lifecycle.stop(servicelib::Context{}, logger);
  EXPECT_TRUE(lifecycle.isStopped());
}

UTEST(ServiceLifecycle, DataConnectorTimeoutMatchesGoTelemetry) {
  EventLog events;
  std::atomic<bool> stopped{false};
  servicelib::testmetrics::TestMetrics metrics;
  servicelib::testlog::TestLog logger;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(
      servicelib::ServiceComponentKind::kDataSource,
      std::make_shared<Component>("source", &events, false, false,
                                  std::chrono::milliseconds{30}, &stopped),
      &metrics, &logger);

  lifecycle.start(servicelib::Context{});
  EXPECT_THROW(lifecycle.stop(
      servicelib::Context{}.bounded(std::chrono::milliseconds{1}), logger),
      std::runtime_error);

  EXPECT_FALSE(stopped.load());
  EXPECT_EQ(metrics
                .counter("datasource_connector.events_total",
                         {{"connector", "0"}, {"event", "stop_timeout"}})
                .count(),
            1);
  const auto entries = logger.entries();
  ASSERT_EQ(entries.size(), 1);
  EXPECT_EQ(entries.front().message, "data source stopped by timeout");
  userver::engine::SleepFor(std::chrono::milliseconds{40});
  EXPECT_TRUE(stopped.load());
  lifecycle.stop(servicelib::Context{}, logger);
  EXPECT_TRUE(lifecycle.isStopped());
}

UTEST(ServiceApp, PreparesConfiguredPoolsOnGraphLookup) {
  PriorityPoolConfig config;
  servicelib::config::RuntimeConfigRegistry::Publish(
      std::make_shared<const servicelib::config::RuntimeConfig>(config));
  {
    Service service;
    EXPECT_NE(service.getPriorityTaskPool("Default Pool"), nullptr);
  }
  servicelib::config::RuntimeConfigRegistry::Publish({});
}

UTEST(ServiceApp, StopWaitsForAcceptedInputInvocation) {
  PriorityPoolConfig config;
  servicelib::config::RuntimeConfigRegistry::Publish(
      std::make_shared<const servicelib::config::RuntimeConfig>(config));
  {
    Service service;
    const servicelib::Context context;
    service.start(context);
    auto invocation = std::make_unique<
        decltype(service.beginInputInvocation())>(
        service.beginInputInvocation());
    std::atomic<bool> stopReturned{false};
    auto stopTask = userver::engine::AsyncNoTracing([&] {
      service.stop(context);
      stopReturned.store(true, std::memory_order_release);
    });

    userver::engine::SleepFor(std::chrono::milliseconds{10});
    EXPECT_FALSE(stopReturned.load(std::memory_order_acquire));
    invocation.reset();
    stopTask.Get();
    EXPECT_TRUE(stopReturned.load(std::memory_order_acquire));
#ifdef NDEBUG
    EXPECT_THROW(static_cast<void>(service.beginInputInvocation()),
                 servicelib::StreamException);
#endif
  }
  servicelib::config::RuntimeConfigRegistry::Publish({});
}

}  // namespace

namespace {
struct HeldComponent final {
  userver::engine::SingleConsumerEvent entered, release;
  std::atomic<int> calls{0};
  std::optional<servicelib::Context> stopContext;
  bool failStart{};
  void start(servicelib::Context) {
    if (failStart) throw std::runtime_error("original startup failure");
  }
  void stop(servicelib::Context context) {
    stopContext = context;
    ++calls;
    entered.Send();
    EXPECT_TRUE(release.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  }
};

UTEST(ServiceLifecycle, IncompleteSourceKeepsSinkAndReusesStopOperation) {
  EventLog events;
  auto source = std::make_shared<HeldComponent>();
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource, source);
  lifecycle.add(servicelib::ServiceComponentKind::kDataSink,
                std::make_shared<Component>("sink", &events));
  lifecycle.start(servicelib::Context{});
  EXPECT_THROW(lifecycle.stop(servicelib::Context{}.bounded(
      std::chrono::milliseconds{5})), std::runtime_error);
  EXPECT_EQ(source->calls.load(), 1);
  EXPECT_FALSE(lifecycle.isStopped());
  EXPECT_EQ(events.snapshot(), (std::vector<std::string>{"sink:start"}));
  EXPECT_THROW(lifecycle.stopAfterGraphDrain(servicelib::Context{}), std::logic_error);
  source->release.Send();
  lifecycle.stop(servicelib::Context{});
  EXPECT_TRUE(lifecycle.isStopped());
  EXPECT_EQ(source->calls.load(), 1);
  EXPECT_TRUE(source->stopContext->deadline().has_value());
  EXPECT_EQ(events.snapshot(), (std::vector<std::string>{"sink:start", "sink:stop"}));
}

UTEST(ServiceLifecycle, ConcurrentCallerCancellationDoesNotCancelOwnerStop) {
  auto source = std::make_shared<HeldComponent>();
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource, source);
  lifecycle.start(servicelib::Context{});
  auto owner = userver::engine::AsyncNoTracing([&] {
    lifecycle.stop(servicelib::Context{});
  });
  EXPECT_TRUE(source->entered.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  std::stop_source cancellation;
  auto other = userver::engine::AsyncNoTracing([&] {
    lifecycle.stop(servicelib::Context{}.withExternalCancellation(cancellation.get_token()));
  });
  std::thread cancel([&] { cancellation.request_stop(); });
  cancel.join();
  EXPECT_THROW(other.Get(), std::runtime_error);
  EXPECT_FALSE(owner.IsFinished());
  EXPECT_EQ(source->calls.load(), 1);
  source->release.Send();
  owner.Get();
  EXPECT_TRUE(lifecycle.isStopped());
}

UTEST(ServiceLifecycle, StartupRollbackRetainsPartialSourceAndDependencies) {
  EventLog events;
  auto source = std::make_shared<HeldComponent>();
  source->failStart = true;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource, source);
  lifecycle.add(servicelib::ServiceComponentKind::kDataSink,
                std::make_shared<Component>("sink", &events));
  try {
    lifecycle.start(servicelib::Context{}.bounded(std::chrono::milliseconds{5}));
    ADD_FAILURE() << "startup must fail";
  } catch (const std::runtime_error& error) {
    EXPECT_STREQ(error.what(), "original startup failure");
  }
  EXPECT_FALSE(lifecycle.isStopped());
  EXPECT_EQ(events.snapshot(), (std::vector<std::string>{"sink:start"}));
  EXPECT_EQ(source->calls.load(), 1);
  source->release.Send();
  lifecycle.stop(servicelib::Context{});
  EXPECT_TRUE(lifecycle.isStopped());
  EXPECT_EQ(source->calls.load(), 1);
  EXPECT_EQ(events.snapshot(), (std::vector<std::string>{"sink:start", "sink:stop"}));
}

template <int Tag>
class RetainedService final
    : public servicelib::ServiceApp<RetainedService<Tag>, ServiceDataTypes> {};

UTEST(ServiceApp, GraphTimeoutRetainsSinkUntilRetry) {
  PriorityPoolConfig config;
  servicelib::config::RuntimeConfigRegistry::Publish(
      std::make_shared<const servicelib::config::RuntimeConfig>(config));
  {
    EventLog events;
    RetainedService<1> service;
    service.registerDataSink(std::make_shared<Component>("sink", &events));
    service.start(servicelib::Context{});
    auto invocation = std::make_unique<decltype(service.beginInputInvocation())>(
        service.beginInputInvocation());
    EXPECT_THROW(service.stop(servicelib::Context{}.bounded(
        std::chrono::milliseconds{5})), std::runtime_error);
    EXPECT_FALSE(service.isStopped());
    EXPECT_FALSE(service.isRunning());
    EXPECT_EQ(events.snapshot(), (std::vector<std::string>{"sink:start"}));
    EXPECT_THROW(service.start(servicelib::Context{}), std::logic_error);
    invocation.reset();
    service.stop(servicelib::Context{});
    EXPECT_TRUE(service.isStopped());
    EXPECT_EQ(events.snapshot(), (std::vector<std::string>{"sink:start", "sink:stop"}));
  }
  servicelib::config::RuntimeConfigRegistry::Publish({});
}

UTEST(ServiceApp, SourceFailureRetainsSinkForExplicitRetry) {
  PriorityPoolConfig config;
  servicelib::config::RuntimeConfigRegistry::Publish(
      std::make_shared<const servicelib::config::RuntimeConfig>(config));
  {
    EventLog events;
    RetainedService<2> service;
    auto source = std::make_shared<Component>("source", &events, false, true);
    service.registerDataSource(source);
    service.registerDataSink(std::make_shared<Component>("sink", &events));
    service.start(servicelib::Context{});
    EXPECT_THROW(service.stop(servicelib::Context{}), std::runtime_error);
    EXPECT_FALSE(service.isStopped());
    const auto pending = events.snapshot();
    EXPECT_EQ(std::count(pending.begin(), pending.end(), "sink:stop"), 0);
    source->failStop = false;
    service.stop(servicelib::Context{});
    EXPECT_TRUE(service.isStopped());
    const auto done = events.snapshot();
    EXPECT_EQ(std::count(done.begin(), done.end(), "sink:stop"), 1);
  }
  servicelib::config::RuntimeConfigRegistry::Publish({});
}
}  // namespace

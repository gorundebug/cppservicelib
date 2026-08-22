/*
 * prioritytaskpool.hpp
 * C++ streams API — priority task pool with a bounded, dynamically
 * resizable set of executor coroutines.
 *
 * Logic mirrors servicelib's Go implementation
 * (runtime/pool/prioritytaskpool.go): a binary heap ordered by
 * priority, guarded by a mutex+condvar, N executor coroutines draining it,
 * a manager coroutine that re-reads PoolConfig.ExecutorsCount once a
 * second and hot-resizes the executor set, and cancellation/deadline handling
 * that bumps a waiting task's priority to the minimum (most urgent) value and
 * re-heapifies it (Go analog: context.AfterFunc callback + heap.Fix, firing on
 * ctx.Done() — deadline OR explicit cancellation).
 *
 * See taskpool.hpp's file comment for the departures from a literal Go
 * port (incremental resize, cancellation-after-enqueue, explicit
 * PoolState/StartupResult state machine) — identical reasoning applies
 * here, just against a heap instead of a linked list.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <userver/concurrent/background_task_storage.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/condition_variable.hpp>
#include <userver/engine/mutex.hpp>
#include <userver/engine/single_consumer_event.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/engine/task/cancel.hpp>
#include <userver/engine/task/current_task.hpp>
#include <userver/engine/task/task_with_result.hpp>
#include <userver/utils/assert.hpp>
#include <userver/utils/async.hpp>

#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/pool/pool.hpp>
#include <servicelib/runtime/pool/userver_aliases.hpp>

namespace servicelib::pool {

inline constexpr std::size_t kDefaultPriorityQueueCapacity = 256;

class PriorityTaskPoolImpl final : public IPriorityTaskPool {
 public:
  PriorityTaskPoolImpl(std::string name, IServiceEnvironment& env)
      : name_(std::move(name)), env_(env) {
    const auto cfgSnapshot = env_.getRuntimeConfigSnapshot();
    const auto* cfg = cfgSnapshot.get();
    if (!cfg || !cfg->GetPoolByName(name_)) {
      throw std::invalid_argument("priority task pool configuration named '" +
                                  name_ + "' not found");
    }
    const auto* poolConfig = cfg->GetPoolByName(name_);
    const std::size_t queueCapacity =
        poolConfig->queueCapacity > 0
            ? static_cast<std::size_t>(poolConfig->queueCapacity)
            : kDefaultPriorityQueueCapacity;
    heap_.reserve(queueCapacity);
    deadlineHeap_.reserve(queueCapacity);

    const auto svcSnapshot = env_.getServiceConfigSnapshot();
    const auto* svc = svcSnapshot.get();
    metricsEnabled_ = env_.getMetrics().enabled();
    auto scope = env_.getMetrics().scope(
        "priority_task_pool",
        metrics::Labels{{"service", svc ? svc->name : std::string()},
                        {"name", name_}});
    gaugeQueueLength_ =
        scope->gauge("queue_length", "Priority task pool wait queue length");
    gaugeExecutorsTarget_ = scope->gauge(
        "executors_target", "Desired number of priority task pool executors");
    gaugeExecutorsAllocated_ = scope->gauge(
        "executors_allocated", "Number of live priority task pool executors");
    gaugeExecutorsBusy_ = scope->gauge(
        "executors_busy",
        "Number of priority task pool executors running callbacks");
    tasksTotal_ = scope->counter(
        "tasks_total", "Total number of tasks executed by priority task pool");
    executionDuration_ = scope->histogram("task_execution_duration_seconds",
                                          "Task execution duration in seconds");
    stopTimeoutCounter_ = scope->counter(
        "events_total", "Total number of events in priority task pool",
        {{"event", "stop_timeout"}});
    taskRejectedCounter_ = scope->counter(
        "events_total", "Total number of events in priority task pool",
        {{"event", "task_rejected"}});
    taskExpeditedCounter_ = scope->counter(
        "events_total", "Total number of events in priority task pool",
        {{"event", "task_expedited"}});
  }

  ~PriorityTaskPoolImpl() override {
    const PoolState state = state_.load(std::memory_order_acquire);
    if (state != PoolState::kCreated && state != PoolState::kStopped) {
      utils::AbortWithStacktrace(
          "PriorityTaskPoolImpl must be stopped before destruction");
    }
  }

  const std::string& getName() const noexcept override { return name_; }

  int getExecutorsCount() const override {
    return targetExecutors_.load(std::memory_order_relaxed);
  }

  void start(Context ctx) override {
    PoolState expected = PoolState::kCreated;
    if (!state_.compare_exchange_strong(expected, PoolState::kStarting,
                                        std::memory_order_acq_rel)) {
      if (expected == PoolState::kDraining ||
          expected == PoolState::kStopping || expected == PoolState::kStopped) {
        throw PoolStoppedError();
      }
      throw PoolAlreadyStartedError();
    }

    // See taskpool.hpp's start() for why this wait is uninterruptible, why
    // the manager is spawned via CriticalAsync (guarantees its body — and
    // the SignalReadyOnExit guard inside it — actually starts running),
    // and why startupResult_ (not just "the event fired") is the source of
    // truth for whether the pool is actually ready.
    {
      engine::TaskCancellationBlocker blocker;
      try {
        taskProcessor_ = &engine::current_task::GetTaskProcessor();
        cancelWatches_.emplace(*taskProcessor_);
        managerTask_ =
            engine::CriticalAsyncNoTracing([this, ctx] { managerLoop(ctx); });
      } catch (...) {
        // See taskpool.hpp's start() for why: publish first (unblocks a
        // concurrent stop() parked on fieldsPublished_), then fail the
        // state transition via CAS rather than an unconditional store.
        fieldsPublished_.Send();
        PoolState fromStarting = PoolState::kStarting;
        state_.compare_exchange_strong(fromStarting, PoolState::kFailed,
                                       std::memory_order_acq_rel);
        throw;
      }
      // See taskpool.hpp's start() for why this handshake is required
      // before touching taskProcessor_/cancelWatches_/managerTask_ from a
      // concurrent stop().
      fieldsPublished_.Send();
      static_cast<void>(readyEvent_.WaitForEvent());
    }

    // CAS (not unconditional store) — see taskpool.hpp's start() for why:
    // a concurrent stop() may have already driven state_ past kStarting by
    // the time we wake up here.
    PoolState fromStarting = PoolState::kStarting;
    switch (startupResult_.load(std::memory_order_acquire)) {
      case StartupResult::kReady:
        if (!state_.compare_exchange_strong(fromStarting, PoolState::kRunning,
                                            std::memory_order_acq_rel)) {
          throw PoolStoppedError();
        }
        return;
      case StartupResult::kCancelled:
        if (!state_.compare_exchange_strong(fromStarting, PoolState::kFailed,
                                            std::memory_order_acq_rel)) {
          throw PoolStoppedError();
        }
        throw PoolCancelledError();
      case StartupResult::kStopped:
      case StartupResult::kPending:  // unreachable: readyEvent_ only fires
                                     // after a resolution is stored
        state_.compare_exchange_strong(fromStarting, PoolState::kFailed,
                                       std::memory_order_acq_rel);
        throw PoolStoppedError();
      case StartupResult::kFailed:
        if (!state_.compare_exchange_strong(fromStarting, PoolState::kFailed,
                                            std::memory_order_acq_rel)) {
          throw PoolStoppedError();
        }
        throw std::runtime_error("failed to start priority task pool '" +
                                 name_ + "'");
    }
  }

  void stop(Context ctx) override {
    // Uninterruptible for its whole duration — see taskpool.hpp's stop()
    // for why (must be immune to cancellation of its own calling task, as
    // opposed to the explicit ctx.deadline() timeout it still honors below).
    engine::TaskCancellationBlocker cancellationBlocker;

    // Serialized with addTask's final admission check. This makes the
    // kStopping transition the exact boundary after which enqueue is rejected.
    PoolState previous;
    {
      std::unique_lock<engine::Mutex> lock(mu_);
      previous = state_.load(std::memory_order_acquire);
      while (true) {
        if (previous == PoolState::kStopped) {
          return;
        }
        if (previous == PoolState::kStopping) {
          static_cast<void>(cv_.Wait(lock, [this] {
            return state_.load(std::memory_order_acquire) ==
                   PoolState::kStopped;
          }));
          return;
        }
        if (state_.compare_exchange_weak(previous, PoolState::kStopping,
                                         std::memory_order_acq_rel)) {
          done_ = true;
          cv_.NotifyAll();
          break;
        }
      }
    }

    if (previous == PoolState::kStarting) {
      // See taskpool.hpp's stop() for why: a concurrent start() may still
      // be writing taskProcessor_/cancelWatches_/managerTask_ — wait for
      // it to publish them (or fail and publish anyway) before reading any
      // of them below.
      static_cast<void>(fieldsPublished_.WaitForEvent());
    }

    if (managerTask_.IsValid()) {
      resolveStartup(StartupResult::kStopped);
      readyEvent_.Send();
      managerTask_.SyncCancel();  // userver API: noexcept
    }

    std::vector<engine::TaskWithResult<void>> toWait;
    {
      std::unique_lock<engine::Mutex> lock(mu_);
      toWait = std::move(executors_);
    }

    if (!toWait.empty()) {
      if (const auto& d = ctx.deadline(); d.has_value()) {
        const auto deadline = engine::Deadline::FromTimePoint(*d);
        bool timedOut = false;
        for (auto& task : toWait) {
          if (task.WaitNothrowUntil(deadline) != engine::FutureStatus::kReady) {
            timedOut = true;
            break;
          }
        }
        if (timedOut) {
          size_t tasksLeft = 0;
          {
            std::unique_lock<engine::Mutex> lock(mu_);
            tasksLeft = heap_.size();
          }
          bestEffortTelemetry([this, tasksLeft] {
            env_.getLogger().warn(
                "priority task pool stopped by timeout",
                {log::Field::Str("pool", name_),
                 log::Field::Int64("tasks_count",
                                   static_cast<int64_t>(tasksLeft))});
          });
          bestEffortMetrics([this] { stopTimeoutCounter_->inc(); });
        }
      }
      for (auto& task : toWait) {
        static_cast<void>(task.WaitNothrow());
      }
    }

    // See taskpool.hpp's stop() for why this drain must happen after the
    // executors_ wait above (which guarantees every task has been popped
    // and its cancelCallback fired-or-reset by now, making this final) and
    // why it's guarded (cancelWatches_ may be unset if start() never got
    // far enough to set it).
    if (cancelWatches_) {
      cancelWatches_->CancelAndWait();  // userver API: noexcept
    }

    state_.store(PoolState::kStopped, std::memory_order_release);
    {
      std::unique_lock<engine::Mutex> lock(mu_);
      cv_.NotifyAll();
    }
  }

  void addTask(Context ctx, int priority, std::function<void()> fn) override {
    const PoolState state = state_.load(std::memory_order_acquire);
    if (state != PoolState::kRunning) {
      bestEffortMetrics([this] { taskRejectedCounter_->inc(); });
      if (state == PoolState::kDraining || state == PoolState::kStopping ||
          state == PoolState::kStopped) {
        throw PoolStoppedError();
      }
      throw PoolNotStartedError();  // kCreated, kStarting, or kFailed
    }
    if (ctx.cancelled()) {
      bestEffortMetrics([this] { taskRejectedCounter_->inc(); });
      throw PoolCancelledError();
    }
    if (const auto& d = ctx.deadline();
        d.has_value() && *d <= std::chrono::steady_clock::now()) {
      bestEffortMetrics([this] { taskRejectedCounter_->inc(); });
      throw PoolCancelledError();
    }

    auto task = std::make_shared<PriorityPoolTask>();
    task->fn = std::move(fn);
    task->priority = priority;

    // mu_ held continuously from here through insertion (or rejection) —
    // see taskpool.hpp's addTask for the exact race this closes.
    std::unique_lock<engine::Mutex> lock(mu_);

    if (done_ ||
        state_.load(std::memory_order_acquire) != PoolState::kRunning) {
      lock.unlock();
      bestEffortMetrics([this] { taskRejectedCounter_->inc(); });
      throw PoolStoppedError();
    }
    if (const auto& d = ctx.deadline();
        ctx.cancelled() ||
        (d.has_value() && *d <= std::chrono::steady_clock::now())) {
      lock.unlock();
      bestEffortMetrics([this] { taskRejectedCounter_->inc(); });
      throw PoolCancelledError();
    }

    // See taskpool.hpp's addTask for why this uses cancelWatches_
    // (BackgroundTaskStorage, safe to call from a non-coroutine thread)
    // and why the callback body must not let exceptions escape.
    std::weak_ptr<PriorityPoolTask> weakTaskForCancel(task);
    const auto onCancel = [this, weakTaskForCancel]() noexcept {
      try {
        cancelWatches_->CriticalAsyncDetach(
            name_ + "-cancel-watch",
            [this, weakTaskForCancel] { expedite(weakTaskForCancel); });
      } catch (...) {
        // Exceptions must not escape std::stop_callback.
      }
    };
    task->cancelCallback.emplace(ctx.stopToken(), onCancel);
    task->externalCancelCallbacks.reserve(ctx.externalStopTokens().size());
    for (const auto& token : ctx.externalStopTokens()) {
      task->externalCancelCallbacks.push_back(
          std::make_unique<PriorityPoolTask::CancelCallback>(token, onCancel));
    }

    if (const auto& deadline = ctx.deadline(); deadline.has_value()) {
      task->deadline = *deadline;
      deadlineHeapPushLocked(task);
    }

    task->sequence = nextSequence_;
    try {
      heapPush(task);
    } catch (...) {
      deadlineHeapRemoveLocked(task);
      throw;
    }
    ++nextSequence_;
    const auto queueSize = static_cast<int64_t>(heap_.size());

    lock.unlock();
    cv_.NotifyOne();
    bestEffortMetrics([this, queueSize] { gaugeQueueLength_->set(queueSize); });
  }

 private:
  template <typename Callback>
  static void bestEffortTelemetry(Callback&& callback) noexcept {
    try {
      std::forward<Callback>(callback)();
    } catch (...) {
    }
  }

  template <typename Callback>
  void bestEffortMetrics(Callback&& callback) noexcept {
    if (!metricsEnabled_) return;
    bestEffortTelemetry(std::forward<Callback>(callback));
  }

  void publishAllocatedExecutors() noexcept {
    bestEffortMetrics([this] {
      gaugeExecutorsAllocated_->set(
          allocatedExecutors_.load(std::memory_order_relaxed));
    });
  }

  void publishBusyExecutors() noexcept {
    bestEffortMetrics([this] {
      gaugeExecutorsBusy_->set(busyExecutors_.load(std::memory_order_relaxed));
    });
  }

  enum class PoolState {
    kCreated,
    kStarting,
    kRunning,
    kDraining,
    kStopping,
    kStopped,
    kFailed
  };
  enum class StartupResult { kPending, kReady, kCancelled, kStopped, kFailed };

  // Sets startupResult_ if (and only if) it hasn't already been resolved.
  bool resolveStartup(StartupResult result) {
    StartupResult expected = StartupResult::kPending;
    return startupResult_.compare_exchange_strong(expected, result,
                                                  std::memory_order_acq_rel);
  }

  void requestDrainFromManager() {
    std::unique_lock<engine::Mutex> lock(mu_);
    PoolState state = state_.load(std::memory_order_acquire);
    while (state == PoolState::kStarting || state == PoolState::kRunning) {
      if (state_.compare_exchange_weak(state, PoolState::kDraining,
                                       std::memory_order_acq_rel)) {
        done_ = true;
        cv_.NotifyAll();
        return;
      }
    }
  }

  struct PriorityPoolTask {
    using CancelCallback = std::stop_callback<std::function<void()>>;

    std::function<void()> fn;
    int priority = 0;
    std::uint64_t sequence = 0;
    std::size_t priorityIndex = kNotInHeap;
    bool expedited = false;  // true once bumped to the front by expedite()
    Deadline deadline;
    std::size_t deadlineIndex = kNotInHeap;
    std::optional<CancelCallback> cancelCallback;
    std::vector<std::unique_ptr<CancelCallback>> externalCancelCallbacks;
  };

  static constexpr std::size_t kNotInHeap =
      std::numeric_limits<std::size_t>::max();

  // Binary min-heap, matching Go's container/heap Less implementation. Lower
  // numeric priority runs first; sequence preserves
  // FIFO order among tasks with equal priority.
  static bool comesBefore(const PriorityPoolTask& lhs,
                          const PriorityPoolTask& rhs) noexcept {
    if (lhs.priority != rhs.priority) {
      return lhs.priority < rhs.priority;
    }
    return lhs.sequence < rhs.sequence;
  }

  void heapSwap(std::size_t i, std::size_t j) {
    std::swap(heap_[i], heap_[j]);
    heap_[i]->priorityIndex = i;
    heap_[j]->priorityIndex = j;
  }

  void heapSiftUp(std::size_t i) {
    while (i > 0) {
      const std::size_t parent = (i - 1) / 2;
      if (!comesBefore(*heap_[i], *heap_[parent])) {
        break;
      }
      heapSwap(parent, i);
      i = parent;
    }
  }

  void heapSiftDown(std::size_t i) {
    const std::size_t n = heap_.size();
    while (i < n / 2) {
      const std::size_t left = 2 * i + 1;
      const std::size_t right = left + 1;
      std::size_t best = left;
      if (right < n && comesBefore(*heap_[right], *heap_[left])) {
        best = right;
      }
      if (!comesBefore(*heap_[best], *heap_[i])) {
        break;
      }
      heapSwap(i, best);
      i = best;
    }
  }

  void heapPush(const std::shared_ptr<PriorityPoolTask>& task) {
    task->priorityIndex = heap_.size();
    heap_.push_back(task);
    heapSiftUp(task->priorityIndex);
  }

  std::shared_ptr<PriorityPoolTask> heapPop() {
    auto top = heap_.front();
    auto last = heap_.back();
    heap_.pop_back();
    top->priorityIndex = kNotInHeap;
    if (!heap_.empty()) {
      heap_[0] = last;
      last->priorityIndex = 0;
      heapSiftDown(0);
    }
    return top;
  }

  static bool deadlineEarlier(
      const std::shared_ptr<PriorityPoolTask>& lhs,
      const std::shared_ptr<PriorityPoolTask>& rhs) noexcept {
    return *lhs->deadline < *rhs->deadline;
  }

  void deadlineHeapSwapLocked(std::size_t lhs, std::size_t rhs) noexcept {
    std::swap(deadlineHeap_[lhs], deadlineHeap_[rhs]);
    deadlineHeap_[lhs]->deadlineIndex = lhs;
    deadlineHeap_[rhs]->deadlineIndex = rhs;
  }

  void deadlineHeapSiftUpLocked(std::size_t index) noexcept {
    while (index != 0) {
      const std::size_t parent = (index - 1) / 2;
      if (!deadlineEarlier(deadlineHeap_[index], deadlineHeap_[parent])) {
        return;
      }
      deadlineHeapSwapLocked(index, parent);
      index = parent;
    }
  }

  void deadlineHeapSiftDownLocked(std::size_t index) noexcept {
    const std::size_t size = deadlineHeap_.size();
    while (true) {
      const std::size_t left = index * 2 + 1;
      if (left >= size) {
        return;
      }
      const std::size_t right = left + 1;
      std::size_t earliest = left;
      if (right < size &&
          deadlineEarlier(deadlineHeap_[right], deadlineHeap_[left])) {
        earliest = right;
      }
      if (!deadlineEarlier(deadlineHeap_[earliest], deadlineHeap_[index])) {
        return;
      }
      deadlineHeapSwapLocked(index, earliest);
      index = earliest;
    }
  }

  void deadlineHeapPushLocked(const std::shared_ptr<PriorityPoolTask>& task) {
    task->deadlineIndex = deadlineHeap_.size();
    try {
      deadlineHeap_.push_back(task);
    } catch (...) {
      task->deadlineIndex = kNotInHeap;
      throw;
    }
    deadlineHeapSiftUpLocked(task->deadlineIndex);
  }

  void deadlineHeapRemoveAtLocked(std::size_t index) noexcept {
    auto removed = deadlineHeap_[index];
    const std::size_t last = deadlineHeap_.size() - 1;
    if (index != last) {
      deadlineHeapSwapLocked(index, last);
    }
    deadlineHeap_.pop_back();
    removed->deadlineIndex = kNotInHeap;

    if (index >= deadlineHeap_.size()) {
      return;
    }
    if (index != 0 &&
        deadlineEarlier(deadlineHeap_[index], deadlineHeap_[(index - 1) / 2])) {
      deadlineHeapSiftUpLocked(index);
    } else {
      deadlineHeapSiftDownLocked(index);
    }
  }

  void deadlineHeapRemoveLocked(
      const std::shared_ptr<PriorityPoolTask>& task) noexcept {
    if (task->deadlineIndex != kNotInHeap) {
      deadlineHeapRemoveAtLocked(task->deadlineIndex);
    }
  }

  bool expediteLocked(const std::shared_ptr<PriorityPoolTask>& task) noexcept {
    if (task->priorityIndex == kNotInHeap || task->expedited) {
      return false;
    }
    task->expedited = true;
    deadlineHeapRemoveLocked(task);
    task->priority = std::numeric_limits<int>::min();
    heapSiftUp(task->priorityIndex);
    return true;
  }

  std::size_t promoteExpiredDeadlinesLocked(
      std::chrono::steady_clock::time_point now) noexcept {
    std::size_t promoted = 0;
    while (!deadlineHeap_.empty() && *deadlineHeap_.front()->deadline <= now) {
      auto task = deadlineHeap_.front();
      deadlineHeapRemoveAtLocked(0);
      if (expediteLocked(task)) {
        ++promoted;
      }
    }
    return promoted;
  }

  // Bumps the task to the most urgent priority (INT_MIN) and re-heapifies.
  // Called from the cancellation callback. Deadline expiry uses the same
  // locked helper before dequeue. The `expedited` flag (not the priority
  // value, which a caller may already have set to INT_MIN) is idempotence.
  void expedite(const std::weak_ptr<PriorityPoolTask>& weakTask) {
    auto task = weakTask.lock();
    if (!task) {
      return;
    }

    std::unique_lock<engine::Mutex> lock(mu_);
    const std::size_t deadlinePromotions =
        deadlineHeap_.empty()
            ? 0
            : promoteExpiredDeadlinesLocked(std::chrono::steady_clock::now());
    const bool cancellationPromoted = expediteLocked(task);
    if (deadlinePromotions == 0 && !cancellationPromoted) {
      return;
    }

    lock.unlock();
    cv_.NotifyOne();
    const std::size_t totalPromotions =
        deadlinePromotions + (cancellationPromoted ? 1 : 0);
    for (std::size_t i = 0; i < totalPromotions; ++i) {
      bestEffortMetrics([this] { taskExpeditedCounter_->inc(); });
    }
  }

  void runTask(const std::shared_ptr<PriorityPoolTask>& task) {
    struct BusyExecutorGuard final {
      PriorityTaskPoolImpl& pool;
      explicit BusyExecutorGuard(PriorityTaskPoolImpl& value) : pool(value) {
        pool.busyExecutors_.fetch_add(1, std::memory_order_relaxed);
        pool.publishBusyExecutors();
      }
      ~BusyExecutorGuard() {
        pool.busyExecutors_.fetch_sub(1, std::memory_order_relaxed);
        pool.publishBusyExecutors();
      }
    } busyGuard{*this};

    const auto start = metricsEnabled_
                           ? std::chrono::steady_clock::now()
                           : std::chrono::steady_clock::time_point{};
    try {
      task->fn();
    } catch (const std::exception& e) {
      bestEffortTelemetry([this, &e] {
        env_.getLogger().warn(
            "priority task pool task error",
            {log::Field::Str("pool", name_), log::Field::Err(e)});
      });
    } catch (...) {
      bestEffortTelemetry([this] {
        env_.getLogger().warn("priority task pool task error",
                              {log::Field::Str("pool", name_),
                               log::Field::Str("error", "<unknown>")});
      });
    }
    task->fn = nullptr;
    if (metricsEnabled_) {
      bestEffortMetrics([this] { tasksTotal_->inc(); });
      const double duration = std::chrono::duration<double>(
                                  std::chrono::steady_clock::now() - start)
                                  .count();
      bestEffortMetrics(
          [this, duration] { executionDuration_->observe(duration); });
    }
  }

  // Claims one excess reserved slot. This is called only while mu_ is held;
  // reconcileExecutors() publishes targetExecutors_ under the same mutex,
  // so a target change cannot race the claim.
  bool shouldExitAsExcess() {
    int allocated = allocatedExecutors_.load(std::memory_order_relaxed);
    const int target = targetExecutors_.load(std::memory_order_relaxed);
    while (allocated > target) {
      if (allocatedExecutors_.compare_exchange_weak(
              allocated, allocated - 1, std::memory_order_acq_rel)) {
        publishAllocatedExecutors();
        return true;
      }
    }
    return false;
  }

  // A slot is reserved before CriticalAsync is called and is released on
  // every executor exit. Keeping pending and running executors in one atomic
  // counter avoids the impossible-to-snapshot live+pending pair.
  class ExecutorSlotGuard {
   public:
    explicit ExecutorSlotGuard(PriorityTaskPoolImpl& pool) : pool_(pool) {}
    ~ExecutorSlotGuard() {
      if (!released_) {
        pool_.allocatedExecutors_.fetch_sub(1, std::memory_order_relaxed);
        pool_.publishAllocatedExecutors();
      }
    }
    ExecutorSlotGuard(const ExecutorSlotGuard&) = delete;
    ExecutorSlotGuard& operator=(const ExecutorSlotGuard&) = delete;

    // Marks the decrement as already performed by shouldExitAsExcess()'s
    // own CAS, so the destructor doesn't double-decrement.
    void release() noexcept { released_ = true; }

   private:
    PriorityTaskPoolImpl& pool_;
    bool released_ = false;
  };

  void executorLoop() {
    ExecutorSlotGuard slotGuard(*this);

    while (true) {
      std::unique_lock<engine::Mutex> lock(mu_);
      const bool woken = cv_.Wait(lock, [this] {
        return !heap_.empty() || done_ ||
               allocatedExecutors_.load(std::memory_order_relaxed) >
                   targetExecutors_.load(std::memory_order_relaxed);
      });
      if (!woken) {
        // Cancelled while genuinely idle — slotGuard's destructor accounts
        // for the decrement.
        return;
      }

      // Taken under mu_ (lock still held) — the only place
      // shouldExitAsExcess() is called. See taskpool.hpp's executorLoop
      // for why this must happen under the same lock reconcileExecutors()
      // uses to write targetExecutors_.
      if (shouldExitAsExcess()) {
        slotGuard.release();
        return;
      }
      if (heap_.empty() && done_) {
        // Not "excess" — the whole pool is stopping. slotGuard's
        // destructor performs the decrement.
        return;
      }

      const std::size_t deadlinePromotions =
          deadlineHeap_.empty()
              ? 0
              : promoteExpiredDeadlinesLocked(std::chrono::steady_clock::now());
      auto task = heapPop();
      deadlineHeapRemoveLocked(task);
      const auto queueSize = static_cast<int64_t>(heap_.size());
      lock.unlock();
      bestEffortMetrics(
          [this, queueSize] { gaugeQueueLength_->set(queueSize); });

      for (std::size_t i = 0; i < deadlinePromotions; ++i) {
        bestEffortMetrics([this] { taskExpeditedCounter_->inc(); });
      }
      task->cancelCallback.reset();
      task->externalCancelCallbacks.clear();

      runTask(task);
    }
  }

  // Reconciles all reserved executor slots (scheduled or running) against
  // target. All vector capacity is reserved before a slot is claimed, so
  // every successfully created handle can be adopted even if a later spawn
  // fails. Called every manager tick to heal exits racing a target change.
  void reconcileExecutors(int target) {
    int toSpawn = 0;
    {
      std::unique_lock<engine::Mutex> lock(mu_);
      targetExecutors_.store(target, std::memory_order_relaxed);
      bestEffortMetrics([this, target] { gaugeExecutorsTarget_->set(target); });
      executors_.erase(std::remove_if(executors_.begin(), executors_.end(),
                                      [](auto& t) { return t.IsFinished(); }),
                       executors_.end());

      const int allocated = allocatedExecutors_.load(std::memory_order_relaxed);
      if (target < allocated) {
        cv_.NotifyAll();
        return;
      }
      toSpawn = target - allocated;
      if (toSpawn == 0) {
        return;
      }
      executors_.reserve(executors_.size() + static_cast<size_t>(toSpawn));
    }

    std::vector<engine::TaskWithResult<void>> newExecutors;
    newExecutors.reserve(static_cast<size_t>(toSpawn));
    std::exception_ptr spawnError;
    for (int i = 0; i < toSpawn; ++i) {
      allocatedExecutors_.fetch_add(1, std::memory_order_relaxed);
      publishAllocatedExecutors();
      try {
        newExecutors.push_back(
            engine::CriticalAsyncNoTracing([this] { executorLoop(); }));
      } catch (...) {
        allocatedExecutors_.fetch_sub(1, std::memory_order_relaxed);
        publishAllocatedExecutors();
        spawnError = std::current_exception();
        break;
      }
    }

    {
      std::unique_lock<engine::Mutex> lock(mu_);
      for (auto& task : newExecutors) {
        executors_.push_back(std::move(task));
      }
    }

    if (spawnError) {
      std::rethrow_exception(spawnError);
    }
  }

  void managerLoop(Context ctx) {
    // See taskpool.hpp's managerLoop for why this guard exists: it
    // guarantees start()'s WaitForEvent() is released on every exit path,
    // including ones that never spawn a single executor, and why it runs
    // even if this function is entered already-cancelled (CriticalAsync).
    struct SignalReadyOnExit {
      engine::SingleConsumerEvent& event;
      ~SignalReadyOnExit() { event.Send(); }
    } signalReadyOnExit{readyEvent_};

    const auto lifecycleCancelled = [&ctx] {
      const auto& deadline = ctx.deadline();
      return engine::current_task::ShouldCancel() || ctx.cancelled() ||
             (deadline.has_value() &&
              *deadline <= std::chrono::steady_clock::now());
    };

    while (true) {
      // See taskpool.hpp's managerLoop for why ctx.deadline() must be
      // checked here too, alongside ctx.cancelled().
      if (lifecycleCancelled()) {
        if (!resolveStartup(StartupResult::kCancelled)) {
          requestDrainFromManager();
        }
        return;
      }
      {
        std::unique_lock<engine::Mutex> lock(mu_);
        if (done_) {
          resolveStartup(StartupResult::kStopped);
          return;
        }
      }

      const auto runtimeConfig = env_.getRuntimeConfigSnapshot();
      const config::PoolConfig* cfg = nullptr;
      if (const auto* rc = runtimeConfig.get()) {
        cfg = rc->GetPoolByName(name_);
      }
      int newTarget =
          (cfg && cfg->executorsCount > 0)
              ? cfg->executorsCount
              : static_cast<int>(std::thread::hardware_concurrency());
      if (newTarget <= 0) {
        newTarget = 1;
      }
      // Every tick, unconditionally — see reconcileExecutors()'s comment.
      try {
        reconcileExecutors(newTarget);
      } catch (const std::exception& e) {
        bestEffortTelemetry([this, &e] {
          env_.getLogger().warn(
              "priority task pool executor resize failed",
              {log::Field::Str("pool", name_), log::Field::Err(e)});
        });
        if (startupResult_.load(std::memory_order_acquire) ==
                StartupResult::kPending &&
            allocatedExecutors_.load(std::memory_order_relaxed) == 0) {
          resolveStartup(StartupResult::kFailed);
          return;
        }
      } catch (...) {
        bestEffortTelemetry([this] {
          env_.getLogger().warn("priority task pool executor resize failed",
                                {log::Field::Str("pool", name_),
                                 log::Field::Str("error", "<unknown>")});
        });
        if (startupResult_.load(std::memory_order_acquire) ==
                StartupResult::kPending &&
            allocatedExecutors_.load(std::memory_order_relaxed) == 0) {
          resolveStartup(StartupResult::kFailed);
          return;
        }
      }

      // Reconciliation can take long enough for the lifecycle context to
      // expire. Do not publish kReady after that has already happened.
      if (lifecycleCancelled()) {
        if (!resolveStartup(StartupResult::kCancelled)) {
          requestDrainFromManager();
        }
        return;
      }

      resolveStartup(StartupResult::kReady);
      readyEvent_.Send();

      engine::InterruptibleSleepFor(std::chrono::seconds(1));
    }
  }

  std::string name_;
  IServiceEnvironment& env_;
  engine::TaskProcessor* taskProcessor_ = nullptr;

  engine::Mutex mu_;
  engine::ConditionVariable cv_;
  std::vector<std::shared_ptr<PriorityPoolTask>> heap_;
  std::vector<std::shared_ptr<PriorityPoolTask>> deadlineHeap_;
  std::uint64_t nextSequence_ = 0;
  bool done_ = false;

  std::atomic<PoolState> state_{PoolState::kCreated};
  std::atomic<StartupResult> startupResult_{StartupResult::kPending};
  std::atomic<int> targetExecutors_{0};
  // Includes both scheduled and already-running executors. Two separate
  // live/pending atomics cannot be snapshotted consistently.
  std::atomic<int> allocatedExecutors_{0};
  std::atomic<int> busyExecutors_{0};
  engine::SingleConsumerEvent readyEvent_{
      engine::SingleConsumerEvent::NoAutoReset{}};
  // See start()/stop(): synchronizes-with handshake so a concurrent stop()
  // never reads taskProcessor_/cancelWatches_/managerTask_ while start()
  // is still writing them.
  engine::SingleConsumerEvent fieldsPublished_{
      engine::SingleConsumerEvent::NoAutoReset{}};

  engine::TaskWithResult<void> managerTask_;
  std::vector<engine::TaskWithResult<void>> executors_;

  std::unique_ptr<metrics::Int64Gauge> gaugeQueueLength_;
  std::unique_ptr<metrics::Int64Gauge> gaugeExecutorsTarget_;
  std::unique_ptr<metrics::Int64Gauge> gaugeExecutorsAllocated_;
  std::unique_ptr<metrics::Int64Gauge> gaugeExecutorsBusy_;
  std::unique_ptr<metrics::Int64Counter> tasksTotal_;
  std::unique_ptr<metrics::Float64Histogram> executionDuration_;
  std::unique_ptr<metrics::Int64Counter> stopTimeoutCounter_;
  std::unique_ptr<metrics::Int64Counter> taskRejectedCounter_;
  // Counts the first successful priority bump while a task is still queued,
  // regardless of whether it was triggered by deadline expiry or explicit
  // context cancellation.
  std::unique_ptr<metrics::Int64Counter> taskExpeditedCounter_;
  bool metricsEnabled_{};

  // Holds cancellation-triggered promote-to-front tasks — see addTask().
  // Self-prunes finished entries (unlike executors_, which is pruned
  // manually), and explicitly supports launching from a non-coroutine
  // thread. Declared after the metrics fields (userver convention for
  // BackgroundTaskStorage) — see taskpool.hpp's cancelWatches_ for why.
  std::optional<concurrent::BackgroundTaskStorage> cancelWatches_;
};

inline std::unique_ptr<IPriorityTaskPool> makePriorityTaskPool(
    std::string name, IServiceEnvironment& env) {
  return std::make_unique<PriorityTaskPoolImpl>(std::move(name), env);
}

}  // namespace servicelib::pool

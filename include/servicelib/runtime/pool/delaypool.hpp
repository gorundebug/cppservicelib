/*
 * delaypool.hpp
 * C++ streams API — service-wide delayed task scheduler.
 *
 * Mirrors servicelib/runtime/pool/delaypool.go: every accepted call owns an
 * independent deadline, context completion may execute the task early, and
 * an atomic once-claim guarantees that the user function runs exactly once.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>

#include <userver/baggage/baggage.hpp>
#include <userver/concurrent/background_task_storage.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/condition_variable.hpp>
#include <userver/engine/deadline.hpp>
#include <userver/engine/mutex.hpp>
#include <userver/engine/single_consumer_event.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/engine/task/cancel.hpp>
#include <userver/engine/task/current_task.hpp>
#include <userver/engine/task/local_variable.hpp>
#include <userver/utils/assert.hpp>

#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/pool/pool.hpp>
#include <servicelib/runtime/pool/userver_aliases.hpp>

namespace servicelib::pool {

class DelayPoolImpl final : public IDelayPool {
 private:
  enum class PoolState { kCreated, kRunning, kStopping, kStopped, kFailed };

  struct SharedState;
  struct DelayTask;
  static_assert(std::atomic<DelayTask*>::is_always_lock_free);
  using TimerQueue = std::multimap<std::chrono::steady_clock::time_point,
                                   std::shared_ptr<DelayTask>>;

  struct DelayTask {
    using CancelCallback = std::stop_callback<std::function<void()>>;

    std::shared_ptr<SharedState> state;
    Context ctx;
    std::function<void()> fn;
    std::atomic<bool> claimed{false};
    std::atomic<bool> cancelRequested{false};
    // Only coroutine code accesses the timer queue, under SharedState::mu.
    std::optional<TimerQueue::iterator> queued;
    std::atomic<bool> admitted{false};
    std::atomic<bool> cancellationPublished{false};
    // Intrusive MPSC cancellation inbox. The winning publisher owns these
    // fields until release-publishing the node; the scheduler consumes it.
    DelayTask* cancelledNext{};
    std::shared_ptr<DelayTask> cancellationKeepAlive;
    bool contextDeadlineWins{};

    // Preserve the same span and baggage capture as CriticalAsyncDetach,
    // at admission time, even though the coroutine is only created when due.
    // Allocate the wrapper only when servicelib tracing and sampling are on.
    std::unique_ptr<userver::utils::impl::SpanWrapCall> tracedCall;
    std::optional<userver::baggage::Baggage> baggage;
    std::optional<CancelCallback> cancelCallback;
    std::vector<std::unique_ptr<CancelCallback>> externalCancelCallbacks;
  };

  struct SharedState {
    explicit SharedState(IServiceEnvironment& environment) : env(environment) {
      const auto serviceSnapshot = env.getServiceConfigSnapshot();
      const auto* service = serviceSnapshot.get();
      metricsEnabled = env.getMetrics().enabled();
      auto scope = env.getMetrics().scope(
          "delay_pool", metrics::Labels{{"service", service ? service->name
                                                            : std::string()}});
      gaugeWaitQueueLength =
          scope->gauge("wait_queue_length", "Delay pool wait queue length");
      tasksTotal = scope->counter(
          "tasks_total", "Total number of tasks executed by delay pool");
      executionDuration =
          scope->histogram("task_execution_duration_seconds",
                           "Task execution duration in seconds");
      stopTimeoutCounter =
          scope->counter("events_total", "Total number of events in delay pool",
                         {{"event", "stop_timeout"}});
      taskCancelledCounter =
          scope->counter("events_total", "Total number of events in delay pool",
                         {{"event", "task_cancelled"}});
      taskRejectedCounter =
          scope->counter("events_total", "Total number of events in delay pool",
                         {{"event", "task_rejected"}});
    }

    IServiceEnvironment& env;
    engine::Mutex mu;
    engine::ConditionVariable cv;
    PoolState poolState = PoolState::kCreated;
    std::int64_t pending = 0;
    bool metricsEnabled{};

    std::unique_ptr<metrics::Int64Gauge> gaugeWaitQueueLength;
    std::unique_ptr<metrics::Int64Counter> tasksTotal;
    std::unique_ptr<metrics::Float64Histogram> executionDuration;
    std::unique_ptr<metrics::Int64Counter> stopTimeoutCounter;
    std::unique_ptr<metrics::Int64Counter> taskCancelledCounter;
    std::unique_ptr<metrics::Int64Counter> taskRejectedCounter;

    // Timers and lifecycle share the coroutine-aware mutex above. Native
    // cancellation callbacks only publish to the atomic inbox and Send().
    TimerQueue timers;
    std::atomic<DelayTask*> cancelledHead{nullptr};
    engine::SingleConsumerEvent queueChanged;
    bool schedulerStopping = false;

    // Scheduler is separate: activeTasksApprox counts executing callbacks,
    // not the one service-wide waiter or the queued timer records.
    std::optional<concurrent::BackgroundTaskStorageCore> tasks;
    std::optional<engine::TaskWithResult<void>> scheduler;
  };

 public:
  explicit DelayPoolImpl(IServiceEnvironment& env)
      : state_(std::make_shared<SharedState>(env)) {}

  ~DelayPoolImpl() override {
    const auto state = state_;
    std::unique_lock<engine::Mutex> lock(state->mu);
    const bool unused =
        state->poolState == PoolState::kCreated && !state->tasks.has_value();
    if (!unused && state->poolState != PoolState::kStopped) {
      utils::AbortWithStacktrace(
          "DelayPoolImpl must be stopped before destruction");
    }
  }

  void start([[maybe_unused]] Context ctx) override {
    const auto state = state_;
    std::unique_lock<engine::Mutex> lock(state->mu);
    switch (state->poolState) {
      case PoolState::kCreated:
        break;
      case PoolState::kStopping:
      case PoolState::kStopped:
        throw PoolStoppedError();
      case PoolState::kRunning:
      case PoolState::kFailed:
        throw PoolAlreadyStartedError();
    }

    ensureTaskStorageLocked(state);
    state->poolState = PoolState::kRunning;
  }

  void stop(Context ctx) override {
    const auto state = state_;
    if (const auto* executingPool = currentExecutingPool_.GetOptional();
        executingPool && *executingPool == state.get()) {
      throw PoolSelfStopError();
    }

    engine::TaskCancellationBlocker cancellationBlocker;
    bool ownsStop = false;
    bool timedOut = false;
    {
      std::unique_lock<engine::Mutex> lock(state->mu);
      if (state->poolState == PoolState::kStopped) {
        return;
      }
      if (state->poolState == PoolState::kStopping) {
        timedOut = !waitWithContext(*state, lock, ctx, [state] {
          return state->poolState == PoolState::kStopped;
        });
      } else {
        state->poolState = PoolState::kStopping;
        ownsStop = true;
        timedOut = !waitWithContext(*state, lock, ctx,
                                    [state] { return state->pending == 0; });
      }
    }

    if (timedOut) {
      recordStopTimeout(state);
    }
    if (!ownsStop) {
      // Another caller owns the transition. A deadline is diagnostic only:
      // no accepted callback may continue after the execution graph is freed.
      std::unique_lock<engine::Mutex> lock(state->mu);
      static_cast<void>(state->cv.Wait(
          lock, [state] { return state->poolState == PoolState::kStopped; }));
      return;
    }
    finishStop(state);
  }

  void delay(Context ctx, Duration delayDuration,
             std::function<void()> fn) override {
    const auto state = state_;
    const auto now = std::chrono::steady_clock::now();
    if (ctx.cancelled()) {
      rejectCancelled(state);
    }

    auto runAt = saturatedAdd(now, delayDuration);
    bool contextDeadlineWins = false;
    if (const auto& contextDeadline = ctx.deadline();
        contextDeadline.has_value() && *contextDeadline < runAt) {
      runAt = *contextDeadline;
      contextDeadlineWins = true;
    }

    if (runAt <= now && ctx.deadline().has_value() && *ctx.deadline() <= now) {
      rejectCancelled(state);
    }

    auto task = std::make_shared<DelayTask>();
    task->state = state;
    task->ctx = std::move(ctx);
    task->fn = std::move(fn);
    task->contextDeadlineWins = contextDeadlineWins;
    using SpanCall = userver::utils::impl::SpanWrapCall;
    if (task->ctx.samplingEnabled() && state->env.getTracing()) {
      task->tracedCall = std::make_unique<SpanCall>(
          runAt <= now ? "delay-pool-task" : "delay-pool-timer",
          SpanCall::InheritVariables::kNo,
          userver::utils::impl::SourceLocation::Current(),
          SpanCall::HideSpan::kNo);
    } else if (const auto* baggage =
                   userver::baggage::kInheritedBaggage.GetOptional()) {
      // Baggage propagation is independent of tracing/sampling.
      task->baggage.emplace(*baggage);
    }

    // Callback construction may allocate and may synchronously invoke the
    // callback for an already-stopped token. It does not need the pool lock.
    if (runAt > now) {
      std::weak_ptr<DelayTask> weakTask(task);
      const auto onCancel = [weakTask]() noexcept {
        if (const auto locked = weakTask.lock()) {
          bool expected = false;
          if (locked->cancelRequested.compare_exchange_strong(
                  expected, true, std::memory_order_acq_rel)) {
            if (locked->admitted.load(std::memory_order_acquire)) {
              publishCancellation(locked);
            }
          }
        }
      };
      if (task->ctx.stopToken().stop_possible()) {
        task->cancelCallback.emplace(task->ctx.stopToken(), onCancel);
      }
      task->externalCancelCallbacks.reserve(
          task->ctx.externalStopTokens().size());
      for (const auto& token : task->ctx.externalStopTokens()) {
        if (token.stop_possible()) {
          task->externalCancelCallbacks.push_back(
              std::make_unique<DelayTask::CancelCallback>(token, onCancel));
        }
      }
    }

    std::unique_lock<engine::Mutex> lock(state->mu);
    if (state->poolState == PoolState::kStopping ||
        state->poolState == PoolState::kStopped) {
      lock.unlock();
      if (state->metricsEnabled) {
        bestEffort([state] { state->taskRejectedCounter->inc(); });
      }
      throw PoolStoppedError();
    }
    if (state->poolState == PoolState::kFailed) {
      lock.unlock();
      if (state->metricsEnabled) {
        bestEffort([state] { state->taskRejectedCounter->inc(); });
      }
      throw PoolNotStartedError();
    }

    ensureTaskStorageLocked(state);
    ++state->pending;
    try {
      if (runAt <= now) {
        // Immediate work still gets its own task, without a timer-queue hop.
        dispatch(state, task);
      } else {
        // Cancellation may have happened while its callbacks were registered,
        // before this task had a queue node.
        const auto key = task->cancelRequested.load(std::memory_order_acquire)
                             ? std::chrono::steady_clock::time_point::min()
                             : runAt;
        const auto it = state->timers.emplace(key, task);
        task->queued = it;
        task->admitted.store(true, std::memory_order_release);
        // Close the race with a cancellation that saw admitted == false.
        if (task->cancelRequested.load(std::memory_order_acquire)) {
          publishCancellation(task);
        }
        if (it == state->timers.begin()) state->queueChanged.Send();
      }
    } catch (...) {
      --state->pending;
      publishPendingGaugeLocked(*state);
      state->cv.NotifyAll();
      throw;
    }

    publishPendingGaugeLocked(*state);
  }

  [[nodiscard]] std::int64_t activeTasksApprox() const noexcept {
    const auto state = state_;
    return state->tasks ? state->tasks->ActiveTasksApprox() : 0;
  }

 private:
  template <typename Callback>
  static void bestEffort(Callback&& callback) noexcept {
    try {
      std::forward<Callback>(callback)();
    } catch (...) {
    }
  }

  static void ensureTaskStorageLocked(
      const std::shared_ptr<SharedState>& state) {
    if (state->tasks) return;
    try {
      state->tasks.emplace();
      state->scheduler.emplace(
          engine::CriticalAsyncNoTracing([state] { schedulerLoop(state); }));
    } catch (...) {
      state->tasks.reset();
      state->poolState = PoolState::kFailed;
      throw;
    }
  }

  static void publishCancellation(
      const std::shared_ptr<DelayTask>& task) noexcept {
    if (task->cancellationPublished.exchange(true, std::memory_order_acq_rel)) {
      return;
    }
    const auto& state = task->state;
    // No allocation or coroutine mutex in a std::stop_callback. This owning
    // reference keeps the intrusive node alive even if execution finishes
    // before the scheduler drains its cancellation notification.
    task->cancellationKeepAlive = task;
    auto* head = state->cancelledHead.load(std::memory_order_relaxed);
    do {
      task->cancelledNext = head;
    } while (!state->cancelledHead.compare_exchange_weak(
        head, task.get(), std::memory_order_release,
        std::memory_order_relaxed));
    state->queueChanged.Send();
  }

  static void drainCancellations(const std::shared_ptr<SharedState>& state) {
    auto* head =
        state->cancelledHead.exchange(nullptr, std::memory_order_acquire);
    while (head) {
      auto task = std::move(head->cancellationKeepAlive);
      head = head->cancelledNext;
      {
        std::unique_lock<engine::Mutex> lock(state->mu);
        if (task->queued) {
          auto node = state->timers.extract(*task->queued);
          node.key() = std::chrono::steady_clock::time_point::min();
          task->queued = state->timers.insert(std::move(node));
        }
      }
      // Release ownership outside the queue lock.
    }
  }

  static void dispatch(const std::shared_ptr<SharedState>& state,
                       const std::shared_ptr<DelayTask>& task) {
    auto callback = engine::CriticalAsyncNoTracing([task] {
      // Destroy the wrapper on the coroutine it attached to, even when other
      // references to DelayTask still exist.
      auto& tracedCall = task->tracedCall;
      const auto invoke = [&] {
        execute(task,
                task->contextDeadlineWins ||
                    task->cancelRequested.load(std::memory_order_acquire));
      };
      if (tracedCall) {
        (*tracedCall)(invoke);
        tracedCall.reset();
      } else {
        if (task->baggage) {
          userver::baggage::kInheritedBaggage.Set(std::move(*task->baggage));
          task->baggage.reset();
        }
        invoke();
      }
    });
    state->tasks->Detach(std::move(callback).AsTask());
  }

  static void schedulerLoop(const std::shared_ptr<SharedState>& state) {
    // Shutdown is explicit, after all accepted callbacks have drained.
    engine::TaskCancellationBlocker cancellationBlocker;
    unsigned dispatched = 0;
    for (;;) {
      if (dispatched == 64) {
        // A large ready batch must not monopolize a task-processor worker.
        engine::Yield();
        dispatched = 0;
      }
      drainCancellations(state);
      TimerQueue::node_type ready;
      engine::Deadline next;
      {
        std::unique_lock<engine::Mutex> lock(state->mu);
        if (state->schedulerStopping) {
          lock.unlock();
          // pending == 0 means all callback registrations have been removed.
          // Drain publications racing the previous exchange before exiting.
          drainCancellations(state);
          return;
        }
        if (!state->timers.empty()) {
          const auto it = state->timers.begin();
          if (it->first <= std::chrono::steady_clock::now()) {
            it->second->queued.reset();
            ready = state->timers.extract(it);
          } else {
            next = engine::Deadline::FromTimePoint(it->first);
          }
        }
      }
      if (ready.empty()) {
        // Send is sticky: an insertion/cancellation between inspection and
        // waiting cannot be lost. Only this coroutine consumes the event.
        static_cast<void>(state->queueChanged.WaitForEventUntil(next));
        continue;
      }

      const auto task = ready.mapped();
      try {
        dispatch(state, task);
        ++dispatched;
      } catch (...) {
        // Admission already succeeded. Do not lose the callback or block the
        // scheduler inside user code if coroutine allocation temporarily fails.
        // Reuse the extracted map node, so retry itself needs no allocation.
        {
          std::unique_lock<engine::Mutex> lock(state->mu);
          ready.key() = std::chrono::steady_clock::now();
          task->queued = state->timers.insert(std::move(ready));
        }
        static_cast<void>(
            state->queueChanged.WaitForEventFor(std::chrono::milliseconds{1}));
      }
    }
  }

  template <typename Predicate>
  static bool waitWithContext(SharedState& state,
                              std::unique_lock<engine::Mutex>& lock,
                              const Context& ctx, Predicate&& predicate) {
    if (const auto& deadline = ctx.deadline(); deadline.has_value()) {
      return state.cv.WaitUntil(lock, *deadline,
                                std::forward<Predicate>(predicate));
    }
    static_cast<void>(state.cv.Wait(lock, std::forward<Predicate>(predicate)));
    return true;
  }

  static void recordStopTimeout(const std::shared_ptr<SharedState>& state) {
    bestEffort([state] {
      state->env.getLogger().warn("delay pool stopped by timeout");
    });
    if (state->metricsEnabled) {
      bestEffort([state] { state->stopTimeoutCounter->inc(); });
    }
  }

  static void finishStop(const std::shared_ptr<SharedState>& state) {
    engine::TaskCancellationBlocker cancellationBlocker;
    {
      std::unique_lock<engine::Mutex> lock(state->mu);
      static_cast<void>(
          state->cv.Wait(lock, [state] { return state->pending == 0; }));
    }

    if (state->scheduler) {
      {
        std::unique_lock<engine::Mutex> lock(state->mu);
        state->schedulerStopping = true;
        state->queueChanged.Send();
      }
      state->scheduler->Get();
      state->scheduler.reset();
    }
    if (state->tasks) {
      state->tasks->CancelAndWait();
    }

    {
      std::unique_lock<engine::Mutex> lock(state->mu);
      state->poolState = PoolState::kStopped;
      state->cv.NotifyAll();
    }
  }

  static std::chrono::steady_clock::time_point saturatedAdd(
      std::chrono::steady_clock::time_point now, Duration delayDuration) {
    if (delayDuration <= Duration::zero()) {
      return now;
    }
    const auto maxDelay = std::chrono::steady_clock::time_point::max() - now;
    return delayDuration >= maxDelay
               ? std::chrono::steady_clock::time_point::max()
               : now + delayDuration;
  }

  [[noreturn]] static void rejectCancelled(
      const std::shared_ptr<SharedState>& state) {
    if (state->metricsEnabled) {
      bestEffort([state] { state->taskRejectedCounter->inc(); });
    }
    throw PoolCancelledError();
  }

  static void publishPendingGaugeLocked(SharedState& state) noexcept {
    if (!state.metricsEnabled) return;
    bestEffort([&state] { state.gaugeWaitQueueLength->set(state.pending); });
  }

  static void execute(const std::shared_ptr<DelayTask>& task, bool expedited) {
    bool expected = false;
    if (!task->claimed.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel)) {
      return;
    }

    task->cancelCallback.reset();
    task->externalCancelCallbacks.clear();

    const auto state = task->state;
    const void*& currentPool = *currentExecutingPool_;
    struct CurrentPoolGuard final {
      const void*& slot;
      const void* previous;
      ~CurrentPoolGuard() { slot = previous; }
    } currentPoolGuard{currentPool, currentPool};
    currentPool = state.get();

    const auto startedAt = state->metricsEnabled
                               ? std::chrono::steady_clock::now()
                               : std::chrono::steady_clock::time_point{};
    try {
      task->fn();
    } catch (const std::exception& error) {
      bestEffort([state, &error] {
        state->env.getLogger().warn(
            "delay pool task error",
            {log::Field::Str("pool", "delay"), log::Field::Err(error)});
      });
    } catch (...) {
      bestEffort([state] {
        state->env.getLogger().warn("delay pool task error",
                                    {log::Field::Str("pool", "delay"),
                                     log::Field::Str("error", "<unknown>")});
      });
    }
    task->fn = nullptr;

    if (state->metricsEnabled) {
      bestEffort([state] { state->tasksTotal->inc(); });
      const double elapsed = std::chrono::duration<double>(
                                 std::chrono::steady_clock::now() - startedAt)
                                 .count();
      bestEffort(
          [state, elapsed] { state->executionDuration->observe(elapsed); });
      if (expedited) {
        bestEffort([state] { state->taskCancelledCounter->inc(); });
      }
    }

    {
      std::unique_lock<engine::Mutex> lock(state->mu);
      --state->pending;
      publishPendingGaugeLocked(*state);
      state->cv.NotifyAll();
    }
  }

  inline static engine::TaskLocalVariable<const void*> currentExecutingPool_;
  std::shared_ptr<SharedState> state_;
};

inline std::unique_ptr<IDelayPool> makeDelayPool(IServiceEnvironment& env) {
  return std::make_unique<DelayPoolImpl>(env);
}

}  // namespace servicelib::pool

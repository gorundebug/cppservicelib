/*
 * delaypool.hpp
 * C++ streams API — service-wide delayed task scheduler.
 *
 * Mirrors servicelib/runtime/pool/delaypool.go: every accepted call owns an
 * independent timer path, context completion may execute the task early, and
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
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>

#include <userver/concurrent/background_task_storage.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/condition_variable.hpp>
#include <userver/engine/deadline.hpp>
#include <userver/engine/mutex.hpp>
#include <userver/engine/single_use_event.hpp>
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

  struct DelayTask {
    using CancelCallback = std::stop_callback<std::function<void()>>;

    std::shared_ptr<SharedState> state;
    Context ctx;
    std::function<void()> fn;
    std::atomic<bool> claimed{false};
    std::atomic<bool> cancelRequested{false};
    engine::SingleUseEvent cancelled;
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

    // Destroyed before everything captured by scheduled work.
    std::optional<concurrent::BackgroundTaskStorage> tasks;
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

    ensureTaskStorageLocked(*state);
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

    // Callback construction may allocate and may synchronously invoke the
    // callback for an already-stopped token. It does not need the pool lock.
    if (runAt > now) {
      std::weak_ptr<DelayTask> weakTask(task);
      const auto onCancel = [weakTask]() noexcept {
        if (const auto locked = weakTask.lock()) {
          bool expected = false;
          if (locked->cancelRequested.compare_exchange_strong(
                  expected, true, std::memory_order_acq_rel)) {
            locked->cancelled.Send();
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

    ensureTaskStorageLocked(*state);
    ++state->pending;
    try {
      if (runAt <= now) {
        state->tasks->CriticalAsyncDetach("delay-pool-task",
                                          [task] { execute(task, false); });
      } else {
        state->tasks->CriticalAsyncDetach(
            "delay-pool-timer", [task, runAt, contextDeadlineWins] {
              const auto waitResult = task->cancelled.WaitUntil(
                  engine::Deadline::FromTimePoint(runAt));
              execute(task, contextDeadlineWins ||
                                waitResult != engine::FutureStatus::kTimeout);
            });
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

  static void ensureTaskStorageLocked(SharedState& state) {
    if (state.tasks) {
      return;
    }
    try {
      state.tasks.emplace(engine::current_task::GetTaskProcessor());
    } catch (...) {
      state.poolState = PoolState::kFailed;
      throw;
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

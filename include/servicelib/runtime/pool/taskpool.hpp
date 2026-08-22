/*
 * taskpool.hpp
 * C++ streams API — FIFO task pool with a bounded, dynamically resizable
 * set of executor coroutines.
 *
 * Logic mirrors servicelib's Go implementation
 * (runtime/pool/taskpool.go): a doubly linked FIFO queue guarded by a
 * mutex+condvar, N executor coroutines draining it, a manager coroutine
 * that re-reads PoolConfig.ExecutorsCount once a second and hot-resizes the
 * executor set, and context cancellation/deadline handling that requeues a
 * waiting task to the front (Go analog: context.AfterFunc-triggered
 * requeue-to-head, firing on ctx.Done() — deadline OR explicit cancellation).
 *
 * Departures from a literal Go port:
 *  - Resize is incremental (spawn the delta on growth; excess executors
 *    self-select to exit via an allocated/target counter pair) instead of
 *    Go's "flag all old executors to restart, spawn a full new batch
 *    immediately" — which transiently exceeds the configured executor
 *    count by design. That's fine for Go's original use case but not
 *    for this port, where PoolConfig.executorsCount is meant to be a
 *    hard concurrency ceiling.
 *  - Cancellation is tracked even after enqueue (Go's context.AfterFunc
 *    fires on ctx.Done(), i.e. deadline *or* cancel). See addTask's
 *    std::stop_callback.
 *  - Deadlines are kept in one min-heap and promoted lazily immediately
 *    before an executor dequeues its next task. A per-task sleeping coroutine
 *    is unnecessary: while all executors are busy, moving a queued task
 *    cannot make it execute; once one is free, heap promotion produces the
 *    same observable execution order without per-task timer/coroutine cost.
 *  - start()/stop()/addTask() are governed by an explicit PoolState state
 *    machine (kCreated -> kStarting -> kRunning -> kDraining/kStopping ->
 * kStopped, or -> kFailed if managerLoop never reaches kRunning) instead of
 *    two independent booleans, so "accepting tasks", "still starting up",
 *    and "failed to start" are always distinguishable — a plain bool can't
 *    tell them apart, which risks the pool silently accepting tasks with
 *    zero executors ever running them.
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

// Optional zero-cost hooks used by the standalone benchmark to sample queue
// mutex wait/hold time. Production builds leave them undefined, so no probe
// state, clock reads or branches are emitted.
#ifndef STREAM_TASKPOOL_LOCK_PROFILE_BEGIN
#define STREAM_TASKPOOL_LOCK_PROFILE_BEGIN(operation)
#define STREAM_TASKPOOL_LOCK_PROFILE_ACQUIRED()
#define STREAM_TASKPOOL_LOCK_PROFILE_RESUMED()
#define STREAM_TASKPOOL_LOCK_PROFILE_RELEASING()
#define STREAM_TASKPOOL_LOCK_PROFILE_COMMIT()
#endif

#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/pool/pool.hpp>
#include <servicelib/runtime/pool/userver_aliases.hpp>

namespace servicelib::pool {

class TaskPoolImpl final : public ITaskPool {
 public:
  TaskPoolImpl(std::string name, IServiceEnvironment& env)
      : name_(std::move(name)), env_(env) {
    const auto cfgSnapshot = env_.getRuntimeConfigSnapshot();
    const auto* cfg = cfgSnapshot.get();
    if (!cfg || !cfg->GetPoolByName(name_)) {
      throw std::invalid_argument("task pool configuration named '" + name_ +
                                  "' not found");
    }

    const auto svcSnapshot = env_.getServiceConfigSnapshot();
    const auto* svc = svcSnapshot.get();
    metricsEnabled_ = env_.getMetrics().enabled();
    auto scope = env_.getMetrics().scope(
        "task_pool",
        metrics::Labels{{"service", svc ? svc->name : std::string()},
                        {"name", name_}});
    gaugeQueueLength_ =
        scope->gauge("queue_length", "Task pool wait queue length");
    gaugeExecutorsTarget_ = scope->gauge(
        "executors_target", "Desired number of task pool executors");
    gaugeExecutorsAllocated_ = scope->gauge(
        "executors_allocated", "Number of live task pool executors");
    gaugeExecutorsBusy_ = scope->gauge(
        "executors_busy", "Number of task pool executors running callbacks");
    tasksTotal_ = scope->counter("tasks_total",
                                 "Total number of tasks executed by task pool");
    executionDuration_ = scope->histogram("task_execution_duration_seconds",
                                          "Task execution duration in seconds");
    stopTimeoutCounter_ =
        scope->counter("events_total", "Total number of events in task pool",
                       {{"event", "stop_timeout"}});
    taskRejectedCounter_ =
        scope->counter("events_total", "Total number of events in task pool",
                       {{"event", "task_rejected"}});
    taskCancelledCounter_ =
        scope->counter("events_total", "Total number of events in task pool",
                       {{"event", "task_cancelled"}});
  }

  ~TaskPoolImpl() override {
    const PoolState state = state_.load(std::memory_order_acquire);
    if (state != PoolState::kCreated && state != PoolState::kStopped) {
      utils::AbortWithStacktrace(
          "TaskPoolImpl must be stopped before destruction");
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

    // Uninterruptible, and covers the whole spawn+wait sequence: Go's
    // Start() blocks unconditionally on <-startedCh, with no cancellation
    // escape. CriticalAsync (not plain Async) guarantees managerLoop's
    // body actually starts running — including constructing the
    // SignalReadyOnExit guard below — even if this task, its TaskProcessor,
    // or a racing stop() requests its cancellation before it would
    // otherwise get scheduled. Without both of these, start() could hang
    // forever waiting for a readyEvent_.Send() that nothing ever sends.
    {
      engine::TaskCancellationBlocker blocker;
      try {
        taskProcessor_ = &engine::current_task::GetTaskProcessor();
        cancelWatches_.emplace(*taskProcessor_);
        managerTask_ =
            engine::CriticalAsyncNoTracing([this, ctx] { managerLoop(ctx); });
      } catch (...) {
        // Publish first (unblocks any concurrent stop() parked on
        // fieldsPublished_ below), then fail the state transition via CAS
        // — not an unconditional store, since a concurrent stop() may have
        // already claimed kStopping out from under us.
        fieldsPublished_.Send();
        PoolState fromStarting = PoolState::kStarting;
        state_.compare_exchange_strong(fromStarting, PoolState::kFailed,
                                       std::memory_order_acq_rel);
        throw;
      }
      // Publishes taskProcessor_/cancelWatches_/managerTask_ to a
      // concurrent stop() that raced past the kCreated->kStarting CAS
      // above while we were still writing them. state_'s CAS only orders
      // the state value itself, not these three plain fields — without
      // this handshake, a concurrent stop() reading them would be a data
      // race. See stop()'s matching WaitForEvent() below.
      fieldsPublished_.Send();
      static_cast<void>(readyEvent_.WaitForEvent());
    }

    // CAS (not unconditional store): a concurrent stop() may have already
    // driven state_ past kStarting (to kStopping/kStopped) by the time we
    // wake up here — e.g. managerLoop resolved kReady and sent the event,
    // then stop() ran start-to-finish (including tearing down
    // cancelWatches_) before we got scheduled again. Overwriting that with
    // kRunning would resurrect an already-fully-stopped pool. If the CAS
    // fails, someone else already owns the state transition from here —
    // just report accordingly without touching state_ again.
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
        throw std::runtime_error("failed to start task pool '" + name_ + "'");
    }
  }

  void stop(Context ctx) override {
    // Uninterruptible for its whole duration, for the same reason as
    // start(): stop() must be immune to cancellation of *its own* calling
    // task (e.g. a request handler whose context gets cancelled mid-call),
    // as opposed to the timeout it explicitly honors via ctx.deadline()
    // below. WaitNothrow is used during draining, so task failures cannot
    // leave the pool permanently stuck in kStopping.
    engine::TaskCancellationBlocker cancellationBlocker;

    // The state transition and done_ publication share mu_ with addTask's
    // final admission check. Whichever side acquires mu_ first defines the
    // linearization order: no task can be accepted after kStopping.
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
      // We won the CAS out of kStarting, i.e. a concurrent start() call is
      // still in progress (or just failed) on this pool. Wait for it to
      // finish publishing taskProcessor_/cancelWatches_/managerTask_ (see
      // start()'s fieldsPublished_.Send(), including on its exception
      // path) before touching any of them below — otherwise this is a
      // data race on plain, unsynchronized fields.
      static_cast<void>(fieldsPublished_.WaitForEvent());
    }

    if (managerTask_.IsValid()) {
      // Defense in depth alongside CriticalAsync in start(): if managerLoop
      // somehow never resolves startup on its own before we cancel it,
      // resolve it here first so a concurrent start() can't be left
      // waiting on a Send() that SyncCancel below might otherwise race.
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
          int64_t tasksLeft = 0;
          {
            std::unique_lock<engine::Mutex> lock(mu_);
            tasksLeft = count_;
          }
          bestEffortTelemetry([this, tasksLeft] {
            env_.getLogger().warn(
                "task pool stopped by timeout",
                {log::Field::Str("pool", name_),
                 log::Field::Int64("tasks_count", tasksLeft)});
          });
          bestEffortMetrics([this] { stopTimeoutCounter_->inc(); });
        }
      }
      // Unconditional final wait — mirrors Go's trailing p.wg.Wait() after
      // the timeout branch: the timeout above is only for logging/metrics,
      // draining always runs to completion.
      for (auto& task : toWait) {
        static_cast<void>(task.WaitNothrow());
      }
    }

    // By now every task has been dequeued (executors only exit due to
    // done_ once count_ == 0), which means every task's cancelCallback has
    // either already fired or been reset() — so no more AsyncDetach calls
    // can happen on cancelWatches_ from this point on, satisfying its
    // "must not be launched after CancelAndWait() returns" contract.
    // Guarded: cancelWatches_ is unset if start() never got far enough to
    // set it (e.g. stop() called on a kCreated pool).
    if (cancelWatches_) {
      cancelWatches_->CancelAndWait();  // userver API: noexcept
    }

    state_.store(PoolState::kStopped, std::memory_order_release);
    {
      // Wakes any concurrent stop() callers parked in the kStopping wait
      // above.
      std::unique_lock<engine::Mutex> lock(mu_);
      cv_.NotifyAll();
    }
  }

  void addTask(Context ctx, std::function<void()> fn) override {
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
      // Already expired — accepting it only to promote it immediately would
      // contradict Go's initial ctx.Err() admission check.
      bestEffortMetrics([this] { taskRejectedCounter_->inc(); });
      throw PoolCancelledError();
    }

    auto task = std::make_shared<PoolTask>();
    task->fn = std::move(fn);

    // mu_ is held continuously from here through insertion (or rejection)
    // below — including across callback construction. Releasing it
    // in between would leave a window where stop() can run its *entire*
    // shutdown sequence — done_=true, drain executors_,
    // cancelWatches_->CancelAndWait() — while this call is still building
    // task->cancelCallback. The callback would not yet be visible to that
    // shutdown sequence, so this call could be rejected after the fact while
    // its callback later calls AsyncDetach on a BackgroundTaskStorage that
    // has already returned from CancelAndWait() — undefined per its
    // documented contract.
    // Holding mu_ throughout closes this: stop()'s done_=true is also
    // mu_-guarded, so it can only happen strictly before or strictly after
    // this whole critical section, never in the middle of it.
    STREAM_TASKPOOL_LOCK_PROFILE_BEGIN(0);
    std::unique_lock<engine::Mutex> lock(mu_);
    STREAM_TASKPOOL_LOCK_PROFILE_ACQUIRED();

    if (done_ ||
        state_.load(std::memory_order_acquire) != PoolState::kRunning) {
      lock.unlock();
      bestEffortMetrics([this] { taskRejectedCounter_->inc(); });
      throw PoolStoppedError();
    }
    // Re-check here too: ctx could have been cancelled (or its deadline
    // could have passed) between the pre-checks above and acquiring mu_.
    if (const auto& d = ctx.deadline();
        ctx.cancelled() ||
        (d.has_value() && *d <= std::chrono::steady_clock::now())) {
      lock.unlock();
      bestEffortMetrics([this] { taskRejectedCounter_->inc(); });
      throw PoolCancelledError();
    }

    // Built while still holding mu_ (see above), and before the task is
    // inserted, so that if stop_callback construction or deadline-heap
    // growth throws, addTask() still has a strong exception guarantee: the
    // task was never made visible to executors, so the caller can safely
    // treat the exception as "not accepted" without risking a silent
    // duplicate if it retries. Firing early (before insertion) is safe:
    // requeueToFront's inQueue guard makes an early fire a no-op.
    // Reacts to cancellation *after* enqueue — Go's context.AfterFunc fires
    // on ctx.Done() (deadline or explicit cancel). Context's explicit stop
    // token covers the cancellation half; the deadline heap below covers the
    // deadline half. A no-op if ctx carries no real
    // stop-state. May run on an arbitrary thread (whoever calls
    // request_stop()), so it must not touch engine::Mutex directly —
    // cancelWatches_->CriticalAsyncDetach explicitly supports being called from
    // a non-coroutine thread. The try/catch is load-bearing: an exception
    // escaping a std::stop_callback invocation terminates the process. If
    // ctx is already stop-requested, this fires synchronously right here
    // (still under mu_) — safe: CriticalAsyncDetach only schedules a coroutine,
    // it doesn't itself touch mu_ or block.
    std::weak_ptr<PoolTask> weakTaskForCancel(task);
    const auto onCancel = [this, weakTaskForCancel]() noexcept {
      try {
        cancelWatches_->CriticalAsyncDetach(
            name_ + "-cancel-watch",
            [this, weakTaskForCancel] { requeueToFront(weakTaskForCancel); });
      } catch (...) {
        // Nothing safe to do with the failure here (unknown calling
        // thread — may not even be a coroutine); dropping the promotion
        // just means this task keeps its original queue position.
      }
    };
    task->cancelCallback.emplace(ctx.stopToken(), onCancel);
    task->externalCancelCallbacks.reserve(ctx.externalStopTokens().size());
    for (const auto& token : ctx.externalStopTokens()) {
      task->externalCancelCallbacks.push_back(
          std::make_unique<PoolTask::CancelCallback>(token, onCancel));
    }

    if (const auto& deadline = ctx.deadline(); deadline.has_value()) {
      task->deadline = *deadline;
      deadlineHeapPushLocked(task);
    }

    if (tail_) {
      tail_->next = task;
      task->prev = tail_;
      tail_ = task.get();
    } else {
      head_ = task;
      tail_ = task.get();
    }
    task->inQueue = true;
    ++count_;
    bestEffortMetrics([this] { gaugeQueueLength_->set(count_); });

    STREAM_TASKPOOL_LOCK_PROFILE_RELEASING();
    lock.unlock();
    STREAM_TASKPOOL_LOCK_PROFILE_COMMIT();
    cv_.NotifyOne();
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

  static constexpr std::size_t kNotInDeadlineHeap =
      std::numeric_limits<std::size_t>::max();

  struct PoolTask {
    using CancelCallback = std::stop_callback<std::function<void()>>;

    std::function<void()> fn;
    std::shared_ptr<PoolTask> next;  // owning forward link
    PoolTask* prev = nullptr;        // non-owning backward link
    bool inQueue = false;
    bool expedited =
        false;  // true once already moved to the front — see requeueToFront
    Deadline deadline;
    std::size_t deadlineIndex = kNotInDeadlineHeap;
    std::optional<CancelCallback> cancelCallback;
    std::vector<std::unique_ptr<CancelCallback>> externalCancelCallbacks;
  };

  static bool deadlineEarlier(const std::shared_ptr<PoolTask>& lhs,
                              const std::shared_ptr<PoolTask>& rhs) noexcept {
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
      std::size_t smallest = left;
      if (right < size &&
          deadlineEarlier(deadlineHeap_[right], deadlineHeap_[left])) {
        smallest = right;
      }
      if (!deadlineEarlier(deadlineHeap_[smallest], deadlineHeap_[index])) {
        return;
      }
      deadlineHeapSwapLocked(index, smallest);
      index = smallest;
    }
  }

  void deadlineHeapPushLocked(const std::shared_ptr<PoolTask>& task) {
    task->deadlineIndex = deadlineHeap_.size();
    try {
      deadlineHeap_.push_back(task);
    } catch (...) {
      task->deadlineIndex = kNotInDeadlineHeap;
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
    removed->deadlineIndex = kNotInDeadlineHeap;

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
      const std::shared_ptr<PoolTask>& task) noexcept {
    if (task->deadlineIndex != kNotInDeadlineHeap) {
      deadlineHeapRemoveAtLocked(task->deadlineIndex);
    }
  }

  // mu_ must be held. Returns true only for the first successful promotion.
  bool requeueToFrontLocked(const std::shared_ptr<PoolTask>& task) noexcept {
    if (!task->inQueue || task->expedited) {
      return false;
    }
    task->expedited = true;
    deadlineHeapRemoveLocked(task);

    if (head_.get() == task.get()) {
      return true;
    }

    if (task->next) {
      task->next->prev = task->prev;
    } else {
      tail_ = task->prev;
    }
    task->prev->next = task->next;

    task->next = head_;
    task->prev = nullptr;
    head_->prev = task.get();
    head_ = task;
    return true;
  }

  std::size_t promoteExpiredDeadlinesLocked(
      std::chrono::steady_clock::time_point now) noexcept {
    std::size_t promoted = 0;
    while (!deadlineHeap_.empty() && *deadlineHeap_.front()->deadline <= now) {
      auto task = deadlineHeap_.front();
      deadlineHeapRemoveAtLocked(0);
      if (requeueToFrontLocked(task)) {
        ++promoted;
      }
    }
    return promoted;
  }

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

  // Requeues the task to the front of the list if it is still waiting.
  // Called from the cancellation callback. Deadline promotion uses the same
  // locked helper immediately before dequeue. Both triggers can legitimately
  // both can legitimately fire for the same task (a deadline elapsing
  // doesn't stop a subsequent explicit cancel, or vice versa). The
  // `expedited` flag (not just the inQueue/head_ position check) is what
  // makes the second call a true no-op: without it, task A could be
  // expedited to head_, task B expedited in front of it next, and then A's
  // *other* trigger firing later would move it back in front of B again —
  // an unbounded head-of-line race the head_-equality check alone doesn't
  // prevent once more than one task has been expedited. Go analog:
  // context.AfterFunc callback in taskpool.go (which itself only ever
  // fires once per context).
  void requeueToFront(const std::weak_ptr<PoolTask>& weakTask) {
    auto task = weakTask.lock();
    if (!task) {
      return;
    }

    std::unique_lock<engine::Mutex> lock(mu_);
    // Preserve the temporal ordering of Go's context.AfterFunc callbacks.
    // A deadline that elapsed before this explicit cancellation logically
    // promoted its task first, even if no executor was free to observe it.
    const std::size_t deadlinePromotions =
        deadlineHeap_.empty()
            ? 0
            : promoteExpiredDeadlinesLocked(std::chrono::steady_clock::now());
    const bool cancellationPromoted = requeueToFrontLocked(task);
    if (deadlinePromotions == 0 && !cancellationPromoted) {
      return;
    }

    lock.unlock();
    cv_.NotifyOne();
    const std::size_t totalPromotions =
        deadlinePromotions + (cancellationPromoted ? 1 : 0);
    for (std::size_t i = 0; i < totalPromotions; ++i) {
      bestEffortMetrics([this] { taskCancelledCounter_->inc(); });
    }
  }

  void runTask(const std::shared_ptr<PoolTask>& task) {
    struct BusyExecutorGuard final {
      TaskPoolImpl& pool;
      explicit BusyExecutorGuard(TaskPoolImpl& value) : pool(value) {
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
            "task pool task error",
            {log::Field::Str("pool", name_), log::Field::Err(e)});
      });
    } catch (...) {
      bestEffortTelemetry([this] {
        env_.getLogger().warn("task pool task error",
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
    explicit ExecutorSlotGuard(TaskPoolImpl& pool) : pool_(pool) {}
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
    TaskPoolImpl& pool_;
    bool released_ = false;
  };

  void executorLoop() {
    ExecutorSlotGuard slotGuard(*this);

    while (true) {
      STREAM_TASKPOOL_LOCK_PROFILE_BEGIN(1);
      std::unique_lock<engine::Mutex> lock(mu_);
      STREAM_TASKPOOL_LOCK_PROFILE_ACQUIRED();
      const bool woken = cv_.Wait(lock, [this] {
        return count_ != 0 || done_ ||
               allocatedExecutors_.load(std::memory_order_relaxed) >
                   targetExecutors_.load(std::memory_order_relaxed);
      });
      if (!woken) {
        // Cancelled while the predicate was still false, i.e. genuinely
        // idle: no task to hand back, nothing to shrink for. slotGuard's
        // destructor accounts for the decrement.
        return;
      }
      // cv_.Wait may have released the mutex while the executor was idle.
      // Reset the optional benchmark probe's hold-time origin after the
      // mutex has been reacquired; idle sleep is not lock hold time.
      STREAM_TASKPOOL_LOCK_PROFILE_RESUMED();

      // Taken under mu_ (lock is still held here, right after cv_.Wait
      // returns) — this is the *only* place shouldExitAsExcess() is
      // called. reconcileExecutors() writes targetExecutors_ under the
      // same mutex, so this decision can never race a concurrent resize.
      if (shouldExitAsExcess()) {
        slotGuard.release();
        return;
      }
      if (count_ == 0 && done_) {
        // Not "excess" — the whole pool is stopping. slotGuard's
        // destructor performs the decrement.
        return;
      }

      const std::size_t deadlinePromotions =
          deadlineHeap_.empty()
              ? 0
              : promoteExpiredDeadlinesLocked(std::chrono::steady_clock::now());

      auto task = head_;
      head_ = task->next;
      if (head_) {
        head_->prev = nullptr;
      } else {
        tail_ = nullptr;
      }
      task->next.reset();
      task->inQueue = false;
      deadlineHeapRemoveLocked(task);
      --count_;
      bestEffortMetrics([this] { gaugeQueueLength_->set(count_); });
      STREAM_TASKPOOL_LOCK_PROFILE_RELEASING();
      lock.unlock();
      STREAM_TASKPOOL_LOCK_PROFILE_COMMIT();

      for (std::size_t i = 0; i < deadlinePromotions; ++i) {
        bestEffortMetrics([this] { taskCancelledCounter_->inc(); });
      }
      task->cancelCallback.reset();
      task->externalCancelCallbacks.clear();

      runTask(task);
    }
  }

  // Reconciles all reserved executor slots (scheduled or running) against
  // target. It is intentionally called every tick so an executor that exits
  // concurrently with a target change is replenished on the next pass.
  //
  // All vector capacity is reserved before a slot is claimed. Consequently
  // every successfully created task handle can be adopted by executors_
  // without allocation, including when a later CriticalAsync call throws.
  // A failed call rolls back exactly its own slot and the exception is
  // rethrown only after all earlier handles have been adopted.
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
    // Guarantees start()'s WaitForEvent() is released on *every* exit path
    // of this function — including the earliest ones (already-cancelled,
    // already-done) that never reach the "spawned first batch" point.
    // Send() is idempotent (NoAutoReset), so firing it more than once
    // (e.g. also from the normal spawn path below) is harmless. Runs even
    // if this function is entered already-cancelled, since managerTask_ is
    // spawned via CriticalAsync (see start()).
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
      // ctx.cancelled() alone only covers explicit cancellation; Go's
      // ctx.Done() closes on deadline too (context.WithTimeout), so check
      // ctx.deadline() here as well — otherwise a pool started with a
      // lifecycle context that has since expired would keep re-reading
      // config and resizing forever instead of winding down.
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
      // Every tick, unconditionally — not just when newTarget changed from
      // the previous tick. See reconcileExecutors()'s comment for why.
      try {
        reconcileExecutors(newTarget);
      } catch (const std::exception& e) {
        bestEffortTelemetry([this, &e] {
          env_.getLogger().warn(
              "task pool executor resize failed",
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
          env_.getLogger().warn("task pool executor resize failed",
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
  std::shared_ptr<PoolTask> head_;  // owns the whole chain
  PoolTask* tail_ = nullptr;        // non-owning
  std::vector<std::shared_ptr<PoolTask>> deadlineHeap_;
  int64_t count_ = 0;
  bool done_ = false;

  std::atomic<PoolState> state_{PoolState::kCreated};
  std::atomic<StartupResult> startupResult_{StartupResult::kPending};
  std::atomic<int> targetExecutors_{0};
  // Includes both scheduled and already-running executors. A single count
  // is required: two atomics for live and pending cannot be snapshotted
  // consistently and can make reconciliation spuriously overspawn.
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
  std::unique_ptr<metrics::Int64Counter> taskCancelledCounter_;
  bool metricsEnabled_{};

  // Holds cancellation-triggered requeue tasks — see addTask(). Self-prunes
  // finished entries (unlike executors_, which we prune manually), and
  // explicitly supports launching from a non-coroutine thread. Declared
  // after the metrics fields (userver convention for
  // BackgroundTaskStorage): members are destroyed in reverse declaration
  // order, so this is torn down — and any CriticalAsyncDetach'd task touching
  // taskCancelledCounter_ fully drained — strictly before those counters
  // are destroyed.
  std::optional<concurrent::BackgroundTaskStorage> cancelWatches_;
};

inline std::unique_ptr<ITaskPool> makeTaskPool(std::string name,
                                               IServiceEnvironment& env) {
  return std::make_unique<TaskPoolImpl>(std::move(name), env);
}

}  // namespace servicelib::pool

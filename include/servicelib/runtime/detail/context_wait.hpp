#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>

#include <userver/engine/async.hpp>
#include <userver/engine/condition_variable.hpp>
#include <userver/engine/mutex.hpp>
#include <userver/engine/single_consumer_event.hpp>
#include <userver/engine/task/task_with_result.hpp>

#include <servicelib/runtime/context.hpp>

namespace servicelib::detail {

// Returns with lock held. Cancellation bounds the wait, not the operation
// being observed. Foreign-thread callbacks never acquire a coroutine mutex.
template <typename Predicate>
bool WaitWithContext(userver::engine::ConditionVariable& changed,
                     std::unique_lock<userver::engine::Mutex>& lock,
                     const Context& context, Predicate ready) {
  if (ready()) return true;
  if (context.cancelled()) return false;
  userver::engine::TaskCancellationBlocker blocker;
  userver::engine::SingleConsumerEvent cancelled;
  using Subscription = std::stop_callback<std::function<void()>>;
  std::vector<std::unique_ptr<Subscription>> subscriptions;
  userver::engine::TaskWithResult<void> notifier;
  auto* mutex = lock.mutex();
  const auto subscribe = [&](std::stop_token token) {
    if (token.stop_possible()) {
      subscriptions.push_back(std::make_unique<Subscription>(
          token, [&cancelled] { cancelled.Send(); }));
    }
  };
  subscribe(context.stopToken());
  for (const auto& token : context.externalStopTokens()) subscribe(token);
  if (!subscriptions.empty()) {
    notifier = userver::engine::AsyncNoTracing([&] {
      if (cancelled.WaitForEvent()) {
        std::unique_lock<userver::engine::Mutex> notificationLock(*mutex);
        changed.NotifyAll();
      }
    });
  }
  const auto finished = [&] { return ready() || context.cancelled(); };
  if (context.deadline()) {
    static_cast<void>(changed.WaitUntil(lock, *context.deadline(), finished));
  } else {
    static_cast<void>(changed.Wait(lock, finished));
  }
  if (!subscriptions.empty()) {
    lock.unlock();
    notifier.SyncCancel();
    lock.lock();
  }
  return ready();
}

// Serializes lifecycle transitions without replacing each caller's context.
class ContextGate final {
 public:
  class Lease final {
   public:
    explicit Lease(ContextGate& gate) : gate_(gate) {}
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    ~Lease() {
      userver::engine::TaskCancellationBlocker blocker;
      std::unique_lock<userver::engine::Mutex> lock(gate_.mutex_);
      gate_.busy_ = false;
      gate_.changed_.NotifyAll();
    }
   private:
    ContextGate& gate_;
  };

  Lease acquire(const Context& context) {
    std::unique_lock<userver::engine::Mutex> lock(mutex_);
    if (!WaitWithContext(changed_, lock, context, [&] { return !busy_; })) {
      throw std::runtime_error("service lifecycle wait cancelled or expired");
    }
    busy_ = true;
    return Lease(*this);
  }

 private:
  userver::engine::Mutex mutex_;
  userver::engine::ConditionVariable changed_;
  bool busy_{};
};
}  // namespace servicelib::detail

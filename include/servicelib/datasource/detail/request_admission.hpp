#pragma once

#include <atomic>
#include <cstddef>
#include <limits>
#include <mutex>
#include <utility>

#include <userver/engine/condition_variable.hpp>
#include <userver/engine/mutex.hpp>
#include <userver/engine/task/cancel.hpp>

#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/detail/context_wait.hpp>

namespace servicelib::datasource::detail {

// Listener ownership stays with userver. Close graph admission and drain
// accepted handlers before ServiceApp stops graph executors.
class RequestAdmission final {
 public:
  class Lease final {
   public:
    Lease() = default;
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    Lease(Lease&& other) noexcept : owner_(std::exchange(other.owner_, nullptr)) {}
    ~Lease() {
      if (owner_) owner_->release();
    }
    explicit operator bool() const noexcept { return owner_ != nullptr; }

   private:
    friend class RequestAdmission;
    explicit Lease(RequestAdmission& owner) : owner_(&owner) {}
    RequestAdmission* owner_{};
  };

  [[nodiscard]] Lease acquire() {
    auto state = state_.load(std::memory_order_acquire);
    while (!(state & kClosed)) {
      if (state_.compare_exchange_weak(state, state + 1,
                                      std::memory_order_acq_rel)) {
        return Lease{*this};
      }
    }
    return {};
  }

  void stop(const Context& context) {
    std::unique_lock lock(mutex_);
    state_.fetch_or(kClosed, std::memory_order_acq_rel);
    if (!servicelib::detail::WaitWithContext(changed_, lock, context, [&] {
          return state_.load(std::memory_order_acquire) == kClosed;
        })) {
      throw OperationCancelledError("datasource request drain cancelled or expired");
    }
  }

 private:
  void release() {
    auto state = state_.load(std::memory_order_acquire);
    while (!(state & kClosed)) {
      if (state_.compare_exchange_weak(state, state - 1,
                                      std::memory_order_acq_rel)) return;
    }
    // Publish zero under the same lock used by stop(). Otherwise stop could
    // return and destroy this gate before the last releaser notifies it.
    userver::engine::TaskCancellationBlocker blocker;
    std::unique_lock lock(mutex_);
    if (state_.fetch_sub(1, std::memory_order_acq_rel) == kClosed + 1) {
      changed_.NotifyAll();
    }
  }

  static constexpr auto kClosed = std::size_t{1}
      << (std::numeric_limits<std::size_t>::digits - 1);
  std::atomic<std::size_t> state_{};
  userver::engine::Mutex mutex_;
  userver::engine::ConditionVariable changed_;
};

}  // namespace servicelib::datasource::detail

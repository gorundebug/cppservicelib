#pragma once
#include <atomic>
#include <servicelib/runtime/context.hpp>
#include <userver/engine/single_use_event.hpp>
#include <userver/engine/task/cancel.hpp>
namespace servicelib::pulsar::detail {
// One native operation, one coroutine waiter. Native callbacks capture shared
// ownership of this state while Send runs; cancellation never destroys a
// sender.
class Event {
 public:
  userver::engine::SingleUseEvent& native() { return event_; }
  void Send() {
    complete_.store(true);
    Wake();
  }
  bool IsReady() const { return complete_.load(); }
  void Wake() {
    if (!sent_.exchange(true)) event_.Send();
  }
  void Wait(Context context = {}) {
    const auto wake = [this] { Wake(); };
    std::stop_callback primary(context.stopToken(), wake);
    std::vector<std::unique_ptr<std::stop_callback<decltype(wake)>>> external;
    for (auto token : context.externalStopTokens())
      external.push_back(
          std::make_unique<std::stop_callback<decltype(wake)>>(token, wake));
    auto deadline =
        context.deadline()
            ? userver::engine::Deadline::FromTimePoint(*context.deadline())
            : userver::engine::Deadline{};
    static_cast<void>(event_.WaitUntil(deadline));
    if (!IsReady() || context.cancelled() ||
        userver::engine::current_task::ShouldCancel())
      throw std::runtime_error(
          "Pulsar operation cancelled or deadline exceeded");
  }

 private:
  std::atomic<bool> sent_{}, complete_{};
  userver::engine::SingleUseEvent event_;
};
}  // namespace servicelib::pulsar::detail

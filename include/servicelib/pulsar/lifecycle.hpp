#pragma once
#include <servicelib/pulsar/client.hpp>
#include <servicelib/pulsar/wait.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <userver/concurrent/background_task_storage.hpp>
#include <userver/engine/condition_variable.hpp>
#include <userver/engine/mutex.hpp>
namespace servicelib::pulsar::detail {
class Gate : public std::enable_shared_from_this<Gate> {
 public:
  struct Lease {
    std::shared_ptr<Gate> gate;
    std::size_t bytes;
    ~Lease() { gate->release(bytes); }
  };
  explicit Gate(std::size_t limit) : limit_(limit) {}
  std::shared_ptr<Lease> acquire(std::size_t bytes = 0) {
    std::lock_guard lock(mutex_);
    if (closed_ || active_ >= limit_ || bytes_ + bytes > kPendingBytes)
      return {};
    ++active_;
    bytes_ += bytes;
    return std::shared_ptr<Lease>(new Lease{shared_from_this(), bytes});
  }
  void close() {
    std::lock_guard lock(mutex_);
    closed_ = true;
    notify();
  }
  void wait(Context context, bool drained = false, std::size_t bytes = 0) {
    for (;;) {
      auto event = std::make_shared<Event>();
      {
        std::lock_guard lock(mutex_);
        if (drained ? active_ == 0
                    : (!closed_ && active_ < limit_ &&
                       bytes_ + bytes <= kPendingBytes))
          return;
        if ((!drained && closed_) || context.cancelled() ||
            userver::engine::current_task::ShouldCancel())
          throw AdmissionRejected("Pulsar admission wait cancelled or closed");
        std::erase_if(waiters_,
                      [](const auto& value) { return value.expired(); });
        // One input loop and one Stop waiter per gate. No publication queue.
        if (waiters_.size() >= 2)
          throw std::logic_error("Pulsar concurrent lifecycle wait");
        waiters_.push_back(event);
      }
      event->Wait(context);
    }
  }

 private:
  void notify() {
    for (auto& weak : waiters_)
      if (auto event = weak.lock()) event->Send();
    waiters_.clear();
  }
  void release(std::size_t bytes) {
    std::lock_guard lock(mutex_);
    --active_;
    bytes_ -= bytes;
    notify();
  }
  std::mutex mutex_;
  std::vector<std::weak_ptr<Event>> waiters_;
  std::size_t limit_, active_{}, bytes_{};
  bool closed_{};
};
inline config::PulsarEndpointConfig EndpointConfig(
    const IServiceEnvironment& env, int id) {
  auto runtime = env.getRuntimeConfigSnapshot();
  auto value = runtime ? runtime->GetEndpointConfigByID(id) : std::nullopt;
  auto* c = value ? value->As<config::PulsarEndpointConfig>() : nullptr;
  if (!c) throw std::invalid_argument("Pulsar endpoint config missing");
  return *c;
}
inline config::PulsarDataConnectorConfig ConnectorConfig(
    const IServiceEnvironment& env, int id) {
  auto ep = EndpointConfig(env, id);
  auto runtime = env.getRuntimeConfigSnapshot();
  auto value = runtime->GetDataConnectorByID(ep.idDataConnector);
  auto* c = value ? value->As<config::PulsarDataConnectorConfig>() : nullptr;
  if (!c) throw std::invalid_argument("Pulsar connector config missing");
  return *c;
}
}  // namespace servicelib::pulsar::detail

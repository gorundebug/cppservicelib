#pragma once

#include <functional>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <servicelib/runtime/context.hpp>

template <typename Interface>
class StopCallbackEndpoint final : public Interface {
 public:
  StopCallbackEndpoint(int id, std::function<void()> stop)
      : id_(id), stop_(std::move(stop)) {}
  int id() const noexcept override { return id_; }
  void start(servicelib::Context) override {}
  void stop(servicelib::Context) override { stop_(); }

 private:
  int id_;
  std::function<void()> stop_;
};

class EndpointLifecycleEvents final {
 public:
  void add(std::string event) {
    std::lock_guard lock(mutex_);
    events_.push_back(std::move(event));
  }
  std::vector<std::string> snapshot() const {
    std::lock_guard lock(mutex_);
    return events_;
  }

 private:
  mutable std::mutex mutex_;
  std::vector<std::string> events_;
};

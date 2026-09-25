#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>

namespace servicelib {

// Owned by the executable, outside DaemonMain. The native waiter deliberately
// does not depend on a userver task processor that shutdown may have blocked.
// It sleeps until armed, and then until the earliest service shutdown deadline.
class ProcessShutdownGuard final {
 public:
  using Deadline = std::optional<std::chrono::steady_clock::time_point>;

  ProcessShutdownGuard() {
    std::lock_guard lock(registryMutex_);
    if (instance_) {
      throw std::logic_error("process shutdown guard already installed");
    }
    worker_ = std::thread([this] { wait(); });
    instance_ = this;
  }

  ~ProcessShutdownGuard() {
    {
      std::lock_guard registryLock(registryMutex_);
      instance_ = nullptr;
      std::lock_guard lock(mutex_);
      finished_ = true;
    }
    changed_.notify_one();
    worker_.join();
  }

  ProcessShutdownGuard(const ProcessShutdownGuard&) = delete;
  ProcessShutdownGuard& operator=(const ProcessShutdownGuard&) = delete;

  // Return the effective deadline so every graceful phase uses the same budget.
  // Runtime-only users need not install a process-wide exit policy.
  static Deadline arm(Deadline deadline, int exitCode = 0) {
    std::lock_guard registryLock(registryMutex_);
    if (!instance_) return deadline;
    auto& guard = *instance_;
    std::lock_guard lock(guard.mutex_);
    if (deadline && (!guard.deadline_ || *deadline < *guard.deadline_)) {
      guard.deadline_ = deadline;
    }
    if (exitCode != 0) guard.exitCode_ = exitCode;
    guard.changed_.notify_one();
    return guard.deadline_;
  }

  // Check synchronously before releasing business functions as well: a native
  // waiter that has become runnable need not have been scheduled yet.
  static void exitIfExpired(Deadline deadline, int exitCode = 0) noexcept {
    if (deadline && std::chrono::steady_clock::now() >= *deadline) {
      std::_Exit(exitCode);
    }
  }

 private:
  void wait() noexcept {
    std::unique_lock lock(mutex_);
    while (!finished_) {
      if (!deadline_) {
        changed_.wait(lock, [this] { return finished_ || deadline_; });
        continue;
      }
      const auto deadline = *deadline_;
      if (changed_.wait_until(lock, deadline, [this, deadline] {
            return finished_ || *deadline_ != deadline;
          })) {
        continue;
      }
      std::_Exit(exitCode_);
    }
  }

  inline static std::mutex registryMutex_;
  inline static ProcessShutdownGuard* instance_{};
  std::mutex mutex_;
  std::condition_variable changed_;
  Deadline deadline_;
  bool finished_{};
  int exitCode_{};
  std::thread worker_;
};

}  // namespace servicelib

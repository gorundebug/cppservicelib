#include <servicelib/runtime/process_shutdown.hpp>

#include <chrono>
#include <cstdio>
#include <string_view>
#include <thread>

namespace {
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using Guard = servicelib::ProcessShutdownGuard;

struct BusinessFunction final {
  bool blockInDestructor{};
  ~BusinessFunction() {
    if (blockInDestructor) {
      std::puts("destructor entered");
      std::fflush(stdout);
      std::this_thread::sleep_for(30s);
    }
    std::puts("function released");
    std::fflush(stdout);
  }
};
}  // namespace

int main(int argc, char* argv[]) {
  if (argc != 2) return 64;
  const std::string_view scenario{argv[1]};
  Guard guard;
  BusinessFunction function;
  std::puts("probe entered");
  std::fflush(stdout);
  if (scenario == "unarmed") return 7;
  if (scenario == "clean") {
    Guard::arm(Clock::now() + 2s);
    return 7;
  }
  const int exitCode = scenario == "startup-failure" ? 1 : 0;
  auto deadline = Guard::arm(Clock::now() + 120ms, exitCode);
  if (scenario == "rearm-later") {
    deadline = Guard::arm(Clock::now() + 2s);
  } else if (scenario == "rearm-earlier") {
    deadline = Guard::arm(Clock::now());
  } else if (scenario == "startup-failure-after-stop") {
    deadline = Guard::arm(deadline, 1);
  } else if (scenario == "destructor") {
    function.blockInDestructor = true;
    return 7;
  } else if (scenario == "expired-before-release") {
    // The synchronous check also works without relying on waiter scheduling.
    Guard::exitIfExpired(Clock::now() - 1ms);
    return 65;
  }
  std::this_thread::sleep_for(30s);
  Guard::exitIfExpired(deadline, exitCode);
  return 66;
}

#define PROBE_USERVER
#include "probe.hpp"
int main(int argc, char** argv) {
  if (argc < 3) return 2;
  int result = 0;
  userver::engine::RunStandalone(1, [&] {
    auto timer = userver::utils::Async("ticker", [] { Tick(); });
    try {
      Run(argv[1], argv[2], argc > 3 ? argv[3] : "", argc > 4,
          argc > 4 && std::string(argv[4]) == "fault",
          argc > 4 && std::string(argv[4]) == "stop");
    } catch (const std::exception& e) {
      std::cerr << e.what() << std::endl;
      result = 1;
    }
    done = true;
    timer.Get();
  });
  return result;
}

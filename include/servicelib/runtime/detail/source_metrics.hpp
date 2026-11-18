#pragma once
#include <atomic>
#include <servicelib/runtime/environment/metrics/metrics.hpp>
#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace servicelib::detail {
// Only observed native/source events. Never inferred from graph Done/return.
class SourceMetrics final {
 public:
  using Slot = std::atomic<std::shared_ptr<SourceMetrics>>;
  SourceMetrics(metrics::Metrics& backend, const std::string& protocol,
                const std::string& connector, const std::string& endpoint) noexcept {
    try {
      if (!backend.enabled()) return;
      auto scope = backend.scope("broker_endpoint", {{"protocol", protocol},
          {"connector", connector}, {"endpoint", endpoint}});
      auto bind = [&](std::initializer_list<const char*> names) {
        for (auto name : names) counters_.emplace_back(name, scope->counter(
            "source_observations_total", "Observed native broker source events", {{"observation", name}}));
      };
      bind({"received"});
      if (protocol == "jetstream" || protocol == "pulsar") bind({"redelivered"});
      if (protocol == "redisstreams") bind({"reclaimed", "pending-deleted"});
    } catch (...) { counters_.clear(); }
  }
  void observe(std::string_view event, std::int64_t count = 1) const noexcept {
    try {
      for (const auto& [name, counter] : counters_)
        if (name == event) { counter->add(count); return; }
    } catch (...) {} // Telemetry cannot change native completion or routing.
  }
  static void Observe(const Slot& slot, std::string_view event, std::int64_t count = 1) noexcept {
    if (auto value = slot.load()) value->observe(event, count);
  }
 private:
  std::vector<std::pair<std::string, std::unique_ptr<metrics::Int64Counter>>> counters_;
};
} // namespace servicelib::detail

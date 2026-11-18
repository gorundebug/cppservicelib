#pragma once
#include <servicelib/runtime/environment/metrics/metrics.hpp>
#include <initializer_list>
#include <utility>

namespace servicelib::detail {
// Instruments are endpoint-owned; asynchronous deliveries retain shared ownership.
class PublicationMetrics final {
 public:
  PublicationMetrics(metrics::Metrics& backend, std::string protocol,
                     std::string connector, std::string endpoint) {
    if (!backend.enabled()) return;
    auto scope = backend.scope("broker_endpoint", {{"protocol", protocol},
        {"connector", connector}, {"endpoint", endpoint}});
    auto bind = [&](std::initializer_list<const char*> names) {
      for (const auto* name : names)
        counters_.emplace_back(name, scope->counter("publication_outcomes_total",
            "Observed broker publication outcomes", {{"outcome", name}}));
    };
    if (protocol == "nats") bind({"client-accepted", "flushed", "error"});
    else if (protocol == "jetstream") bind({"acknowledged", "error"});
    else bind({"not-sent", "accepted", "rejected", "unknown"});
  }
  void observe(std::string_view outcome) const noexcept {
    // Telemetry must not change a delivery result or suppress its callback.
    try {
      for (const auto& [name, counter] : counters_)
        if (name == outcome) { counter->inc(); return; }
    } catch (...) {}
  }
  template <class Outcome> void observeOutcome(Outcome outcome) const noexcept {
    switch (outcome) {
      case Outcome::NotSent: observe(std::string_view{"not-sent"}); break;
      case Outcome::Accepted: observe(std::string_view{"accepted"}); break;
      case Outcome::Rejected: observe(std::string_view{"rejected"}); break;
      case Outcome::Unknown: observe(std::string_view{"unknown"}); break;
    }
  }
 private:
  std::vector<std::pair<std::string, std::unique_ptr<metrics::Int64Counter>>> counters_;
};
}

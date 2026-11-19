#pragma once
#include <chrono>
#include <exception>
#include <memory>
#include <stdexcept>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/common.hpp>

namespace servicelib::dataquery {
// Independent Query telemetry; configured identities only, no payload labels.
class EndpointMetrics final {
 public:
  using Clock = std::chrono::steady_clock;
  EndpointMetrics(IServiceEnvironment& environment, int endpointId) {
    auto& metrics = environment.getMetrics();
    if (!metrics.enabled()) return;
    const auto config = environment.getRuntimeConfigSnapshot();
    const auto endpoint = config->GetEndpointConfigByID(endpointId);
    const auto* query = endpoint ? endpoint->As<config::QueryEndpointConfig>() : nullptr;
    if (!query) throw std::invalid_argument("Query metrics require a Query endpoint");
    const auto connector = config->GetDataConnectorByID(query->idDataConnector);
    if (!connector) throw std::invalid_argument("Query metrics require a connector");
    scope_ = metrics.scope("dataquery_endpoint", {{"connector", connector->GetName()},
        {"endpoint", query->name}, {"operation", query->queryOperation}});
    requests_ = scope_->counter("requests_total", "Admitted Query endpoint requests");
    results_ = scope_->counter("results_total", "Query result collector emissions, including application completion");
    errors_ = scope_->counter("failures_total", "Failed Query endpoint requests", {{"outcome", "error"}});
    cancellations_ = scope_->counter("failures_total", "Failed Query endpoint requests", {{"outcome", "cancelled"}});
    active_ = scope_->gauge("active_requests", "Active Query endpoint requests through EndRequest");
    duration_ = scope_->histogram("request_duration_seconds", "Query request duration through EndRequest in seconds");
  }
  [[nodiscard]] bool enabled() const noexcept { return bool(scope_); }
  void result() noexcept { if (scope_) observe([&] { results_->inc(); }); }
  class Request final {
   public:
    explicit Request(EndpointMetrics& metrics) : metrics_(metrics) {
      if (!metrics_.enabled()) return;
      started_ = Clock::now();
      observe([&] { metrics_.requests_->inc(); });
      observe([&] { metrics_.active_->inc(); });
    }
    Request(const Request&) = delete;
    ~Request() {
      if (!metrics_.enabled()) return;
      observe([&] { metrics_.active_->dec(); });
      observe([&] { metrics_.duration_->observe(std::chrono::duration<double>(Clock::now() - started_).count()); });
    }
    void outcome(const std::exception_ptr& error, bool cancelled) noexcept {
      if (!error || !metrics_.enabled()) return;
      try { std::rethrow_exception(error); }
      catch (const OperationCancelledError&) { cancelled = true; }
      catch (...) {}
      observe([&] { (cancelled ? metrics_.cancellations_ : metrics_.errors_)->inc(); });
    }
   private:
    EndpointMetrics& metrics_;
    Clock::time_point started_{};
  };
 private:
  template<class F> static void observe(F&& action) noexcept { try { action(); } catch (...) {} }
  std::unique_ptr<metrics::MetricsScope> scope_;
  std::unique_ptr<metrics::Int64Counter> requests_, results_, errors_, cancellations_;
  std::unique_ptr<metrics::Int64Gauge> active_;
  std::unique_ptr<metrics::Float64Histogram> duration_;
};
}

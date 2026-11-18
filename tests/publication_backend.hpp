#pragma once
#include <servicelib/runtime/detail/publication_metrics.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>
#include <atomic>
#include <stdexcept>

// Exercise the real metric wrapper with a backend whose recording can fail.
class PublicationBackend final : public servicelib::metrics::Metrics {
 public:
  servicelib::testmetrics::TestMetrics recorded;
  bool disabled{}, throwing{}, failBinding{};
  std::atomic<int> attempts{0};
  bool enabled() const noexcept override { return !disabled; }
  class Counter final : public servicelib::metrics::Int64Counter {
   public:
    Counter(PublicationBackend& b, std::unique_ptr<servicelib::metrics::Int64Counter> c)
        : backend(b), counter(std::move(c)) {}
    void inc() override {
      ++backend.attempts;
      if (backend.throwing) throw std::runtime_error("test recording failure");
      counter->inc();
    }
    void add(std::int64_t n) override { while (n-- > 0) inc(); }
    PublicationBackend& backend;
    std::unique_ptr<servicelib::metrics::Int64Counter> counter;
  };
  class Scope final : public servicelib::metrics::MetricsScope {
   public:
    Scope(PublicationBackend& b, std::unique_ptr<servicelib::metrics::MetricsScope> s)
        : backend(b), scope(std::move(s)) {}
    std::unique_ptr<servicelib::metrics::Int64Counter> counter(std::string_view n, std::string_view h, const servicelib::metrics::Labels& l) override {
      return std::make_unique<Counter>(backend, scope->counter(n,h,l));
    }
    std::unique_ptr<servicelib::metrics::Int64Gauge> gauge(std::string_view n, std::string_view h, const servicelib::metrics::Labels& l) override { return scope->gauge(n,h,l); }
    std::unique_ptr<servicelib::metrics::Float64Histogram> histogram(std::string_view n, std::string_view h, const servicelib::metrics::Labels& l, std::vector<double> v) override { return scope->histogram(n,h,l,std::move(v)); }
    std::unique_ptr<servicelib::metrics::ObservableFloat64Gauge> observableFloat64Gauge(std::string_view n, std::string_view h, std::function<double()> f, const servicelib::metrics::Labels& l) override { return scope->observableFloat64Gauge(n,h,std::move(f),l); }
    PublicationBackend& backend;
    std::unique_ptr<servicelib::metrics::MetricsScope> scope;
  };
  std::unique_ptr<servicelib::metrics::MetricsScope> scope(std::string_view n, const servicelib::metrics::Labels& l) override {
    if (failBinding) throw std::runtime_error("test binding failure");
    if (disabled) throw std::logic_error("disabled backend must not bind instruments");
    return std::make_unique<Scope>(*this, recorded.scope(n,l));
  }
  auto count(const std::string& protocol, const std::string& outcome) {
    return recorded.counter("broker_endpoint.publication_outcomes_total", {{"protocol",protocol},{"connector","boundary"},{"endpoint","output"},{"outcome",outcome}}).count();
  }
};

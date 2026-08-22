/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the LICENSE file for details.
 */
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <userver/utils/statistics/entry.hpp>
#include <userver/utils/statistics/histogram.hpp>
#include <userver/utils/statistics/labels.hpp>
#include <userver/utils/statistics/prometheus.hpp>
#include <userver/utils/statistics/rate_counter.hpp>
#include <userver/utils/statistics/storage.hpp>
#include <userver/utils/statistics/writer.hpp>
#include <userver/components/component_context.hpp>
#include <userver/components/statistics_storage.hpp>
#include <userver/server/handlers/http_handler_base.hpp>
#include <userver/yaml_config/merge_schemas.hpp>

#include <servicelib/runtime/environment/metrics/metrics.hpp>

namespace servicelib::telemetry::userver_adapter {

namespace utils = ::userver::utils;

namespace detail {

inline std::vector<utils::statistics::Label> makeLabels(
    const metrics::Labels& base, const metrics::Labels& extra) {
  std::vector<utils::statistics::Label> result;
  result.reserve(base.size() + extra.size());
  for (const auto& [name, value] : base) {
    result.emplace_back(name, value);
  }
  for (const auto& [name, value] : extra) {
    result.emplace_back(name, value);
  }
  return result;
}

class MetricNode {
 public:
  MetricNode(std::string path,
             std::vector<utils::statistics::Label> labels)
      : path_(std::move(path)), labels_(std::move(labels)) {}
  virtual ~MetricNode() = default;

  virtual void write(utils::statistics::Writer& root) const = 0;

 protected:
  template <typename Value>
  void writeValue(utils::statistics::Writer& root, const Value& value) const {
    std::vector<utils::statistics::LabelView> labels;
    labels.reserve(labels_.size());
    for (const auto& label : labels_) {
      labels.emplace_back(label);
    }
    auto writer = root[path_];
    writer.ValueWithLabels(value, utils::statistics::LabelsSpan{labels});
  }

 private:
  std::string path_;
  std::vector<utils::statistics::Label> labels_;
};

class CounterNode final : public MetricNode {
 public:
  using MetricNode::MetricNode;

  void inc() { value_.Increment(); }
  void add(std::int64_t value) {
    value_.Add(utils::statistics::Rate{static_cast<std::uint64_t>(value)});
  }
  void write(utils::statistics::Writer& root) const override {
    writeValue(root, value_);
  }

 private:
  utils::statistics::RateCounter value_;
};

class GaugeNode final : public MetricNode {
 public:
  using MetricNode::MetricNode;

  void set(std::int64_t value) {
    value_.store(value, std::memory_order_relaxed);
  }
  void add(std::int64_t value) {
    value_.fetch_add(value, std::memory_order_relaxed);
  }
  void write(utils::statistics::Writer& root) const override {
    writeValue(root, value_.load(std::memory_order_relaxed));
  }

 private:
  std::atomic<std::int64_t> value_{0};
};

class HistogramNode final : public MetricNode {
 public:
  HistogramNode(std::string path,
                std::vector<utils::statistics::Label> labels,
                std::vector<double> buckets)
      : MetricNode(std::move(path), std::move(labels)),
        value_(buckets.empty() ? defaultBuckets() : buckets) {}

  void observe(double value) { value_.Account(value); }
  void write(utils::statistics::Writer& root) const override {
    writeValue(root, value_);
  }

 private:
  static std::vector<double> defaultBuckets() {
    return {0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1,
            0.25,  0.5,   1,     2.5,  5,     10};
  }

  utils::statistics::Histogram value_;
};

class ObservableGaugeNode final : public MetricNode {
 public:
  ObservableGaugeNode(std::string path,
                      std::vector<utils::statistics::Label> labels,
                      std::function<double()> value)
      : MetricNode(std::move(path), std::move(labels)),
        value_(std::move(value)) {}

  double value() const noexcept {
    try {
      return value_();
    } catch (...) {
      return 0.0;
    }
  }
  void write(utils::statistics::Writer& root) const override {
    writeValue(root, value());
  }

 private:
  std::function<double()> value_;
};

class Registry final {
 public:
  template <typename Node, typename... Args>
  std::shared_ptr<Node> add(Args&&... args) {
    auto node = std::make_shared<Node>(std::forward<Args>(args)...);
    std::lock_guard lock(mutex_);
    nodes_.emplace_back(node);
    return node;
  }

  void write(utils::statistics::Writer& writer) {
    std::vector<std::shared_ptr<MetricNode>> alive;
    {
      std::lock_guard lock(mutex_);
      auto output = nodes_.begin();
      for (auto input = nodes_.begin(); input != nodes_.end(); ++input) {
        if (auto node = input->lock()) {
          alive.push_back(std::move(node));
          *output++ = *input;
        }
      }
      nodes_.erase(output, nodes_.end());
    }
    for (const auto& node : alive) {
      node->write(writer);
    }
  }

 private:
  std::mutex mutex_;
  std::vector<std::weak_ptr<MetricNode>> nodes_;
};

}  // namespace detail

// Publishes userver's statistics storage on the service's regular HTTP
// listener. Unlike userver's handler-server-monitor, this component does not
// require a separate monitor listener, matching Go and Python servicelib
// semantics where Service.metricsHandler is a route on the service HTTP port.
class MetricsHandler final
    : public ::userver::server::handlers::HttpHandlerBase {
 public:
  static constexpr std::string_view kName = "servicelib-metrics-handler";

  MetricsHandler(
      const ::userver::components::ComponentConfig& config,
      const ::userver::components::ComponentContext& context)
      : HttpHandlerBase(config, context),
        storage_(context
                     .FindComponent<
                         ::userver::components::StatisticsStorage>()
                     .GetStorage()) {}

  static ::userver::yaml_config::Schema GetStaticConfigSchema() {
    return ::userver::yaml_config::MergeSchemas<HttpHandlerBase>(R"(
type: object
description: Prometheus metrics endpoint on the main service listener.
additionalProperties: false
properties: {}
)");
  }

 protected:
  std::string HandleRequestThrow(
      const ::userver::server::http::HttpRequest& request,
      ::userver::server::request::RequestContext&) const override {
    request.GetHttpResponse().SetContentType(
        "text/plain; version=0.0.4; charset=utf-8");
    return ::userver::utils::statistics::ToPrometheusFormat(storage_);
  }

 private:
  const ::userver::utils::statistics::Storage& storage_;
};

class UserverInt64Counter final : public metrics::Int64Counter {
 public:
  explicit UserverInt64Counter(std::shared_ptr<detail::CounterNode> node)
      : node_(std::move(node)) {}
  void inc() override { node_->inc(); }
  void add(std::int64_t value) override { node_->add(value); }

 private:
  std::shared_ptr<detail::CounterNode> node_;
};

class UserverInt64Gauge final : public metrics::Int64Gauge {
 public:
  explicit UserverInt64Gauge(std::shared_ptr<detail::GaugeNode> node)
      : node_(std::move(node)) {}
  void set(std::int64_t value) override { node_->set(value); }
  void inc() override { node_->add(1); }
  void dec() override { node_->add(-1); }
  void add(std::int64_t value) override { node_->add(value); }
  void sub(std::int64_t value) override { node_->add(-value); }

 private:
  std::shared_ptr<detail::GaugeNode> node_;
};

class UserverFloat64Histogram final : public metrics::Float64Histogram {
 public:
  explicit UserverFloat64Histogram(
      std::shared_ptr<detail::HistogramNode> node)
      : node_(std::move(node)) {}
  void observe(double value) override { node_->observe(value); }

 private:
  std::shared_ptr<detail::HistogramNode> node_;
};

class UserverObservableFloat64Gauge final
    : public metrics::ObservableFloat64Gauge {
 public:
  explicit UserverObservableFloat64Gauge(
      std::shared_ptr<detail::ObservableGaugeNode> node)
      : node_(std::move(node)) {}
  double value() const override { return node_->value(); }

 private:
  std::shared_ptr<detail::ObservableGaugeNode> node_;
};

class UserverMetricsScope final : public metrics::MetricsScope {
 public:
  UserverMetricsScope(std::shared_ptr<detail::Registry> registry,
                      std::string prefix, metrics::Labels base)
      : registry_(std::move(registry)),
        prefix_(std::move(prefix)),
        base_(std::move(base)) {}

  std::unique_ptr<metrics::Int64Counter> counter(
      std::string_view name, std::string_view,
      const metrics::Labels& labels) override {
    return std::make_unique<UserverInt64Counter>(
        registry_->add<detail::CounterNode>(
            fullName(name), detail::makeLabels(base_, labels)));
  }

  std::unique_ptr<metrics::Int64Gauge> gauge(
      std::string_view name, std::string_view,
      const metrics::Labels& labels) override {
    return std::make_unique<UserverInt64Gauge>(
        registry_->add<detail::GaugeNode>(
            fullName(name), detail::makeLabels(base_, labels)));
  }

  std::unique_ptr<metrics::Float64Histogram> histogram(
      std::string_view name, std::string_view,
      const metrics::Labels& labels, std::vector<double> buckets) override {
    return std::make_unique<UserverFloat64Histogram>(
        registry_->add<detail::HistogramNode>(
            fullName(name), detail::makeLabels(base_, labels),
            std::move(buckets)));
  }

  std::unique_ptr<metrics::ObservableFloat64Gauge> observableFloat64Gauge(
      std::string_view name, std::string_view,
      std::function<double()> value,
      const metrics::Labels& labels) override {
    return std::make_unique<UserverObservableFloat64Gauge>(
        registry_->add<detail::ObservableGaugeNode>(
            fullName(name), detail::makeLabels(base_, labels),
            std::move(value)));
  }

 private:
  std::string fullName(std::string_view name) const {
    if (name.empty()) {
      return prefix_;
    }
    if (prefix_.empty()) {
      return std::string{name};
    }
    return prefix_ + "." + std::string{name};
  }

  std::shared_ptr<detail::Registry> registry_;
  std::string prefix_;
  metrics::Labels base_;
};

class UserverMetrics final : public metrics::Metrics {
 public:
  explicit UserverMetrics(utils::statistics::Storage& storage)
      : registry_(std::make_shared<detail::Registry>()),
        entry_(storage.RegisterWriter(
            "",
            [registry = std::weak_ptr<detail::Registry>{registry_}](
                utils::statistics::Writer& writer) {
              if (const auto locked = registry.lock()) {
                locked->write(writer);
              }
            })) {}

  std::unique_ptr<metrics::MetricsScope> scope(
      std::string_view prefix, const metrics::Labels& labels) override {
    return std::make_unique<UserverMetricsScope>(
        registry_, std::string{prefix}, labels);
  }

 private:
  std::shared_ptr<detail::Registry> registry_;
  // Must be destroyed before registry_ because its callback references it.
  utils::statistics::Entry entry_;
};

}  // namespace servicelib::telemetry::userver_adapter

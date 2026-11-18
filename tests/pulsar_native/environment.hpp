#pragma once
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>
class TestConfig final : public servicelib::config::IConfig {
 public:
  TestConfig() {
    pulsarEndpoint.id = 3;
    pulsarEndpoint.name = "input";
    pulsarEndpoint.idDataConnector = 4;
    pulsarEndpoint.topic = std::getenv("PULSAR_TEST_TOPIC");
    pulsarEndpoint.subscriptionName = "workers";
    pulsarEndpoint.createTopic = true;
    pulsarEndpoint.createSubscription = true;
    pulsarEndpoint.subscriptionInitialPosition = "Earliest";
    pulsarEndpoint.negativeAckRedeliveryDelayMs = 100;
    pulsarConnector.id = 4;
    pulsarConnector.name = "pulsar";
    pulsarConnector.servers = {std::getenv("PULSAR_TEST_URL")};
    pulsarConnector.adminUrl = std::getenv("PULSAR_TEST_ADMIN");
    pulsarConnector.operationTimeoutMs = 2000;
    sinkEndpoint = pulsarEndpoint;
    sinkEndpoint.id = 5;
    sinkEndpoint.name = "output";
    sinkEndpoint.subscriptionName = "";
    sinkEndpoint.createSubscription = false;
    sinkEndpoint.subscriptionInitialPosition = "";
  }

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {pulsarConnector};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {pulsarEndpoint, sinkEndpoint};
  }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override {
    return {};
  }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override {
    return {};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override {
    return {};
  }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override {
    return {};
  }

  servicelib::config::PulsarEndpointConfig pulsarEndpoint, sinkEndpoint;
  servicelib::config::PulsarDataConnectorConfig pulsarConnector;
};

class TestEnvironment final : public servicelib::IRuntimeEnvironment {
 public:
  TestEnvironment() : runtimeConfig_(config_) {
    service_.name = "endpoint-test";
  }
  servicelib::pool::ITaskPool* getTaskPool(const std::string&) override {
    return nullptr;
  }
  servicelib::pool::IPriorityTaskPool* getPriorityTaskPool(
      const std::string&) override {
    return nullptr;
  }
  std::shared_ptr<const servicelib::config::RuntimeConfig>
  getRuntimeConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::RuntimeConfig>(
        runtimeConfig_);
  }
  std::shared_ptr<const servicelib::config::ServiceConfig>
  getServiceConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::ServiceConfig>(service_);
  }
  servicelib::log::Logger& getLogger() override { return log_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override { return tracingEngine; }
  servicelib::tracing::Tracing* tracingEngine{};
  TestConfig& config() noexcept { return config_; }

 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtimeConfig_;
  servicelib::config::ServiceConfig service_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};

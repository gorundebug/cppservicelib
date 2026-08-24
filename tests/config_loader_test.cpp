/*
 * config_loader_test.cpp
 * Tests for servicelib::config::ConfigLoader: base+override merge, $var
 * substitution via yaml_config::YamlConfig, and mtime-poll hot-reload.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <userver/engine/sleep.hpp>
#include <userver/formats/yaml/serialize.hpp>
#include <userver/fs/blocking/temp_directory.hpp>
#include <userver/fs/blocking/write.hpp>
#include <userver/utest/utest.hpp>

#include <servicelib/runtime/config/component.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

namespace {

using namespace std::chrono_literals;

// Minimal IConfig with a single pool — enough to exercise ConfigLoader's
// own mechanics (base+override merge, $var, hot-reload) without dragging
// in a full topology. Go analog of the test fixture: none needed there,
// since Go's config.NewRuntimeConfig has no equivalent "must parse from
// YAML" step to isolate.
class TestConfig final : public servicelib::config::IConfig {
 public:
  TestConfig() = default;

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {&service};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {};
  }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override {
    return {&pool};
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

  servicelib::config::ServiceConfig service;
  servicelib::config::PoolConfig pool;
};

class TemporalTopologyConfig final : public servicelib::config::IConfig {
 public:
  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {cronConnector, temporalConnector};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {cronEndpoint, temporalEndpoint};
  }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override {
    return {};
  }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override {
    return {&durableLink};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override {
    return {};
  }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override {
    return {};
  }

  servicelib::config::CronDataConnectorConfig cronConnector;
  servicelib::config::TemporalDataConnectorConfig temporalConnector;
  servicelib::config::CronEndpointConfig cronEndpoint;
  servicelib::config::TemporalEndpointConfig temporalEndpoint;
  servicelib::config::LinkConfig durableLink;
};

TestConfig MakeConfig(userver::formats::parse::To<TestConfig>) {
  TestConfig result;
  result.service.id = 1;
  result.service.name = "real-service";
  result.service.environment = servicelib::api::Environment::kProduction;
  result.pool.name = "generated-pool";
  result.pool.executorsCount = 2;
  result.pool.queueCapacity = 64;
  return result;
}

void ApplyConfig(const userver::formats::yaml::Value& value,
                 TestConfig& result) {
  const auto pool = value["pool"];
  if (pool.IsMissing()) {
    return;
  }
  if (const auto field = pool["name"]; !field.IsMissing()) {
    result.pool.name = field.As<std::string>();
  }
  if (const auto field = pool["executorsCount"]; !field.IsMissing()) {
    result.pool.executorsCount = field.As<int>();
  }
  if (const auto field = pool["queueCapacity"]; !field.IsMissing()) {
    result.pool.queueCapacity = field.As<int>();
  }
}

void ApplyEnvironment(TestConfig&) {}

class ConfigLoaderFixture {
 public:
  ConfigLoaderFixture()
      : dir_(userver::fs::blocking::TempDirectory::Create()) {}

  std::string WriteFile(const std::string& name, std::string_view contents) {
    const auto path = dir_.GetPath() + "/" + name;
    userver::fs::blocking::RewriteFileContents(path, contents);
    return path;
  }

  void RewriteFile(const std::string& path, std::string_view contents) {
    userver::fs::blocking::RewriteFileContents(path, contents);
  }

  servicelib::config::ConfigLoader<TestConfig> MakeLoader(
      servicelib::config::ConfigLoader<TestConfig>::Paths paths,
      userver::formats::yaml::Value configVars = {}) {
    return servicelib::config::ConfigLoader<TestConfig>(
        std::move(paths), std::move(configVars), log_, metrics_,
        "config-loader-test");
  }

  servicelib::testmetrics::TestMetrics& Metrics() { return metrics_; }

 private:
  userver::fs::blocking::TempDirectory dir_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};

}  // namespace

// Keep the userver component wrapper compiled as part of the config tests;
// production code instantiates it with the generated service Config type.
template class servicelib::config::ServicelibRuntimeComponent<TestConfig>;

UTEST(ConfigLoader, LoadReadsBaseConfig) {
  ConfigLoaderFixture fixture;
  const auto basePath = fixture.WriteFile("base.yaml", R"(
pool:
  name: test-pool
  executorsCount: 4
)");

  auto loader = fixture.MakeLoader(
      {.configPath = basePath, .overridePath = std::nullopt});
  const auto runtimeConfig = loader.Load();

  const auto* pool = runtimeConfig->GetPoolByName("test-pool");
  ASSERT_NE(pool, nullptr);
  EXPECT_EQ(pool->executorsCount, 4);
}

UTEST(ConfigLoader, InitialMetricsUseTopologyServiceIdentity) {
  ConfigLoaderFixture fixture;
  auto loader = fixture.MakeLoader({});

  loader.Load();

  EXPECT_EQ(fixture.Metrics()
                .gauge("service.info", {{"service", "real-service"},
                                        {"environment", "production"}})
                .value(),
            1);
  const auto names = fixture.Metrics().registeredNames();
  EXPECT_NE(std::find(names.begin(), names.end(), "service.info"), names.end());
  EXPECT_NE(
      std::find(names.begin(), names.end(), "service.config_reloads_total"),
      names.end());
}

UTEST(ConfigLoader, LoadWithoutBaseUsesGeneratedConfig) {
  ConfigLoaderFixture fixture;
  auto loader = fixture.MakeLoader({});

  const auto runtimeConfig = loader.Load();

  const auto* pool = runtimeConfig->GetPoolByName("generated-pool");
  ASSERT_NE(pool, nullptr);
  EXPECT_EQ(pool->executorsCount, 2);
  EXPECT_EQ(pool->queueCapacity, 64);
}

UTEST(ConfigLoader, LoadAppliesYamlOnTopOfGeneratedDefaults) {
  ConfigLoaderFixture fixture;
  const auto basePath = fixture.WriteFile("base.yaml", R"(
pool:
  executorsCount: 7
)");

  auto loader = fixture.MakeLoader(
      {.configPath = basePath, .overridePath = std::nullopt});
  const auto runtimeConfig = loader.Load();

  const auto* pool = runtimeConfig->GetPoolByName("generated-pool");
  ASSERT_NE(pool, nullptr);
  EXPECT_EQ(pool->executorsCount, 7);
  EXPECT_EQ(pool->queueCapacity, 64);
}

UTEST(ConfigLoader, ParsesGoCompatibleEnumWireValuesAndScalarSemantics) {
  const auto yaml = userver::formats::yaml::FromString(R"(
service:
  environment: production
  defaultCallSemantics: FunctionCall
connector:
  implementation: userver/http
type:
  type: unicode string
link:
  from: 1
  to: 2
  callSemantics: PriorityTaskPool
  poolName: priority
  priority: 17
functionCallLink:
  from: 2
  to: 3
  callSemantics: FunctionCall
  async: true
)");

  const auto service = yaml["service"].As<servicelib::config::ServiceConfig>();
  EXPECT_EQ(service.environment, servicelib::api::Environment::kProduction);
  ASSERT_TRUE(service.defaultCallSemantics.has_value());
  EXPECT_TRUE(service.defaultCallSemantics->functionCall.has_value());

  const auto connector =
      yaml["connector"].As<servicelib::config::HttpDataConnectorConfig>();
  EXPECT_EQ(connector.implementation,
            servicelib::api::DataConnectorImplementation::kUserverHTTP);

  const auto type = yaml["type"].As<servicelib::config::TypeConfig>();
  EXPECT_EQ(type.type, servicelib::api::DataType::kUnicodeString);

  const auto link = yaml["link"].As<servicelib::config::LinkConfig>();
  ASSERT_TRUE(link.callSemantics.has_value());
  ASSERT_TRUE(link.callSemantics->priorityTaskPool.has_value());
  EXPECT_EQ(link.callSemantics->priorityTaskPool->poolName, "priority");
  EXPECT_EQ(link.callSemantics->priorityTaskPool->priority, 17);

  const auto functionCallLink =
      yaml["functionCallLink"].As<servicelib::config::LinkConfig>();
  ASSERT_TRUE(functionCallLink.callSemantics.has_value());
  ASSERT_TRUE(functionCallLink.callSemantics->functionCall.has_value());
  EXPECT_TRUE(functionCallLink.callSemantics->functionCall->async);
}

UTEST(ConfigLoader, ParsesCronTemporalAndDurableCallContracts) {
  const auto yaml = userver::formats::yaml::FromString(R"(
cronConnector:
  id: 31
  name: local-scheduler
  implementation: node/croner
temporalConnector:
  id: 32
  name: temporal
  implementation: temporal/go
  address: temporal:7233
  namespace: servicegen
  identity: automation-service
  maxConcurrentActivities: 12
  maxConcurrentWorkflows: 7
cronEndpoint:
  id: 41
  name: hourly-trigger
  idDataConnector: 31
  enabled: true
  schedule: "0 * * * *"
  timezone: UTC
  overlapPolicy: Skip
  missedRunPolicy: FireOnce
temporalEndpoint:
  id: 42
  name: durable-trigger
  idDataConnector: 32
  enabled: true
  taskQueue: automation
  schedule: "15 * * * *"
  scheduleId: hourly-automation
  timezone: UTC
  overlapPolicy: Allow
  missedRunPolicy: Skip
  workflowExecutionTimeout: 60000
  activityStartToCloseTimeout: 10000
  activityHeartbeatTimeout: 1000
  maximumAttempts: 5
link:
  from: 51
  to: 52
  callSemantics: DurableCall
  idDataConnector: 32
)");

  const auto cronConnector =
      yaml["cronConnector"].As<servicelib::config::CronDataConnectorConfig>();
  EXPECT_EQ(cronConnector.GetType(), servicelib::api::DataConnectorType::kCron);
  EXPECT_EQ(cronConnector.id, 31);
  const auto temporalConnector =
      yaml["temporalConnector"]
          .As<servicelib::config::TemporalDataConnectorConfig>();
  EXPECT_EQ(temporalConnector.namespaceName, "servicegen");
  EXPECT_EQ(temporalConnector.maxConcurrentActivities, 12);

  const auto cronEndpoint =
      yaml["cronEndpoint"].As<servicelib::config::CronEndpointConfig>();
  EXPECT_TRUE(cronEndpoint.enabled);
  EXPECT_EQ(cronEndpoint.schedule, "0 * * * *");
  EXPECT_EQ(cronEndpoint.missedRunPolicy,
            servicelib::api::ScheduleMissedRunPolicy::kFireOnce);
  const auto temporalEndpoint =
      yaml["temporalEndpoint"].As<servicelib::config::TemporalEndpointConfig>();
  EXPECT_EQ(temporalEndpoint.taskQueue, "automation");
  EXPECT_EQ(temporalEndpoint.maximumAttempts, 5);

  const auto link = yaml["link"].As<servicelib::config::LinkConfig>();
  ASSERT_TRUE(link.callSemantics.has_value());
  ASSERT_TRUE(link.callSemantics->durableCall.has_value());
  EXPECT_EQ(link.callSemantics->durableCall->idDataConnector, 32);
}

UTEST(ConfigLoader, RejectsNonUtcScheduledEndpointTimezone) {
  const auto yaml = userver::formats::yaml::FromString(R"(
schedule: "0 * * * *"
timezone: Europe/Moscow
)");
  EXPECT_THROW(yaml.As<servicelib::config::CronEndpointConfig>(),
               std::invalid_argument);
  EXPECT_THROW(yaml.As<servicelib::config::TemporalEndpointConfig>(),
               std::invalid_argument);
}

UTEST(ConfigLoader, ValidatesEndpointAndDurableConnectorTypes) {
  TemporalTopologyConfig config;
  config.cronConnector.id = 31;
  config.cronConnector.name = "local-scheduler";
  config.temporalConnector.id = 32;
  config.temporalConnector.name = "temporal";
  config.cronEndpoint.id = 41;
  config.cronEndpoint.name = "hourly-trigger";
  config.cronEndpoint.idDataConnector = 31;
  config.temporalEndpoint.id = 42;
  config.temporalEndpoint.name = "durable-trigger";
  config.temporalEndpoint.idDataConnector = 32;
  config.durableLink.from = 51;
  config.durableLink.to = 52;
  config.durableLink.callSemantics = servicelib::config::MakeCallSemanticsGroup(
      servicelib::api::CallSemantics::kDurableCall, {}, 0, false, 32);

  EXPECT_NO_THROW(servicelib::config::RuntimeConfig{config});

  config.durableLink.callSemantics->durableCall->idDataConnector = 31;
  EXPECT_THROW(servicelib::config::RuntimeConfig{config}, std::runtime_error);
  config.durableLink.callSemantics->durableCall->idDataConnector = 32;
  config.cronEndpoint.idDataConnector = 32;
  EXPECT_THROW(servicelib::config::RuntimeConfig{config}, std::runtime_error);
}

UTEST(ConfigLoader, LoadThrowsOnMissingBaseFile) {
  ConfigLoaderFixture fixture;
  auto loader = fixture.MakeLoader(
      {.configPath = "/nonexistent/path.yaml", .overridePath = std::nullopt});
  EXPECT_THROW(loader.Load(), std::exception);
}

UTEST(ConfigLoader, LoadMergesOverrideOnTopOfBase) {
  ConfigLoaderFixture fixture;
  const auto basePath = fixture.WriteFile("base.yaml", R"(
pool:
  name: test-pool
  executorsCount: 4
  queueCapacity: 100
)");
  const auto overridePath = fixture.WriteFile("override.yaml", R"(
pool:
  executorsCount: 8
)");

  auto loader = fixture.MakeLoader(
      {.configPath = basePath, .overridePath = overridePath});
  const auto runtimeConfig = loader.Load();

  const auto* pool = runtimeConfig->GetPoolByName("test-pool");
  ASSERT_NE(pool, nullptr);
  // Overridden field wins...
  EXPECT_EQ(pool->executorsCount, 8);
  // ...sibling fields not mentioned in the override survive the merge.
  EXPECT_EQ(pool->queueCapacity, 100);
}

UTEST(ConfigLoader, LoadSubstitutesConfigVars) {
  ConfigLoaderFixture fixture;
  const auto basePath = fixture.WriteFile("base.yaml", R"(
pool:
  name: test-pool
  executorsCount: $pool-executors
)");
  auto configVars = userver::formats::yaml::FromString("pool-executors: 6\n");

  auto loader = fixture.MakeLoader(
      {.configPath = basePath, .overridePath = std::nullopt}, configVars);
  const auto runtimeConfig = loader.Load();

  const auto* pool = runtimeConfig->GetPoolByName("test-pool");
  ASSERT_NE(pool, nullptr);
  EXPECT_EQ(pool->executorsCount, 6);
}

UTEST(ConfigLoader, HotReloadPicksUpOverrideChangeOnPoll) {
  ConfigLoaderFixture fixture;
  const auto basePath = fixture.WriteFile("base.yaml", R"(
pool:
  name: test-pool
  executorsCount: 4
)");
  const auto overridePath = fixture.WriteFile("override.yaml", R"(
pool:
  executorsCount: 4
)");

  auto loader = fixture.MakeLoader(
      {.configPath = basePath, .overridePath = overridePath});
  const auto originalConfig = loader.Load();
  ASSERT_EQ(
      loader.GetRuntimeConfig()->GetPoolByName("test-pool")->executorsCount, 4);

  std::atomic<int> reloadCount{0};
  loader.Start(
      50ms,
      [&reloadCount](std::shared_ptr<const servicelib::config::RuntimeConfig>) {
        reloadCount.fetch_add(1, std::memory_order_relaxed);
      });

  fixture.RewriteFile(overridePath, R"(
pool:
  executorsCount: 9
)");

  // Poll interval is 50ms; give it several ticks' worth of headroom rather
  // than syncing on a single boundary.
  userver::engine::SleepFor(500ms);

  EXPECT_GE(reloadCount.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(
      loader.GetRuntimeConfig()->GetPoolByName("test-pool")->executorsCount, 9);
  // A reader that acquired the previous immutable snapshot remains valid
  // across publication of the replacement, just like a Go reader holding
  // the previous RuntimeConfig value.
  EXPECT_EQ(originalConfig->GetPoolByName("test-pool")->executorsCount, 4);
}

UTEST(ConfigLoader, ReloadCallbackFailureDoesNotTurnSuccessIntoRetry) {
  ConfigLoaderFixture fixture;
  const auto basePath = fixture.WriteFile("base.yaml", R"(
pool:
  name: test-pool
  executorsCount: 4
)");
  const auto overridePath = fixture.WriteFile("override.yaml", R"(
pool:
  executorsCount: 4
)");

  auto loader = fixture.MakeLoader(
      {.configPath = basePath, .overridePath = overridePath});
  loader.Load();

  std::atomic<int> callbackCount{0};
  loader.Start(30ms,
               [&callbackCount](
                   std::shared_ptr<const servicelib::config::RuntimeConfig>) {
                 callbackCount.fetch_add(1, std::memory_order_relaxed);
                 throw std::runtime_error("consumer failed");
               });
  fixture.RewriteFile(overridePath, R"(
pool:
  executorsCount: 11
)");

  userver::engine::SleepFor(300ms);

  EXPECT_EQ(callbackCount.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(
      loader.GetRuntimeConfig()->GetPoolByName("test-pool")->executorsCount,
      11);
}

UTEST(ConfigLoader, HotReloadDoesNothingWithoutOverridePath) {
  ConfigLoaderFixture fixture;
  const auto basePath = fixture.WriteFile("base.yaml", R"(
pool:
  name: test-pool
  executorsCount: 4
)");

  auto loader = fixture.MakeLoader(
      {.configPath = basePath, .overridePath = std::nullopt});
  loader.Load();

  bool reloadCalled = false;
  // No overridePath configured — Start() must be a no-op (see loader.hpp:
  // "matches Go/Python only starting ... when an override file is
  // actually configured").
  loader.Start(20ms,
               [&reloadCalled](
                   std::shared_ptr<const servicelib::config::RuntimeConfig>) {
                 reloadCalled = true;
               });

  userver::engine::SleepFor(200ms);

  EXPECT_FALSE(reloadCalled);
  EXPECT_EQ(
      loader.GetRuntimeConfig()->GetPoolByName("test-pool")->executorsCount, 4);
}

UTEST(ConfigLoader, ReloadFailureKeepsServingPreviousConfig) {
  ConfigLoaderFixture fixture;
  const auto basePath = fixture.WriteFile("base.yaml", R"(
pool:
  name: test-pool
  executorsCount: 4
)");
  const auto overridePath = fixture.WriteFile("override.yaml", R"(
pool:
  executorsCount: 4
)");

  auto loader = fixture.MakeLoader(
      {.configPath = basePath, .overridePath = overridePath});
  loader.Load();

  loader.Start(50ms,
               [](std::shared_ptr<const servicelib::config::RuntimeConfig>) {});

  // Malformed YAML: reload must fail without disturbing the config already
  // being served.
  fixture.RewriteFile(overridePath, "pool: [this is not a mapping\n");

  userver::engine::SleepFor(300ms);

  EXPECT_EQ(
      loader.GetRuntimeConfig()->GetPoolByName("test-pool")->executorsCount, 4);
}

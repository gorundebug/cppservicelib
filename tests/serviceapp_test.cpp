#include <algorithm>
#include <atomic>
#include <chrono>
#include <concepts>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <userver/engine/sleep.hpp>
#include <userver/utest/utest.hpp>

#include <servicelib/runtime/serviceapp.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

namespace {

struct ServiceDataTypes final {
  template <typename>
  struct DataType {};
};

class Service final : public servicelib::ServiceApp<Service, ServiceDataTypes> {
};

class PriorityPoolConfig final : public servicelib::config::IConfig {
 public:
  PriorityPoolConfig()
      : pool_{.name = "Default Pool", .executorsCount = 1},
        link_{.from = 1,
              .to = 2,
              .callSemantics = servicelib::config::MakeCallSemanticsGroup(
                  servicelib::api::CallSemantics::kPriorityTaskPool,
                  "Default Pool", 1)} {}

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams()
      const override {
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
  std::vector<const servicelib::config::PoolConfig*> GetPools()
      const override {
    return {&pool_};
  }
  std::vector<const servicelib::config::LinkConfig*> GetLinks()
      const override {
    return {&link_};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override {
    return {};
  }
  std::vector<const servicelib::config::TypeConfig*> GetTypes()
      const override {
    return {};
  }

 private:
  servicelib::config::PoolConfig pool_;
  servicelib::config::LinkConfig link_;
};

static_assert(std::derived_from<servicelib::IRuntimeEnvironment,
                                servicelib::IServiceEnvironment>);
static_assert(std::derived_from<Service, servicelib::IRuntimeEnvironment>);
static_assert(std::derived_from<Service, servicelib::IServiceEnvironment>);
static_assert(
    std::derived_from<Service, servicelib::ServiceExecutionEnvironment<
                                   Service, ServiceDataTypes>>);

struct EventLog final {
  void add(std::string event) {
    std::lock_guard lock(mutex);
    events.push_back(std::move(event));
  }

  std::vector<std::string> snapshot() const {
    std::lock_guard lock(mutex);
    return events;
  }

  mutable std::mutex mutex;
  std::vector<std::string> events;
};

struct Component final {
  std::string name;
  EventLog* events{};
  bool failStart{};
  bool failStop{};
  std::chrono::milliseconds stopDelay{};
  std::atomic<bool>* stopped{};

  void start(servicelib::Context) {
    events->add(name + ":start");
    if (failStart) throw std::runtime_error("start failed");
  }

  void stop(servicelib::Context) {
    if (stopDelay > std::chrono::milliseconds::zero()) {
      userver::engine::SleepFor(stopDelay);
    }
    events->add(name + ":stop");
    if (stopped) stopped->store(true);
    if (failStop) throw std::runtime_error("stop failed");
  }
};

struct RecordingLogger final : servicelib::log::Logger {
  struct Record final {
    std::string message;
    std::string resource;
    std::string error;
  };

  void debug(std::string_view,
             std::initializer_list<servicelib::log::Field>) override {}
  void info(std::string_view,
            std::initializer_list<servicelib::log::Field>) override {}
  void error(std::string_view,
             std::initializer_list<servicelib::log::Field>) override {}
  void warn(std::string_view message,
            std::initializer_list<servicelib::log::Field> fields) override {
    Record record{.message = std::string(message), .resource = {}, .error = {}};
    for (const auto& field : fields) {
      if (field.key() == "resource") {
        record.resource = field.stringValue();
      } else if (field.key() == "error") {
        record.error = field.stringValue();
      }
    }
    records.push_back(std::move(record));
  }

  std::vector<Record> records;
};

UTEST(ServiceLifecycle, StartsAndStopsInServiceAppOrder) {
  EventLog events;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kDataSink,
                std::make_shared<Component>("sink", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDelayPool,
                std::make_shared<Component>("delay", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource,
                std::make_shared<Component>("source", &events));

  lifecycle.start(servicelib::Context{});
  lifecycle.stopBeforeGraphDrain(servicelib::Context{});

  const auto beforeGraphDrain = events.snapshot();
  EXPECT_EQ(std::find(beforeGraphDrain.begin(), beforeGraphDrain.end(),
                      "sink:stop"),
            beforeGraphDrain.end());

  lifecycle.stopAfterGraphDrain(servicelib::Context{});

  const auto recorded = events.snapshot();
  ASSERT_EQ(recorded.size(), 6);
  EXPECT_EQ(
      std::vector(recorded.begin(), recorded.begin() + 3),
      (std::vector<std::string>{"source:start", "sink:start", "delay:start"}));
  EXPECT_EQ(recorded.back(), "sink:stop");
  EXPECT_NE(std::find(recorded.begin() + 3, recorded.end(), "source:stop"),
            recorded.end());
  EXPECT_NE(std::find(recorded.begin() + 3, recorded.end(), "delay:stop"),
            recorded.end());
}

UTEST(ServiceLifecycle, RollsBackAlreadyStartedComponents) {
  EventLog events;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource,
                std::make_shared<Component>("source", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSink,
                std::make_shared<Component>("sink", &events, true));

  EXPECT_THROW(lifecycle.start(servicelib::Context{}), std::runtime_error);
  EXPECT_EQ(
      events.snapshot(),
      (std::vector<std::string>{"source:start", "sink:start", "source:stop"}));
}

UTEST(ServiceLifecycle, StopFailureDoesNotSkipOtherResources) {
  EventLog events;
  RecordingLogger logger;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kComponent,
                std::make_shared<Component>("healthy", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kComponent,
                std::make_shared<Component>("failing", &events, false, true));

  lifecycle.start(servicelib::Context{});
  EXPECT_NO_THROW(lifecycle.stop(servicelib::Context{}, logger));

  const auto recorded = events.snapshot();
  EXPECT_NE(std::find(recorded.begin(), recorded.end(), "healthy:stop"),
            recorded.end());
  EXPECT_NE(std::find(recorded.begin(), recorded.end(), "failing:stop"),
            recorded.end());
  ASSERT_EQ(logger.records.size(), 1);
  EXPECT_EQ(logger.records.front().message,
            "service shutdown operation failed");
  EXPECT_EQ(logger.records.front().resource, "component:1");
  EXPECT_EQ(logger.records.front().error, "stop failed");
}

UTEST(ServiceLifecycle, DeadlineIsUpperBoundForWholeShutdown) {
  EventLog events;
  RecordingLogger logger;
  std::atomic<bool> stopped{false};
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(
      servicelib::ServiceComponentKind::kComponent,
      std::make_shared<Component>("slow", &events, false, false,
                                  std::chrono::milliseconds{30}, &stopped));

  lifecycle.start(servicelib::Context{});
  const auto started = std::chrono::steady_clock::now();
  lifecycle.stop(servicelib::Context{}.bounded(std::chrono::milliseconds{1}),
                 logger);

  EXPECT_LT(std::chrono::steady_clock::now() - started,
            std::chrono::milliseconds{20});
  EXPECT_FALSE(stopped.load());
  ASSERT_EQ(logger.records.size(), 1);
  EXPECT_EQ(logger.records.front().message,
            "service shutdown operation timed out");
  EXPECT_EQ(logger.records.front().resource, "component:0");
  userver::engine::SleepFor(std::chrono::milliseconds{40});
  EXPECT_TRUE(stopped.load());
}

UTEST(ServiceLifecycle, DataConnectorTimeoutMatchesGoTelemetry) {
  EventLog events;
  std::atomic<bool> stopped{false};
  servicelib::testmetrics::TestMetrics metrics;
  servicelib::testlog::TestLog logger;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(
      servicelib::ServiceComponentKind::kDataSource,
      std::make_shared<Component>("source", &events, false, false,
                                  std::chrono::milliseconds{30}, &stopped),
      &metrics, &logger);

  lifecycle.start(servicelib::Context{});
  lifecycle.stop(servicelib::Context{}.bounded(std::chrono::milliseconds{1}),
                 logger);

  EXPECT_FALSE(stopped.load());
  EXPECT_EQ(metrics
                .counter("datasource_connector.events_total",
                         {{"connector", "0"}, {"event", "stop_timeout"}})
                .count(),
            1);
  const auto entries = logger.entries();
  ASSERT_EQ(entries.size(), 1);
  EXPECT_EQ(entries.front().message, "data source stopped by timeout");
  userver::engine::SleepFor(std::chrono::milliseconds{40});
  EXPECT_TRUE(stopped.load());
}

UTEST(ServiceApp, PreparesConfiguredPoolsOnGraphLookup) {
  PriorityPoolConfig config;
  servicelib::config::RuntimeConfigRegistry::Publish(
      std::make_shared<const servicelib::config::RuntimeConfig>(config));
  {
    Service service;
    EXPECT_NE(service.getPriorityTaskPool("Default Pool"), nullptr);
  }
  servicelib::config::RuntimeConfigRegistry::Publish({});
}

}  // namespace

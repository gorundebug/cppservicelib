#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <stop_token>
#include <thread>
#include <vector>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/transformation/streams.hpp>
#include <userver/engine/single_use_event.hpp>

namespace cron_interval_test {
class Config final : public servicelib::config::IConfig {
 public:
  Config() {
    connector.id = 1;
    connector.name = "interval";
    connector.implementation = servicelib::api::DataConnectorImplementation::kCppLibcron;
    endpoint.id = 1;
    endpoint.idDataConnector = 1;
    endpoint.name = "maintenance";
    endpoint.enabled = true;
    endpoint.schedule = "@every 100ms";
    endpoint.timezone = "UTC";
    endpoint.overlapPolicy = servicelib::api::ScheduleOverlapPolicy::kSkip;
  }
  std::vector<const servicelib::config::ServiceConfig*> GetServices() const override { return {}; }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override { return {}; }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors() const override { return {connector}; }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints() const override { return {endpoint}; }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override { return {}; }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override { return {}; }
  std::vector<const servicelib::config::ModuleConfig*> GetModules() const override { return {}; }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override { return {}; }
  servicelib::config::CronDataConnectorConfig connector;
  servicelib::config::CronEndpointConfig endpoint;
};

class Environment final : public servicelib::IRuntimeEnvironment {
 public:
  Environment() : runtime_(std::make_shared<servicelib::config::RuntimeConfig>(config_)) {}
  servicelib::pool::ITaskPool* getTaskPool(const std::string&) override { return nullptr; }
  servicelib::pool::IPriorityTaskPool* getPriorityTaskPool(const std::string&) override { return nullptr; }
  std::shared_ptr<const servicelib::config::RuntimeConfig> getRuntimeConfigSnapshot() const override { return runtime_; }
  std::shared_ptr<const servicelib::config::ServiceConfig> getServiceConfigSnapshot() const override { return std::make_shared<servicelib::config::ServiceConfig>(); }
  servicelib::log::Logger& getLogger() override { return servicelib::log::NoopLogger::instance(); }
  servicelib::metrics::Metrics& getMetrics() override { return servicelib::metrics::NoopMetrics::instance(); }
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }
 private:
  Config config_;
  std::shared_ptr<const servicelib::config::RuntimeConfig> runtime_;
};

class DirectContext final {
 public:
  struct InputInvocation {};
  static DirectContext& getExecutionEnvironment() {
    static DirectContext context;
    return context;
  }
  InputInvocation beginInputInvocation() const { return {}; }
  template <typename T, typename Source, typename Consumer>
  void consume(servicelib::MessageContext context, Source&, Consumer& consumer,
               servicelib::Payload<T> payload) {
    consumer.consume(std::move(context), std::move(payload));
  }
};

UTEST(CronDataSource, IntervalSourceUsesCorrelatedResultAndPreservesOwner) {
  using namespace std::chrono;
  
  Environment environment;
  using Input = servicelib::InputStream<int, int, std::exception_ptr, DirectContext>;
  servicelib::config::InputStreamConfig inputConfig;
  inputConfig.id = 1;
  inputConfig.idEndpoint = 1;
  inputConfig.name = "maintenance";
  auto input = Input::make(inputConfig, nullptr, environment);
  inputConfig.id = 2;
  inputConfig.name = "completion";
  auto result = Input::make(inputConfig, nullptr, environment);
  input->setSource(*result);

  std::stop_source cancellation;
  const auto owner = servicelib::Context{}.withStopToken(cancellation.get_token())
                         .bounded(seconds{5}).withSampling(true);
  std::atomic<unsigned> calls{0};
  std::mutex mutex;
  std::vector<system_clock::time_point> scheduled;
  std::vector<std::string> streamIds;
  userver::engine::SingleConsumerEvent release;
  auto function = [&](servicelib::MessageContext context,
                      const servicelib::ScheduleTrigger& trigger,
                      [[maybe_unused]] auto collector) {
    EXPECT_EQ(context.deadline(), owner.deadline());
    EXPECT_TRUE(context.samplingEnabled());
    EXPECT_FALSE(context.streamId().empty());
    EXPECT_EQ(trigger.backend, servicelib::ScheduleBackend::kLocal);
    {
      std::lock_guard lock(mutex);
      scheduled.push_back(trigger.scheduledAt);
      streamIds.emplace_back(context.streamId());
    }
    const auto invocation = calls.fetch_add(1);
    if (invocation == 2) {
      EXPECT_TRUE(release.WaitForEvent());
    }
    // Complete via Input's real result link, not ResultWaiter test internals.
    result->consume(context, servicelib::Payload<int>::make(1));
  };
  auto endpoint = servicelib::datasource::cron::Endpoint::make(environment, *input, function);
  auto source = servicelib::datasource::cron::LibcronDataSource::make(environment, 1);
  source->addEndpoint(endpoint);
  
  const auto waitForCalls = [&](unsigned count) {
    const auto deadline = steady_clock::now() + seconds{2};
    while (calls.load() < count && steady_clock::now() < deadline) {
      userver::engine::SleepFor(milliseconds{5});
    }
    return calls.load() >= count;
  };
  source->start(owner);
  EXPECT_TRUE(waitForCalls(3));
  userver::engine::SleepFor(milliseconds{250});
  EXPECT_EQ(calls.load(), 3U);
  EXPECT_THROW(source->stop(servicelib::Context{}.bounded(milliseconds{5})), std::runtime_error);
  EXPECT_THROW(source->start(owner), std::logic_error);
  release.Send();
  source->stop(servicelib::Context{}.bounded(seconds{2}));
  const auto restartBoundary = calls.load();
  source->start(owner);
  EXPECT_TRUE(waitForCalls(6));
  cancellation.request_stop();
  source->stop(servicelib::Context{}.bounded(seconds{2}));
  const auto stoppedCalls = calls.load();
  userver::engine::SleepFor(milliseconds{120});
  EXPECT_EQ(calls.load(), stoppedCalls);
  std::lock_guard lock(mutex);
  ASSERT_GE(scheduled.size(), 6U);
  for (std::size_t index = 1; index < scheduled.size(); ++index) {
    const auto delta = duration_cast<milliseconds>(scheduled[index] - scheduled[index - 1]);
    EXPECT_GT(delta.count(), 0);
    // Restart establishes a new interval anchor. Check cadence within each run.
    if (index != restartBoundary) {
      EXPECT_EQ(delta.count() % 100, 0);
    }
  }
  for (std::size_t index = 1; index < streamIds.size(); ++index) {
    EXPECT_NE(streamIds[index], streamIds[index - 1]);
  }
}
}  // namespace cron_interval_test

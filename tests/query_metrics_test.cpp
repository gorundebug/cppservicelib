#include <servicelib/dataquery/metrics.hpp>
#include <servicelib/dataquery/endpoint.hpp>
#include <servicelib/runtime/caller.hpp>
#include <servicelib/runtime/serviceapp.hpp>
#include <userver/engine/sleep.hpp>
#include <servicelib/runtime/pool/taskpool.hpp>
#include <userver/engine/single_consumer_event.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>
#include <userver/utest/utest.hpp>
namespace {
using namespace servicelib;
struct Config final : config::IConfig {
  config::PostgreSQLUserverDataConnectorConfig connector{.id = 8, .name = "Database",
      .connectionString = "postgresql://localhost/test"};
  config::QueryEndpointConfig endpoint{.id = 1, .name = "Rows", .idDataConnector = 8,
      .queryOperation = "Rows"};
  std::vector<const config::ServiceConfig*> GetServices() const override { return {}; }
  std::vector<config::StreamConfigRef> GetStreams() const override { return {}; }
  std::vector<config::DataConnectorConfigRef> GetDataConnectors() const override { return {connector}; }
  std::vector<config::EndpointConfigRef> GetEndpoints() const override { return {endpoint}; }
  config::PoolConfig pool{.name = "results", .executorsCount = 1};
  std::vector<const config::PoolConfig*> GetPools() const override { return {&pool}; }
  std::vector<const config::LinkConfig*> GetLinks() const override { return {}; }
  std::vector<const config::ModuleConfig*> GetModules() const override { return {}; }
  std::vector<const config::TypeConfig*> GetTypes() const override { return {}; }
};
const metrics::Labels labels{{"connector", "Database"}, {"endpoint", "Rows"}, {"operation", "Rows"}};
struct Environment final : IServiceEnvironment {
  testmetrics::TestMetrics metrics;
  Config config;
  std::shared_ptr<const config::RuntimeConfig> snapshot = std::make_shared<config::RuntimeConfig>(config);
  log::NoopLogger logger;
  std::shared_ptr<const config::RuntimeConfig> getRuntimeConfigSnapshot() const override { return snapshot; }
  std::shared_ptr<const config::ServiceConfig> getServiceConfigSnapshot() const override { return {}; }
  log::Logger& getLogger() override { return logger; }
  metrics::Metrics& getMetrics() override { return metrics; }
  tracing::Tracing* getTracing() override { return nullptr; }
};
UTEST(QueryMetrics, ConcurrentScopesAndUnwindingReleaseActiveCount) {
  Environment environment;
  dataquery::EndpointMetrics endpoint(environment, 1);
  auto& metrics = environment.metrics;
  {
    dataquery::EndpointMetrics::Request first(endpoint);
    EXPECT_EQ(metrics.gauge("dataquery_endpoint.active_requests", labels).value(), 1);
    try {
      dataquery::EndpointMetrics::Request second(endpoint);
      EXPECT_EQ(metrics.gauge("dataquery_endpoint.active_requests", labels).value(), 2);
      throw std::runtime_error("application finalizer failed");
    } catch (const std::runtime_error&) {}
    EXPECT_EQ(metrics.gauge("dataquery_endpoint.active_requests", labels).value(), 1);
  }
  EXPECT_EQ(metrics.gauge("dataquery_endpoint.active_requests", labels).value(), 0);
  EXPECT_EQ(metrics.counter("dataquery_endpoint.requests_total", labels).count(), 2);
  EXPECT_EQ(metrics.histogram("dataquery_endpoint.request_duration_seconds", labels).count(), 2);
}

UTEST(QueryMetrics, ResultsAndNativeCancellationUseBoundedLabels) {
  Environment environment;
  dataquery::EndpointMetrics endpoint(environment, 1);
  {
    dataquery::EndpointMetrics::Request request(endpoint);
    endpoint.result(); endpoint.result();
    request.outcome(std::make_exception_ptr(OperationCancelledError("cancelled")), false);
  }
  auto& metrics = environment.metrics;
  EXPECT_EQ(metrics.counter("dataquery_endpoint.results_total", labels).count(), 2);
  auto cancelled = labels; cancelled["outcome"] = "cancelled";
  EXPECT_EQ(metrics.counter("dataquery_endpoint.failures_total", cancelled).count(), 1);
  auto failure = labels; failure["outcome"] = "error";
  EXPECT_EQ(metrics.counter("dataquery_endpoint.failures_total", failure).count(), 0);
}
}

namespace {
struct QueuedQueryStream final : QueryEndpointStream<int, int, std::exception_ptr> {
  mutable Environment env;
  struct SlowConsumer {
    userver::engine::SingleConsumerEvent entered;
    userver::engine::SingleConsumerEvent release;
    std::vector<int> values;
    void consume(MessageContext, Payload<int> value) {
      if (value.get() == 0) {
        entered.Send();
        EXPECT_TRUE(release.WaitForEventFor(userver::utest::kMaxTestWaitTime));
      }
      values.push_back(value.get());
    }
  } slow;
  pool::TaskPoolImpl pool{"results", env};
  TaskPoolCaller<int, SlowConsumer> caller{slow, pool, env.logger,
      CallerBase::Params{.metricsEnabled = false}};
  IServiceEnvironment& environment() const override { return env; }
  int endpointId() const noexcept override { return 1; }
  std::size_t streamConfigId() const noexcept override { return 2; }
  void collectResult(MessageContext ctx, Payload<int> value) override {
    caller.consume(ctx, std::move(value));
  }
  void collectError(MessageContext, Payload<std::exception_ptr>) override {
    ADD_FAILURE() << "Unexpected Query error output";
  }
};
struct QueueHandler {
  using State = int;
  int ends{0};
  dataquery::BeginRequest<State> beginRequest(MessageContext ctx,
      QueryEndpointStream<int, int, std::exception_ptr>&) { return {ctx, 0, {}}; }
  void endRequest(MessageContext ctx, QueryEndpointStream<int, int, std::exception_ptr>&,
      State*, std::exception_ptr error, dataquery::Collector<int>& out,
      dataquery::Collector<std::exception_ptr>&) {
    EXPECT_FALSE(error);
    ++ends;
    out.out(ctx, 99);
  }
};
struct QueueOperation {
  void execute(MessageContext ctx, int&, const int&, QueueHandler&, dataquery::Collector<int>& out) {
    out.out(ctx, 0);
    out.out(ctx, 1);
  }
};
UTEST(QueryLifecycle, EndPrecedesTaskPoolDownstreamCompletion) {
  QueuedQueryStream stream;
  QueueHandler handler;
  dataquery::Endpoint<int, int, std::exception_ptr, QueueHandler&, QueueOperation> endpoint(
      stream, handler, QueueOperation{});
  stream.pool.start({});
  endpoint.consume(MessageContext{}, Payload<int>::make(1));
  EXPECT_TRUE(stream.slow.entered.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  EXPECT_EQ(handler.ends, 1);
  EXPECT_TRUE(stream.slow.values.empty());
  stream.slow.release.Send();
  stream.pool.stop({});
  EXPECT_EQ(stream.slow.values, (std::vector<int>{0, 1, 99}));
}
}

namespace {
struct QueryServiceTypes { template<typename> struct DataType {}; };
struct QueryService : ServiceApp<QueryService, QueryServiceTypes> {};
struct ClientLifetime {
  std::atomic<bool> closed{false};
  void start(Context) {}
  void stop(Context) { closed.store(true); }
};
struct ShutdownProbe {
  userver::engine::SingleConsumerEvent operationEntered, releaseOperation, endEntered, releaseEnd;
  std::atomic<bool> cleaned{false};
  int ends = 0;
};
struct ShutdownHandler : QueueHandler {
  ShutdownProbe& probe;
  explicit ShutdownHandler(ShutdownProbe& value) : probe(value) {}
  void endRequest(MessageContext ctx, QueryEndpointStream<int, int, std::exception_ptr>&,
      State*, std::exception_ptr error, dataquery::Collector<int>& out,
      dataquery::Collector<std::exception_ptr>&) {
    EXPECT_FALSE(error);
    EXPECT_TRUE(probe.cleaned.load());
    ++probe.ends;
    probe.endEntered.Send();
    EXPECT_TRUE(probe.releaseEnd.WaitForEventFor(userver::utest::kMaxTestWaitTime));
    out.out(ctx, 99);
  }
};
struct ShutdownOperation {
  ShutdownProbe& probe;
  void execute(MessageContext, int&, const int&, ShutdownHandler&, dataquery::Collector<int>&) {
    probe.operationEntered.Send();
    EXPECT_TRUE(probe.releaseOperation.WaitForEventFor(userver::utest::kMaxTestWaitTime));
    probe.cleaned.store(true);
  }
};
UTEST(QueryLifecycle, ServiceStopKeepsClientUntilQueryEndReturns) {
  Config config;
  config::RuntimeConfigRegistry::Publish(std::make_shared<const config::RuntimeConfig>(config));
  struct Clear { ~Clear() { config::RuntimeConfigRegistry::Publish({}); } } clear;
  QueryService service;
  auto client = std::make_shared<ClientLifetime>();
  service.registerDataSink(client); // Query infrastructure uses this existing lifecycle slot.
  service.start(Context{});
  ShutdownProbe probe;
  // No output is needed to establish the finalizer boundary; use a separate
  // direct stream rather than starting the unrelated TaskPool in this fixture.
  struct DirectStream : QueryEndpointStream<int, int, std::exception_ptr> {
    mutable Environment env;
    int delivered = 0;
    IServiceEnvironment& environment() const override { return env; }
    int endpointId() const noexcept override { return 1; }
    std::size_t streamConfigId() const noexcept override { return 2; }
    void collectResult(MessageContext, Payload<int> value) override { EXPECT_EQ(value.get(), 99); ++delivered; }
    void collectError(MessageContext, Payload<std::exception_ptr>) override { ADD_FAILURE(); }
  } direct;
  dataquery::Endpoint<int, int, std::exception_ptr, ShutdownHandler, ShutdownOperation> endpoint(
      direct, ShutdownHandler{probe}, ShutdownOperation{probe});
  auto request = userver::engine::AsyncNoTracing([&] {
    auto accepted = service.beginInputInvocation();
    endpoint.consume(MessageContext{}, Payload<int>::make(1));
  });
  EXPECT_TRUE(probe.operationEntered.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  std::atomic<bool> stopped{false};
  auto stop = userver::engine::AsyncNoTracing([&] { service.stop(Context{}); stopped.store(true); });
  userver::engine::SleepFor(std::chrono::milliseconds(10));
  EXPECT_FALSE(client->closed.load());
  EXPECT_FALSE(stopped.load());
  probe.releaseOperation.Send();
  EXPECT_TRUE(probe.endEntered.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  EXPECT_FALSE(client->closed.load());
  EXPECT_FALSE(stopped.load());
  probe.releaseEnd.Send();
  request.Get(); stop.Get();
  EXPECT_EQ(probe.ends, 1);
  EXPECT_EQ(direct.delivered, 1);
  EXPECT_TRUE(client->closed.load());
}
}

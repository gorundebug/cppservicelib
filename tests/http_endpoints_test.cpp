#include <exception>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <userver/engine/single_use_event.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/engine/task/task_with_result.hpp>
#include <userver/server/http/http_request_builder.hpp>
#include <userver/utest/utest.hpp>
#include <userver/utils/async.hpp>

#include <servicelib/datasink/http/userver.hpp>
#include <servicelib/datasource/http/userver.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>
#include <servicelib/runtime/testtracing/testtracing.hpp>
#include <servicelib/transformation/streams.hpp>

#include "test_sink_endpoint_stream.hpp"

namespace {

class TestConfig final : public servicelib::config::IConfig {
 public:
  TestConfig() {
    endpoint.id = 1;
    endpoint.name = "messages";
    endpoint.idDataConnector = 2;
    endpoint.httpMethodType = servicelib::api::HTTPMethodType::kPOST;
    endpoint.path = "/messages";
    connector.id = 2;
    connector.name = "http";
    connector.host = "127.0.0.1";
    connector.port = 8080;
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
    return {connector};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {endpoint};
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

  servicelib::config::HttpEndpointConfig endpoint;
  servicelib::config::HttpDataConnectorConfig connector;
};

class TestEnvironment final : public servicelib::IRuntimeEnvironment {
 public:
  explicit TestEnvironment(bool tracingEnabled = false)
      : runtimeConfig_(config_), tracingEnabled_(tracingEnabled) {
    serviceConfig_.name = "http-test";
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
    return std::make_shared<const servicelib::config::ServiceConfig>(
        serviceConfig_);
  }
  servicelib::log::Logger& getLogger() override { return log_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override {
    return tracingEnabled_ ? &tracing_ : nullptr;
  }

  servicelib::testmetrics::TestMetrics& metrics() { return metrics_; }
  TestConfig& config() { return config_; }

 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtimeConfig_;
  servicelib::config::ServiceConfig serviceConfig_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
  servicelib::testtracing::TestTracing tracing_;
  bool tracingEnabled_;
};

// Endpoint delivery still goes through the stream execution context. This
// direct context keeps the test focused on the typed endpoint/Input boundary;
// production applications use StreamExecutionEnvironment here.
class DirectStreamContext final {
 public:
  static DirectStreamContext& getExecutionEnvironment() {
    static DirectStreamContext context;
    return context;
  }

  template <typename T, typename Source, typename Consumer>
  void consume(servicelib::MessageContext context, Source&, Consumer& consumer,
               servicelib::Payload<T> payload) {
    consumer.consume(std::move(context), std::move(payload));
  }
};

struct ResultForwarder final {
  using Input =
      servicelib::InputStream<std::string, std::string, std::exception_ptr,
                              DirectStreamContext>;
  userver::engine::TaskWithResult<void>* task;
  std::shared_ptr<Input> resultSource;
  std::string* observed;

  void operator()(servicelib::MessageContext context,
                  const std::string& value) const {
    *observed = value;
    *task = userver::utils::Async(
        "typed-input-result",
        [source = resultSource, context = std::move(context)]() mutable {
          userver::engine::SleepFor(std::chrono::milliseconds{5});
          source->consume(std::move(context),
                          servicelib::Payload<std::string>::make("ok"));
        });
  }
};

struct SourceHandler final {
  using State = int;
  using Request = std::string;
  using Response = std::string;
  int* endCalls{};
  std::string* observedStreamId{};
  std::string* observedTraceState{};
  std::string* observedBaggage{};

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&, auto&) {
    return {std::move(context), 7};
  }

  void consumeMessage(servicelib::MessageContext context, auto& streamContext,
                      State&, auto&, auto resultContext) {
    *observedStreamId = context.streamId();
    if (observedTraceState) *observedTraceState = context.trace().traceState;
    if (observedBaggage) *observedBaggage = context.trace().baggage;
    auto* const expectedStreamContext = &streamContext;
    resultContext.setResultCallback(
        "response", [resultContext, expectedStreamContext](
                        servicelib::MessageContext, auto& callbackStreamContext,
                        State&, const std::string& value, auto& data) mutable {
          EXPECT_EQ(&callbackStreamContext, expectedStreamContext);
          data.response.SetStatus(userver::server::http::HttpStatus::kCreated);
          data.setResponseBody("reply:" + value);
          resultContext.done();
          return true;
        });
    streamContext.collect(std::move(context), std::string{"request"});
  }

  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const std::string&) {
    return "response";
  }

  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr, State&,
                  auto&) noexcept {
    ++*endCalls;
  }
};

UTEST(HttpDataSource, CorrelatesPipelineResultAndPropagatesStreamId) {
  TestEnvironment environment{true};
  int endCalls = 0;
  std::string observedStreamId;
  std::string observedTraceState;
  std::string observedBaggage;
  using Endpoint =
      servicelib::datasource::http::UserverEndpoint<std::string, std::string,
                                                    SourceHandler>;
  Endpoint* endpointPtr = nullptr;
  userver::engine::TaskWithResult<void> resultTask;
  Endpoint endpoint{
      environment, 1,
      SourceHandler{&endCalls, &observedStreamId, &observedTraceState,
                    &observedBaggage},
      [&](servicelib::MessageContext context,
          servicelib::Payload<std::string> value) {
        EXPECT_EQ(value.get(), "request");
        resultTask = userver::utils::Async(
            "http-datasource-result",
            [endpointPtr, context = std::move(context)]() mutable {
              userver::engine::SleepFor(std::chrono::milliseconds{5});
              endpointPtr->consumeResult(
                  std::move(context),
                  servicelib::Payload<std::string>::make("ok"));
            });
      },
      true};
  endpointPtr = &endpoint;
  endpoint.start(servicelib::Context{});

  auto request = userver::server::http::HttpRequestBuilder{}
                     .SetMethod(userver::server::http::HttpMethod::kPost)
                     .SetRequestPath("/messages")
                     .AddHeader("x-stream-id", "incoming-id")
                     .AddHeader("x-trace", "1")
                     .AddHeader("tracestate", "vendor=value")
                     .AddHeader("baggage", "tenant=acme")
                     .Build();
  EXPECT_EQ(endpoint.handle(*request), "reply:ok");
  EXPECT_EQ(request->GetHttpResponse().GetStatus(),
            userver::server::http::HttpStatus::kCreated);
  EXPECT_EQ(observedStreamId, "incoming-id");
  EXPECT_EQ(observedTraceState, "vendor=value");
  EXPECT_EQ(observedBaggage, "tenant=acme");
  EXPECT_EQ(endCalls, 1);
  resultTask.Get();

  endpoint.stop(servicelib::Context{});
}

struct NoResultSourceHandler {
  using State = int;
  using Request = std::string;
  using Response = std::string;
  int* consumed{};
  std::string* observedTraceState{};
  std::string* observedBaggage{};

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext context, auto& streamContext,
                      State&, auto& data, auto) {
    ++*consumed;
    if (observedTraceState) *observedTraceState = context.trace().traceState;
    if (observedBaggage) *observedBaggage = context.trace().baggage;
    streamContext.collect(std::move(context), std::string{"value"});
    data.setResponseBody("accepted");
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const std::string&) {
    return {};
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr, State&,
                  auto&) noexcept {}
};

UTEST(HttpDataSource, NoResultDoesNotWaitAndRejectsInvalidMethod) {
  TestEnvironment environment;
  int consumed = 0;
  std::string observedTraceState;
  std::string observedBaggage;
  servicelib::datasource::http::UserverEndpoint<std::string, std::string,
                                                NoResultSourceHandler>
      endpoint{
          environment, 1,
          NoResultSourceHandler{&consumed, &observedTraceState,
                                &observedBaggage},
          [](servicelib::MessageContext, servicelib::Payload<std::string>) {},
          false};

  auto get = userver::server::http::HttpRequestBuilder{}
                 .SetMethod(userver::server::http::HttpMethod::kGet)
                 .SetRequestPath("/messages")
                 .Build();
  EXPECT_TRUE(endpoint.handle(*get).empty());
  EXPECT_EQ(get->GetHttpResponse().GetStatus(),
            userver::server::http::HttpStatus::kMethodNotAllowed);
  EXPECT_EQ(consumed, 0);

  auto post = userver::server::http::HttpRequestBuilder{}
                  .SetMethod(userver::server::http::HttpMethod::kPost)
                  .SetRequestPath("/messages")
                  .AddHeader("x-trace", "1")
                  .AddHeader("tracestate", "vendor=value")
                  .AddHeader("baggage", "tenant=acme")
                  .Build();
  EXPECT_EQ(endpoint.handle(*post), "accepted");
  EXPECT_EQ(consumed, 1);
  EXPECT_TRUE(observedTraceState.empty());
  EXPECT_TRUE(observedBaggage.empty());
}

UTEST(HttpDataSource, TypedEndpointConsumerFeedsInputStream) {
  TestEnvironment environment;
  servicelib::config::InputStreamConfig config;
  config.id = 12;
  config.name = "messages-input";
  config.idEndpoint = 1;

  auto input =
      servicelib::makeInputStream<std::string, std::string, std::exception_ptr,
                                  DirectStreamContext>(config, nullptr,
                                                       environment);
  std::string observed;
  servicelib::config::SinkStreamConfig sinkConfig;
  sinkConfig.id = 13;
  sinkConfig.name = "messages-observer";
  input->sink(sinkConfig, servicelib::StreamType<std::exception_ptr>{},
              servicelib::StreamFunction(
                  [&observed](servicelib::MessageContext,
                              const std::string& value) { observed = value; }));

  int consumed = 0;
  auto endpointConsumer = servicelib::datasource::http::UserverEndpointConsumer<
      std::string, std::string, std::exception_ptr, DirectStreamContext,
      NoResultSourceHandler>::make(environment, *input,
                                   NoResultSourceHandler{&consumed});
  endpointConsumer->endpoint()->start(servicelib::Context{});

  auto request = userver::server::http::HttpRequestBuilder{}
                     .SetMethod(userver::server::http::HttpMethod::kPost)
                     .SetRequestPath("/messages")
                     .Build();
  EXPECT_EQ(endpointConsumer->endpoint()->handle(*request), "accepted");
  EXPECT_EQ(consumed, 1);
  EXPECT_EQ(observed, "value");

  endpointConsumer->endpoint()->stop(servicelib::Context{});
}

UTEST(HttpDataSource, InputResultSourceCompletesHttpRequest) {
  TestEnvironment environment;
  servicelib::config::InputStreamConfig inputConfig;
  inputConfig.id = 20;
  inputConfig.name = "request-input";
  inputConfig.idEndpoint = 1;
  servicelib::config::InputStreamConfig resultConfig;
  resultConfig.id = 21;
  resultConfig.name = "result-source";

  using Input =
      servicelib::InputStream<std::string, std::string, std::exception_ptr,
                              DirectStreamContext>;
  auto input = Input::make(inputConfig, nullptr, environment);
  auto resultSource = Input::make(resultConfig, nullptr, environment);
  input->setSource(*resultSource);

  userver::engine::TaskWithResult<void> resultTask;
  std::string observedRequest;
  servicelib::config::SinkStreamConfig sinkConfig;
  sinkConfig.id = 22;
  sinkConfig.name = "request-processor";
  ResultForwarder forwarder{&resultTask, resultSource, &observedRequest};
  input->sink(sinkConfig, servicelib::StreamType<std::exception_ptr>{},
              servicelib::StreamFunction(std::ref(forwarder)));

  int endCalls = 0;
  std::string observedStreamId;
  auto endpointConsumer = servicelib::datasource::http::UserverEndpointConsumer<
      std::string, std::string, std::exception_ptr, DirectStreamContext,
      SourceHandler>::make(environment, *input,
                           SourceHandler{&endCalls, &observedStreamId});
  endpointConsumer->endpoint()->start(servicelib::Context{});

  auto request = userver::server::http::HttpRequestBuilder{}
                     .SetMethod(userver::server::http::HttpMethod::kPost)
                     .SetRequestPath("/messages")
                     .Build();
  EXPECT_EQ(endpointConsumer->endpoint()->handle(*request), "reply:ok");
  EXPECT_EQ(observedRequest, "request");
  EXPECT_EQ(endCalls, 1);
  resultTask.Get();

  endpointConsumer->endpoint()->stop(servicelib::Context{});
}

struct FailingBeginSourceHandler final : NoResultSourceHandler {
  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext, auto&,
                                              auto& data) {
    data.response.SetStatus(userver::server::http::HttpStatus::kBadRequest);
    data.setResponseBody("bad request");
    throw std::runtime_error("invalid request");
  }
};

UTEST(HttpDataSource, BeginFailureUsesHandlerResponseAndSkipsEnd) {
  TestEnvironment environment;
  int consumed = 0;
  servicelib::datasource::http::UserverEndpoint<std::string, std::string,
                                                FailingBeginSourceHandler>
      endpoint{
          environment, 1, FailingBeginSourceHandler{{&consumed}},
          [](servicelib::MessageContext, servicelib::Payload<std::string>) {},
          false};
  auto request = userver::server::http::HttpRequestBuilder{}
                     .SetMethod(userver::server::http::HttpMethod::kPost)
                     .SetRequestPath("/messages")
                     .Build();
  EXPECT_EQ(endpoint.handle(*request), "bad request");
  EXPECT_EQ(request->GetHttpResponse().GetStatus(),
            userver::server::http::HttpStatus::kBadRequest);
  EXPECT_EQ(consumed, 0);
}

UTEST(HttpDataSource, CancellationIsVisibleAndPendingAgeIsObservable) {
  TestEnvironment environment;
  int endCalls = 0;
  std::string observedStreamId;
  servicelib::MessageContext pipelineContext;
  userver::engine::SingleUseEvent collected;
  using Endpoint =
      servicelib::datasource::http::UserverEndpoint<std::string, std::string,
                                                    SourceHandler>;
  Endpoint endpoint{environment, 1, SourceHandler{&endCalls, &observedStreamId},
                    [&](servicelib::MessageContext context,
                        servicelib::Payload<std::string>) {
                      pipelineContext = std::move(context);
                      collected.Send();
                    },
                    true};
  endpoint.start(servicelib::Context{});

  auto request = userver::server::http::HttpRequestBuilder{}
                     .SetMethod(userver::server::http::HttpMethod::kPost)
                     .SetRequestPath("/messages")
                     .Build();
  auto requestTask = userver::utils::Async(
      "cancel-http-datasource", [&] { return endpoint.handle(*request); });
  collected.Wait();
  userver::engine::SleepFor(std::chrono::milliseconds{5});

  const servicelib::metrics::Labels labels{{"connector", "http"},
                                           {"endpoint", "messages"}};
  EXPECT_GT(environment.metrics()
                .observableGauge(
                    "datasource_endpoint.pending_oldest_age_seconds", labels)
                .value(),
            0.0);
  EXPECT_FALSE(pipelineContext.cancelled());

  requestTask.RequestCancel();
  static_cast<void>(requestTask.Get());
  EXPECT_TRUE(pipelineContext.cancelled());
  EXPECT_EQ(endCalls, 1);
  EXPECT_DOUBLE_EQ(
      environment.metrics()
          .observableGauge("datasource_endpoint.pending_oldest_age_seconds",
                           labels)
          .value(),
      0.0);
  endpoint.stop(servicelib::Context{});
}

class MockSinkClient final : public servicelib::datasink::http::Client {
 public:
  servicelib::datasink::http::Response perform(
      servicelib::datasink::http::Request request) override {
    lastRequest = std::move(request);
    ++calls;
    return {userver::clients::http::Status::kOk, "response", {}};
  }

  int calls{};
  std::optional<servicelib::datasink::http::Request> lastRequest;
};

struct SinkHandler {
  using State = int;
  int* endCalls{};
  bool* hadError{};

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 1};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value,
                      servicelib::datasink::http::Requester& requester) {
    auto& request =
        requester.newRequest("POST", "http://example.test/data", value);
    request.headers[std::string{"content-type"}] = "text/plain";
  }
  void handleResponse(servicelib::MessageContext context, auto& streamContext,
                      State&,
                      const servicelib::datasink::http::Response& response) {
    streamContext.collect(std::move(context), response.body);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    ++*endCalls;
    *hadError = static_cast<bool>(error);
  }
};

UTEST(HttpDataSink, ExecutesUserRequestAndPropagatesStreamId) {
  TestEnvironment environment;
  MockSinkClient client;
  int endCalls = 0;
  bool hadError = true;
  std::string result;
  TestSinkEndpointStream<std::string, std::string> stream{
      environment, 1,
      [&](servicelib::MessageContext,
          servicelib::Payload<std::string> value) { result = value.get(); }};
  servicelib::datasink::http::UserverEndpoint<std::string, std::string,
                                              SinkHandler>
      endpoint{stream, client, SinkHandler{&endCalls, &hadError}};

  endpoint.consume(
      servicelib::MessageContext{}
          .withStreamId("stream-42")
          .withSampling(true)
          .withTrace({"4bf92f3577b34da6a3ce929d0e0e4736",
                      "00f067aa0ba902b7", true, "vendor=value",
                      "tenant=acme"}),
      servicelib::Payload<std::string>::make("payload"));

  ASSERT_TRUE(client.lastRequest.has_value());
  EXPECT_EQ(client.calls, 1);
  EXPECT_EQ(client.lastRequest->url, "http://example.test/data");
  EXPECT_EQ(client.lastRequest->body, "payload");
  const auto& requestStreamId =
      client.lastRequest->headers[std::string{"x-stream-id"}];
  EXPECT_FALSE(requestStreamId.empty());
  EXPECT_NE(requestStreamId, "stream-42");
  EXPECT_EQ(client.lastRequest->headers[std::string{"traceparent"}],
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
  EXPECT_EQ(client.lastRequest->headers[std::string{"tracestate"}],
            "vendor=value");
  EXPECT_EQ(client.lastRequest->headers[std::string{"baggage"}],
            "tenant=acme");
  EXPECT_EQ(result, "response");
  EXPECT_EQ(endCalls, 1);
  EXPECT_FALSE(hadError);
  const servicelib::metrics::Labels labels{{"connector", "http"},
                                            {"endpoint", "messages"}};
  EXPECT_EQ(environment.metrics()
                .counter("datasink_endpoint.messages_total", labels)
                .count(),
            1);
  EXPECT_EQ(environment.metrics()
                .gauge("datasink_endpoint.active_requests", labels)
                .value(),
            0);
  EXPECT_EQ(environment.metrics()
                .histogram("datasink_endpoint.request_duration_seconds",
                           labels)
                .count(),
            1);
}

struct MissingRequestHandler final : SinkHandler {
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string&,
                      servicelib::datasink::http::Requester&) {}
};

UTEST(HttpDataSink, MissingRequestIsReportedToEndRequest) {
  TestEnvironment environment;
  MockSinkClient client;
  int endCalls = 0;
  bool hadError = false;
  TestSinkEndpointStream<std::string, std::string> stream{environment, 1};
  servicelib::datasink::http::UserverEndpoint<std::string, std::string,
                                              MissingRequestHandler>
      endpoint{stream, client, MissingRequestHandler{{&endCalls, &hadError}}};

  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("payload"));
  EXPECT_EQ(client.calls, 0);
  EXPECT_EQ(endCalls, 1);
  EXPECT_TRUE(hadError);
}

struct FailingBeginSinkHandler final : SinkHandler {
  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext,
                                              auto&) {
    throw std::runtime_error("cannot begin");
  }
};

UTEST(HttpDataSink, BeginFailureStopsWithoutEndRequestOrHttpCall) {
  TestEnvironment environment;
  MockSinkClient client;
  int endCalls = 0;
  bool hadError = false;
  TestSinkEndpointStream<std::string, std::string> stream{environment, 1};
  servicelib::datasink::http::UserverEndpoint<std::string, std::string,
                                              FailingBeginSinkHandler>
      endpoint{stream, client,
               FailingBeginSinkHandler{{&endCalls, &hadError}}};
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("payload"));
  EXPECT_EQ(client.calls, 0);
  EXPECT_EQ(endCalls, 0);
}

UTEST(HttpConnectors, OwnEndpointsAndResolveConfigThroughEnvironment) {
  TestEnvironment environment;
  int consumed = 0;
  auto sourceEndpoint =
      std::make_shared<servicelib::datasource::http::UserverEndpoint<
          std::string, std::string, NoResultSourceHandler>>(
          environment, 1, NoResultSourceHandler{&consumed},
          [](servicelib::MessageContext, servicelib::Payload<std::string>) {},
          false);

  servicelib::config::InputStreamConfig inputConfig;
  inputConfig.id = 30;
  inputConfig.name = "http-connector-input";
  inputConfig.idEndpoint = 1;
  auto input = servicelib::makeInputStream<
      std::string, std::string, std::exception_ptr, DirectStreamContext>(
      inputConfig, nullptr, environment);
  auto source = servicelib::datasource::http::UserverDataSource::make(
      environment, *input);
  EXPECT_EQ(source->config().name, "http");
  source->addEndpoint(sourceEndpoint);
  EXPECT_EQ(source->endpoint(1), sourceEndpoint);
  EXPECT_THROW(source->addEndpoint(sourceEndpoint), std::invalid_argument);
  source->start(servicelib::Context{});
  source->stop(servicelib::Context{});

  MockSinkClient client;
  int endCalls = 0;
  bool hadError = false;
  TestSinkEndpointStream<std::string, std::string> stream{environment, 1};
  auto sinkEndpoint =
      std::make_shared<servicelib::datasink::http::UserverEndpoint<
          std::string, std::string, SinkHandler>>(
          stream, client, SinkHandler{&endCalls, &hadError});
  auto sink = servicelib::datasink::http::UserverDataSink::make(stream);
  EXPECT_EQ(sink->config().name, "http");
  sink->addEndpoint(sinkEndpoint);
  EXPECT_EQ(sink->endpoint(1), sinkEndpoint);
  EXPECT_THROW(sink->addEndpoint(sinkEndpoint), std::invalid_argument);
  sink->start(servicelib::Context{});
  sink->stop(servicelib::Context{});
}

UTEST(HttpConnectors, DedicatedListenerIsRejected) {
  TestEnvironment environment;
  environment.config().connector.useDedicatedListener = true;
  environment.config().connector.name = "Primary_HTTPSource";
  environment.config().connector.host = "127.0.0.1";
  environment.config().connector.port = 19091;
  servicelib::config::InputStreamConfig inputConfig;
  inputConfig.id = 31;
  inputConfig.name = "dedicated-listener-input";
  inputConfig.idEndpoint = 1;
  auto input = servicelib::makeInputStream<
      std::string, std::string, std::exception_ptr, DirectStreamContext>(
      inputConfig, nullptr, environment);
  EXPECT_THROW(
      servicelib::datasource::http::UserverDataSource::make(environment,
                                                            *input),
      std::invalid_argument);
}

}  // namespace

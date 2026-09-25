#include <atomic>
#include <chrono>
#include <exception>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <userver/engine/single_use_event.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/utest/utest.hpp>
#include <userver/utils/async.hpp>

#include <servicelib/datasink/grpc/userver.hpp>
#include <servicelib/datasource/grpc/userver.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include "test_sink_endpoint_stream.hpp"
#include "test_endpoint_lifecycle.hpp"

namespace {

class TestConfig final : public servicelib::config::IConfig {
 public:
  TestConfig() {
    connector.id = 10;
    connector.name = "grpc";
    connector.address = "localhost:9201";
    const std::vector<servicelib::api::GrpcMethodType> methods{
        servicelib::api::GrpcMethodType::kNoStreaming,
        servicelib::api::GrpcMethodType::kServerStreaming,
        servicelib::api::GrpcMethodType::kClientStreaming,
        servicelib::api::GrpcMethodType::kBidirectionalStreaming};
    for (std::size_t i = 0; i < methods.size(); ++i) {
      endpoints[i].id = static_cast<int>(i + 1);
      endpoints[i].name = "grpc-" + std::to_string(i + 1);
      endpoints[i].idDataConnector = connector.id;
      endpoints[i].grpcMethodType = methods[i];
      endpoints[i].methodName = endpoints[i].name;
    }
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
    return {endpoints[0], endpoints[1], endpoints[2], endpoints[3]};
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

  servicelib::config::GrpcDataConnectorConfig connector;
  servicelib::config::GrpcEndpointConfig endpoints[4];
};

class TestEnvironment final : public servicelib::IRuntimeEnvironment {
 public:
  TestEnvironment() : runtimeConfig_(config_) { service_.name = "grpc-test"; }
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
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }

 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtimeConfig_;
  servicelib::config::ServiceConfig service_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};

struct SourceHandler {
  using State = int;
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext context, auto& sc, State&,
                      const std::string& request, auto result, auto&) {
    result.setResultCallback(
        "result", [result](servicelib::MessageContext, auto&, State&,
                           const std::string& value, auto& sender) mutable {
          sender.send("reply:" + value);
          result.done();
          return true;
        });
    sc.collect(std::move(context), request);
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const std::string&) {
    return "result";
  }
  void eof(servicelib::MessageContext, auto&, State&) {}
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr,
                  State&) {}
};

struct FakeWriter {
  void Write(std::string&& value) { values.push_back(std::move(value)); }
  std::vector<std::string> values;
};

struct FakeReader {
  bool Read(std::string& value) {
    if (next == values.size()) return false;
    value = values[next++];
    return true;
  }
  std::vector<std::string> values;
  std::size_t next{};
};

struct FakeReaderWriter : FakeReader, FakeWriter {};

struct RetainedSourceHandler final : SourceHandler {
  void consumeMessage(servicelib::MessageContext context, auto& sc, State&,
                      const std::string& request, auto result, auto&) {
    result.setResultCallback(
        "result", [result, calls = 0](servicelib::MessageContext, auto&, State&,
                                     const std::string&, auto& sender) mutable {
          sender.send(std::to_string(++calls));
          if (calls == 2) result.done();
          return calls == 2;
        });
    sc.collect(context, request);
    sc.collect(std::move(context), request);
  }
};

UTEST(GrpcDataSource, RetainsCallbackAcrossResults) {
  TestEnvironment environment;
  using Server = servicelib::datasource::grpc::ServerStreamingEndpoint<
      std::string, std::string, std::string, std::string, RetainedSourceHandler>;
  Server* endpointPtr{};
  Server endpoint{environment, 2, RetainedSourceHandler{},
                  [&](servicelib::MessageContext context,
                      servicelib::Payload<std::string> value) {
                    endpointPtr->consumeResult(std::move(context), std::move(value));
                  }, true};
  endpointPtr = &endpoint;
  endpoint.start(servicelib::Context{});
  FakeWriter writer;
  endpoint.handle(servicelib::MessageContext{}.withStreamId("retained"),
                  "request", writer);
  endpoint.stop(servicelib::Context{});
  EXPECT_EQ(writer.values, (std::vector<std::string>{"1", "2"}));
}

UTEST(GrpcTracing, RequiresExplicitSampledTraceParent) {
  using servicelib::tracing::SampledTraceParent;
  EXPECT_FALSE(SampledTraceParent(""));
  EXPECT_FALSE(SampledTraceParent("00"));
  EXPECT_FALSE(SampledTraceParent(
      "00-00000000000000000000000000000001-0000000000000001-00"));
  EXPECT_TRUE(SampledTraceParent(
      "00-00000000000000000000000000000001-0000000000000001-01"));
  EXPECT_TRUE(SampledTraceParent(
      "00-00000000000000000000000000000001-0000000000000001-03"));
  EXPECT_FALSE(SampledTraceParent(
      "00-00000000000000000000000000000001-0000000000000001-g1"));
  EXPECT_FALSE(SampledTraceParent(
      "00-00000000000000000000000000000000-0000000000000001-01"));
}

UTEST(GrpcDataSource, SupportsAllFourMethodTypesAndCorrelation) {
  TestEnvironment environment;

  using Unary = servicelib::datasource::grpc::NoStreamingEndpoint<
      std::string, std::string, std::string, std::string, SourceHandler>;
  Unary* unaryPtr{};
  Unary unary{environment, 1, SourceHandler{},
              [&](servicelib::MessageContext context,
                  servicelib::Payload<std::string> value) {
                unaryPtr->consumeResult(
                    std::move(context),
                    servicelib::Payload<std::string>::make(value.get()));
              },
              true};
  unaryPtr = &unary;
  unary.start(servicelib::Context{});
  EXPECT_EQ(
      unary.handle(servicelib::MessageContext{}.withStreamId("unary"), "one"),
      "reply:one");
  unary.stop(servicelib::Context{});

  using Server = servicelib::datasource::grpc::ServerStreamingEndpoint<
      std::string, std::string, std::string, std::string, SourceHandler>;
  Server* serverPtr{};
  Server server{environment, 2, SourceHandler{},
                [&](servicelib::MessageContext context,
                    servicelib::Payload<std::string> value) {
                  serverPtr->consumeResult(
                      std::move(context),
                      servicelib::Payload<std::string>::make(value.get()));
                },
                true};
  serverPtr = &server;
  server.start(servicelib::Context{});
  FakeWriter writer;
  server.handle(servicelib::MessageContext{}.withStreamId("server"), "two",
                writer);
  ASSERT_EQ(writer.values.size(), 1);
  EXPECT_EQ(writer.values[0], "reply:two");
  server.stop(servicelib::Context{});

  using Client = servicelib::datasource::grpc::ClientStreamingEndpoint<
      std::string, std::string, std::string, std::string, SourceHandler>;
  Client* clientPtr{};
  Client client{environment, 3, SourceHandler{},
                [&](servicelib::MessageContext context,
                    servicelib::Payload<std::string> value) {
                  clientPtr->consumeResult(
                      std::move(context),
                      servicelib::Payload<std::string>::make(value.get()));
                },
                true};
  clientPtr = &client;
  client.start(servicelib::Context{});
  FakeReader reader{{"three"}};
  EXPECT_EQ(client.handle(servicelib::MessageContext{}.withStreamId("client"),
                          reader),
            "reply:three");
  client.stop(servicelib::Context{});

  using Bidi = servicelib::datasource::grpc::BidirectionalStreamingEndpoint<
      std::string, std::string, std::string, std::string, SourceHandler>;
  Bidi* bidiPtr{};
  Bidi bidi{environment, 4, SourceHandler{},
            [&](servicelib::MessageContext context,
                servicelib::Payload<std::string> value) {
              bidiPtr->consumeResult(
                  std::move(context),
                  servicelib::Payload<std::string>::make(value.get()));
            },
            true};
  bidiPtr = &bidi;
  bidi.start(servicelib::Context{});
  FakeReaderWriter rw;
  rw.FakeReader::values = {"four"};
  bidi.handle(servicelib::MessageContext{}.withStreamId("bidi"), rw);
  ASSERT_EQ(rw.FakeWriter::values.size(), 1);
  EXPECT_EQ(rw.FakeWriter::values[0], "reply:four");
  bidi.stop(servicelib::Context{});
}

struct SinkHandler final {
  using State = int;
  std::vector<std::string>* responses{};
  userver::engine::SingleUseEvent* ended{};

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& sender, auto result) {
    sender.send(value);
    if (value == "last") result.done();
  }
  void handleResponse(servicelib::MessageContext, auto&, State&,
                      const std::string& response) {
    responses->push_back(response);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr,
                  State&) noexcept {
    if (ended) ended->Send();
  }
};

struct FakeClientStreamState {
  std::vector<std::string> requests;
};

struct FakeClientStream {
  std::shared_ptr<FakeClientStreamState> state;
  void WriteAndCheck(const std::string& request) {
    state->requests.push_back(request);
  }
  std::string Finish() { return "client:" + state->requests.back(); }
};

struct FakeBidiState {
  std::vector<std::string> requests;
  userver::engine::SingleUseEvent done;
};

struct FakeBidiStream {
  std::shared_ptr<FakeBidiState> state;
  bool read{};
  void WriteAndCheck(const std::string& request) {
    state->requests.push_back(request);
  }
  bool WritesDone() {
    state->done.Send();
    return true;
  }
  bool Read(std::string& response) {
    state->done.Wait();
    if (read) return false;
    read = true;
    response = "bidi:" + state->requests.back();
    return true;
  }
};

template <typename Cell>
void CheckCancelledSessionWaiter() {
  auto cell = std::make_shared<Cell>();
  userver::engine::SingleUseEvent entered;
  std::atomic<bool> returned{false};
  auto waiter = userver::utils::Async("grpc-session-waiter", [&] {
    entered.Send();
    cell->wait();
    EXPECT_TRUE(cell->ready);
    EXPECT_NE(cell->error, nullptr);
    returned.store(true);
  });
  entered.Wait();
  waiter.RequestCancel();
  userver::engine::SleepFor(std::chrono::milliseconds(5));
  EXPECT_FALSE(returned.load());
  cell->error = std::make_exception_ptr(std::runtime_error("creation failed"));
  cell->markReady();
  EXPECT_NO_THROW(waiter.Get());
  EXPECT_TRUE(returned.load());
}

UTEST(GrpcDataSink, ClientStreamingCancellationWaitsForSessionPublication) {
  auto client = [](userver::ugrpc::client::CallOptions) {
    return FakeClientStream{std::make_shared<FakeClientStreamState>()};
  };
  using Endpoint = servicelib::datasink::grpc::ClientStreamingEndpoint<
      std::string, std::string, std::string, std::string, SinkHandler,
      decltype(client)>;
  CheckCancelledSessionWaiter<typename Endpoint::SessionCell>();
}

UTEST(GrpcDataSink, BidiCancellationWaitsForSessionPublication) {
  auto client = [](userver::ugrpc::client::CallOptions) {
    return FakeBidiStream{std::make_shared<FakeBidiState>()};
  };
  using Endpoint = servicelib::datasink::grpc::BidirectionalStreamingEndpoint<
      std::string, std::string, std::string, std::string, SinkHandler,
      decltype(client)>;
  CheckCancelledSessionWaiter<typename Endpoint::SessionCell>();
}

UTEST(GrpcDataSink, SupportsAllFourMethodTypesAndStreamIdSessions) {
  TestEnvironment environment;
  std::vector<std::string> responses;
  TestSinkEndpointStream<std::string, std::string> unaryStream{environment, 1};
  TestSinkEndpointStream<std::string, std::string> serverStream{environment, 2};
  TestSinkEndpointStream<std::string, std::string> clientStream{environment, 3};
  TestSinkEndpointStream<std::string, std::string> bidiStream{environment, 4};

  auto unaryClient = [](const std::string& request,
                        userver::ugrpc::client::CallOptions) {
    return "unary:" + request;
  };
  servicelib::datasink::grpc::NoStreamingEndpoint<
      std::string, std::string, std::string, std::string, SinkHandler,
      decltype(unaryClient)>
      unary{unaryStream, SinkHandler{&responses}, unaryClient};
  unary.consume(servicelib::MessageContext{},
                servicelib::Payload<std::string>::make("one"));

  struct ServerRpc {
    bool read{};
    std::string value;
    bool Read(std::string& response) {
      if (read) return false;
      read = true;
      response = "server:" + value;
      return true;
    }
  };
  auto serverClient = [](const std::string& request,
                         userver::ugrpc::client::CallOptions) {
    return ServerRpc{false, request};
  };
  servicelib::datasink::grpc::ServerStreamingEndpoint<
      std::string, std::string, std::string, std::string, SinkHandler,
      decltype(serverClient)>
      server{serverStream, SinkHandler{&responses}, serverClient};
  server.consume(servicelib::MessageContext{},
                 servicelib::Payload<std::string>::make("two"));

  auto clientState = std::make_shared<FakeClientStreamState>();
  userver::engine::SingleUseEvent clientEnded;
  auto clientFn = [clientState](userver::ugrpc::client::CallOptions) {
    return FakeClientStream{clientState};
  };
  servicelib::datasink::grpc::ClientStreamingEndpoint<
      std::string, std::string, std::string, std::string, SinkHandler,
      decltype(clientFn)>
      client{clientStream, SinkHandler{&responses, &clientEnded}, clientFn};
  client.start(servicelib::Context{});
  auto clientContext =
      servicelib::MessageContext{}.withStreamId("client-stream");
  client.consume(clientContext,
                 servicelib::Payload<std::string>::make("first"));
  client.consume(clientContext, servicelib::Payload<std::string>::make("last"));
  clientEnded.Wait();
  client.stop(servicelib::Context{});
  EXPECT_EQ(clientState->requests, (std::vector<std::string>{"first", "last"}));

  auto bidiState = std::make_shared<FakeBidiState>();
  userver::engine::SingleUseEvent bidiEnded;
  auto bidiFn = [bidiState](userver::ugrpc::client::CallOptions) {
    return FakeBidiStream{bidiState};
  };
  servicelib::datasink::grpc::BidirectionalStreamingEndpoint<
      std::string, std::string, std::string, std::string, SinkHandler,
      decltype(bidiFn)>
      bidi{bidiStream, SinkHandler{&responses, &bidiEnded}, bidiFn};
  bidi.start(servicelib::Context{});
  auto bidiContext = servicelib::MessageContext{}.withStreamId("bidi-stream");
  bidi.consume(bidiContext, servicelib::Payload<std::string>::make("first"));
  bidi.consume(bidiContext, servicelib::Payload<std::string>::make("last"));
  bidiEnded.Wait();
  bidi.stop(servicelib::Context{});

  EXPECT_EQ(responses, (std::vector<std::string>{"unary:one", "server:two",
                                                 "client:last", "bidi:last"}));
}

struct GrpcConsumeGate final {
  userver::engine::SingleUseEvent entered;
  userver::engine::SingleUseEvent release;

  void wait() {
    entered.Send();
    EXPECT_EQ(release.WaitUntil(userver::engine::Deadline::FromDuration(
                  std::chrono::seconds{2})),
              userver::engine::FutureStatus::kReady);
  }
};

struct GatedGrpcSinkHandler final {
  using State = int;
  GrpcConsumeGate* responseGate;
  GrpcConsumeGate* endGate;
  std::vector<std::string>* responses;

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& sender, auto) {
    sender.send(value);
  }
  void handleResponse(servicelib::MessageContext, auto&, State&,
                      const std::string& response) {
    responseGate->wait();
    responses->push_back(response);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_EQ(error, nullptr);
    endGate->wait();
  }
};

template <bool ServerStreaming>
void CheckGrpcSynchronousConsume() {
  TestEnvironment environment;
  GrpcConsumeGate transportGate;
  GrpcConsumeGate responseGate;
  GrpcConsumeGate endGate;
  std::vector<std::string> responses;
  std::atomic<bool> returned{false};
  TestSinkEndpointStream<std::string, std::string> stream{
      environment, ServerStreaming ? 2 : 1};
  struct Rpc final {
    bool read = false;
    bool Read(std::string& response) {
      if (read) return false;
      read = true;
      response = "response";
      return true;
    }
  };
  auto client = [&](const std::string&, userver::ugrpc::client::CallOptions) {
    transportGate.wait();
    if constexpr (ServerStreaming) {
      return Rpc{};
    } else {
      return std::string{"response"};
    }
  };
  using Endpoint = std::conditional_t<
      ServerStreaming,
      servicelib::datasink::grpc::ServerStreamingEndpoint<
          std::string, std::string, std::string, std::string,
          GatedGrpcSinkHandler, decltype(client)>,
      servicelib::datasink::grpc::NoStreamingEndpoint<
          std::string, std::string, std::string, std::string,
          GatedGrpcSinkHandler, decltype(client)>>;
  Endpoint endpoint{stream,
                    GatedGrpcSinkHandler{&responseGate, &endGate, &responses},
                    client};
  auto task = userver::utils::Async("grpc-consume-return-contract", [&] {
    endpoint.consume(servicelib::MessageContext{},
                     servicelib::Payload<std::string>::make("payload"));
    returned.store(true);
  });
  for (auto* gate : {&transportGate, &responseGate, &endGate}) {
    EXPECT_EQ(gate->entered.WaitUntil(userver::engine::Deadline::FromDuration(
                  std::chrono::seconds{2})),
              userver::engine::FutureStatus::kReady);
    EXPECT_FALSE(returned.load());
    gate->release.Send();
  }
  task.Get();
  EXPECT_TRUE(returned.load());
  EXPECT_EQ(responses, (std::vector<std::string>{"response"}));
}

UTEST(GrpcDataSink, UnaryConsumeWaitsForResponseAndFinalizer) {
  CheckGrpcSynchronousConsume<false>();
}

UTEST(GrpcDataSink, ServerStreamingConsumeWaitsForResponseAndFinalizer) {
  CheckGrpcSynchronousConsume<true>();
}

struct FinishBeforeConsumeReturnHandler final {
  using State = int;
  userver::engine::SingleUseEvent* finishEntered;
  userver::engine::SingleUseEvent* ended;

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& sender, auto result) {
    sender.send(value);
    result.done();
    // Go starts CloseAndRecv on Done, while ConsumeMessage may still be active.
    EXPECT_EQ(finishEntered->WaitUntil(userver::engine::Deadline::FromDuration(
                  std::chrono::seconds{1})),
              userver::engine::FutureStatus::kReady);
  }
  void handleResponse(servicelib::MessageContext, auto&, State&,
                      const std::string& response) {
    EXPECT_EQ(response, "response");
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_EQ(error, nullptr);
    ended->Send();
  }
};

UTEST(GrpcDataSink, ClientStreamingDoneStartsFinishBeforeConsumeReturns) {
  TestEnvironment environment;
  userver::engine::SingleUseEvent finishEntered;
  userver::engine::SingleUseEvent ended;
  struct Rpc final {
    userver::engine::SingleUseEvent* finishEntered;
    void WriteAndCheck(const std::string&) {}
    std::string Finish() {
      finishEntered->Send();
      return "response";
    }
  };
  auto client = [&](userver::ugrpc::client::CallOptions) {
    return Rpc{&finishEntered};
  };
  TestSinkEndpointStream<std::string, std::string> stream{environment, 3};
  servicelib::datasink::grpc::ClientStreamingEndpoint<
      std::string, std::string, std::string, std::string,
      FinishBeforeConsumeReturnHandler, decltype(client)>
      endpoint{stream,
               FinishBeforeConsumeReturnHandler{&finishEntered, &ended}, client};
  endpoint.start(servicelib::Context{});
  endpoint.consume(servicelib::MessageContext{}.withStreamId("finish-order"),
                   servicelib::Payload<std::string>::make("payload"));
  EXPECT_EQ(ended.WaitUntil(userver::engine::Deadline::FromDuration(
                std::chrono::seconds{2})),
            userver::engine::FutureStatus::kReady);
  endpoint.stop(servicelib::Context{});
}

struct FinalizingSessionProbe final {
  std::atomic<int> opened{0};
  std::atomic<int> finalized{0};
  GrpcConsumeGate firstFinalizer;
};

struct FinalizingSessionHandler final {
  using State = int;
  FinalizingSessionProbe* probe;

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& sender, auto result) {
    sender.send(value);
    result.done();
  }
  void handleResponse(servicelib::MessageContext, auto&, State&,
                      const std::string&) {}
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr,
                  State&) noexcept {
    if (probe->finalized.fetch_add(1) == 0) probe->firstFinalizer.wait();
  }
};

template <bool Bidirectional>
void CheckStreamingReservationDuringFinalizer() {
  TestEnvironment environment;
  FinalizingSessionProbe probe;
  struct RpcState final {
    userver::engine::SingleUseEvent done;
  };
  struct Rpc final {
    std::shared_ptr<RpcState> state;
    void WriteAndCheck(const std::string&) {}
    bool WritesDone() {
      state->done.Send();
      return true;
    }
    std::string Finish() { return "response"; }
    bool Read(std::string&) {
      state->done.Wait();
      return false;
    }
  };
  auto client = [&](userver::ugrpc::client::CallOptions) {
    probe.opened.fetch_add(1);
    return Rpc{std::make_shared<RpcState>()};
  };
  TestSinkEndpointStream<std::string, std::string> stream{
      environment, Bidirectional ? 4 : 3};
  using Endpoint = std::conditional_t<
      Bidirectional,
      servicelib::datasink::grpc::BidirectionalStreamingEndpoint<
          std::string, std::string, std::string, std::string,
          FinalizingSessionHandler, decltype(client)>,
      servicelib::datasink::grpc::ClientStreamingEndpoint<
          std::string, std::string, std::string, std::string,
          FinalizingSessionHandler, decltype(client)>>;
  Endpoint endpoint{stream, FinalizingSessionHandler{&probe}, client};
  endpoint.start(servicelib::Context{});
  const auto context =
      servicelib::MessageContext{}.withStreamId("held-finalizer");
  endpoint.consume(context, servicelib::Payload<std::string>::make("first"));
  EXPECT_EQ(probe.firstFinalizer.entered.WaitUntil(
                userver::engine::Deadline::FromDuration(std::chrono::seconds{2})),
            userver::engine::FutureStatus::kReady);
  endpoint.consume(context, servicelib::Payload<std::string>::make("second"));
  EXPECT_EQ(probe.opened.load(), 1)
      << "An RPC with this ID is still inside EndRequest";
  probe.firstFinalizer.release.Send();
  endpoint.stop(servicelib::Context{});
}

UTEST(GrpcDataSink, ClientStreamingReservesIdWhileFinalizerIsActive) {
  CheckStreamingReservationDuringFinalizer<false>();
}

UTEST(GrpcDataSink, BidiReservesIdWhileFinalizerIsActive) {
  CheckStreamingReservationDuringFinalizer<true>();
}

UTEST(GrpcConnectors, StopsConsumersOfOneIdInReverseRegistrationOrder) {
  TestEnvironment environment;
  TestSinkEndpointStream<std::string, std::string> stream{environment, 1};
  auto sink = servicelib::datasink::grpc::UserverDataSink::make(stream);
  EndpointLifecycleEvents events;
  using Endpoint =
      StopCallbackEndpoint<servicelib::datasink::grpc::IEndpoint>;
  sink->addEndpoint(std::make_shared<Endpoint>(1, [&] { events.add("first"); }));
  sink->addEndpoint(std::make_shared<Endpoint>(1, [&] { events.add("second"); }));
  sink->start(servicelib::Context{});
  sink->stop(servicelib::Context{});
  EXPECT_EQ(events.snapshot(), (std::vector<std::string>{"second", "first"}));
}

UTEST(GrpcConnectors, StopsDifferentIdsConcurrentlyAndWaitsForBoth) {
  TestEnvironment environment;
  TestSinkEndpointStream<std::string, std::string> stream{environment, 1};
  auto sink = servicelib::datasink::grpc::UserverDataSink::make(stream);
  GrpcConsumeGate first;
  GrpcConsumeGate second;
  using Endpoint =
      StopCallbackEndpoint<servicelib::datasink::grpc::IEndpoint>;
  const auto stop = [](GrpcConsumeGate& gate) {
    gate.entered.Send();
    EXPECT_EQ(gate.release.WaitUntil(userver::engine::Deadline::FromDuration(
                  std::chrono::seconds{5})),
              userver::engine::FutureStatus::kReady);
  };
  sink->addEndpoint(std::make_shared<Endpoint>(1, [&] { stop(first); }));
  sink->addEndpoint(std::make_shared<Endpoint>(2, [&] { stop(second); }));
  sink->start(servicelib::Context{});
  std::atomic<bool> returned{false};
  auto task = userver::utils::Async("connector-stop-contract", [&] {
    sink->stop(servicelib::Context{});
    returned.store(true);
  });
  EXPECT_EQ(first.entered.WaitUntil(userver::engine::Deadline::FromDuration(
                std::chrono::seconds{1})),
            userver::engine::FutureStatus::kReady);
  EXPECT_EQ(second.entered.WaitUntil(userver::engine::Deadline::FromDuration(
                std::chrono::seconds{1})),
            userver::engine::FutureStatus::kReady);
  EXPECT_FALSE(returned.load());
  first.release.Send();
  second.release.Send();
  task.Get();
  EXPECT_TRUE(returned.load());
}

}  // namespace

#include "grpc_source_lifetime_review_test.hpp"
#include "grpc_finalization_matrix_test.hpp"
#include "grpc_source_completion_matrix_test.hpp"
#include "grpc_source_cancellation_matrix_test.hpp"
#include "grpc_source_callback_drain_test.hpp"
#include "grpc_failed_open_reentry_test.hpp"
#include "grpc_sink_context_cancel_test.hpp"

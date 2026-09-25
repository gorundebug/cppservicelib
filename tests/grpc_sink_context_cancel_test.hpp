#pragma once

namespace {

struct SinkContextCancelProbe final {
  userver::engine::SingleUseEvent ended;
  int finalizers{};
  int errors{};
  int responses{};
};

struct SinkContextCancelHandler final {
  using State = int;
  SinkContextCancelProbe* probe;
  bool finishNow{};
  servicelib::BeginResult<State> beginRequest(servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& sender, auto result) {
    sender.send(value);
    if (finishNow) result.done();
  }
  void handleResponse(servicelib::MessageContext, auto&, State&, const std::string&) {
    ++probe->responses;
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error, State&) {
    ++probe->finalizers;
    if (error) ++probe->errors;
    probe->ended.Send();
  }
};

template <bool Bidirectional>
void CheckSinkContextCancellation() {
  for (const bool finishNow : {false, true}) {
    for (const int cancellation : {0, 1, 2}) {
      SCOPED_TRACE(finishNow);
      SCOPED_TRACE(cancellation);
      TestEnvironment environment;
      SinkContextCancelProbe probe;
      struct Transport final {
        userver::engine::SingleUseEvent waiting;
        userver::engine::Deadline deadline;
        explicit Transport(userver::engine::Deadline value) : deadline(value) {}
      };
      struct Rpc final {
        std::shared_ptr<Transport> transport;
        void WriteAndCheck(const std::string&) {}
        bool WritesDone() { return true; }
        void wait() {
          // Like the actual userver transport, this operation honors the
          // native task's cancellation and CallOptions deadline.
          static_cast<void>(transport->waiting.WaitUntil(transport->deadline));
          throw std::runtime_error("transport cancelled or deadline expired");
        }
        std::string Finish() { wait(); return {}; }
        bool Read(std::string&) { wait(); return false; }
      };
      std::weak_ptr<Transport> transport;
      auto client = [&](userver::ugrpc::client::CallOptions options) {
        auto state = std::make_shared<Transport>(options.GetDeadline());
        transport = state;
        return Rpc{std::move(state)};
      };
      TestSinkEndpointStream<std::string, std::string> stream{
          environment, Bidirectional ? 4 : 3};
      using Endpoint = std::conditional_t<
          Bidirectional,
          servicelib::datasink::grpc::BidirectionalStreamingEndpoint<
              std::string, std::string, std::string, std::string,
              SinkContextCancelHandler, decltype(client)>,
          servicelib::datasink::grpc::ClientStreamingEndpoint<
              std::string, std::string, std::string, std::string,
              SinkContextCancelHandler, decltype(client)>>;
      Endpoint endpoint{stream, SinkContextCancelHandler{&probe, finishNow}, client};
      endpoint.start(servicelib::Context{});
      std::stop_source stop;
      auto context = servicelib::MessageContext{}.withStreamId("cancel-outgoing");
      if (cancellation == 0) context = context.withStopToken(stop.get_token());
      if (cancellation == 1) context = context.withExternalCancellation(stop.get_token());
      if (cancellation == 2) {
        context = context.withDeadline(std::chrono::steady_clock::now() +
                                       std::chrono::milliseconds{100});
      }
      endpoint.consume(context, servicelib::Payload<std::string>::make("value"));
      if (cancellation != 2) stop.request_stop();
      EXPECT_EQ(probe.ended.WaitUntil(userver::engine::Deadline::FromDuration(
                    std::chrono::milliseconds{500})),
                userver::engine::FutureStatus::kReady);
      // Also clean up a broken implementation without hanging the whole suite.
      endpoint.stop(servicelib::Context{});
      EXPECT_EQ(probe.finalizers, 1);
      EXPECT_EQ(probe.errors, 1);
      EXPECT_EQ(probe.responses, 0);
      EXPECT_TRUE(transport.expired());
    }
  }
}

UTEST_MT(GrpcSinkContextCancellation, ClientStreaming, 1) {
  CheckSinkContextCancellation<false>();
}
UTEST_MT(GrpcSinkContextCancellation, BidirectionalStreaming, 1) {
  CheckSinkContextCancellation<true>();
}

}  // namespace

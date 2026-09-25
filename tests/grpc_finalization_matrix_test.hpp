#pragma once

#include <functional>

namespace {

enum class FinalizationFailure { None, Transport, Response, Finalizer };

struct FinalizationMatrixState final {
  userver::engine::SingleUseEvent done;
  bool read{};
};

struct FinalizationMatrixProbe final {
  FinalizationFailure failure{};
  int opened{};
  int finalized{};
  int responses{};
  int errors{};
  GrpcConsumeGate firstFinalizer;
  std::function<void()> reenter;
  std::weak_ptr<FinalizationMatrixState> transport;
};

struct FinalizationMatrixHandler final {
  using State = int;
  FinalizationMatrixProbe* probe;

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
                      const std::string&) {
    ++probe->responses;
    if (probe->opened == 1 &&
        probe->failure == FinalizationFailure::Response) {
      throw std::runtime_error("response failure");
    }
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) {
    if (error) ++probe->errors;
    if (++probe->finalized == 1) {
      // Reentry must be rejected, not wait for this same callback to finish.
      probe->reenter();
      probe->firstFinalizer.wait();
      if (probe->failure == FinalizationFailure::Finalizer) {
        throw std::runtime_error("finalizer failure");
      }
    }
  }
};

template <bool Bidirectional>
void CheckFinalizationFailureMatrix() {
  for (const auto failure : {FinalizationFailure::None,
                             FinalizationFailure::Transport,
                             FinalizationFailure::Response,
                             FinalizationFailure::Finalizer}) {
    SCOPED_TRACE(static_cast<int>(failure));
    TestEnvironment environment;
    FinalizationMatrixProbe probe;
    probe.failure = failure;
    struct Rpc final {
      std::shared_ptr<FinalizationMatrixState> state;
      bool fail{};
      void WriteAndCheck(const std::string&) {}
      bool WritesDone() {
        state->done.Send();
        return true;
      }
      std::string Finish() {
        if (fail) throw std::runtime_error("transport failure");
        return "response";
      }
      bool Read(std::string& response) {
        state->done.Wait();
        if (fail) throw std::runtime_error("transport failure");
        if (state->read) return false;
        state->read = true;
        response = "response";
        return true;
      }
    };
    auto client = [&](userver::ugrpc::client::CallOptions) {
      auto state = std::make_shared<FinalizationMatrixState>();
      probe.transport = state;
      return Rpc{std::move(state), ++probe.opened == 1 &&
                                      failure == FinalizationFailure::Transport};
    };
    TestSinkEndpointStream<std::string, std::string> stream{
        environment, Bidirectional ? 4 : 3};
    using Endpoint = std::conditional_t<
        Bidirectional,
        servicelib::datasink::grpc::BidirectionalStreamingEndpoint<
            std::string, std::string, std::string, std::string,
            FinalizationMatrixHandler, decltype(client)>,
        servicelib::datasink::grpc::ClientStreamingEndpoint<
            std::string, std::string, std::string, std::string,
            FinalizationMatrixHandler, decltype(client)>>;
    Endpoint endpoint{stream, FinalizationMatrixHandler{&probe}, client};
    endpoint.start(servicelib::Context{});
    const auto context =
        servicelib::MessageContext{}.withStreamId("finalization-matrix");
    probe.reenter = [&] {
      endpoint.consume(context, servicelib::Payload<std::string>::make("reentry"));
    };
    endpoint.consume(context, servicelib::Payload<std::string>::make("first"));
    EXPECT_EQ(probe.firstFinalizer.entered.WaitUntil(
                  userver::engine::Deadline::FromDuration(std::chrono::seconds{2})),
              userver::engine::FutureStatus::kReady);
    EXPECT_EQ(probe.opened, 1);
    EXPECT_EQ(probe.finalized, 1);
    endpoint.consume(context, servicelib::Payload<std::string>::make("late"));
    EXPECT_EQ(probe.opened, 1);
    probe.firstFinalizer.release.Send();

    // EndRequest returning is not enough: wait for the old RPC owner to go.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (!probe.transport.expired() &&
           std::chrono::steady_clock::now() < deadline) {
      userver::engine::SleepFor(std::chrono::milliseconds{1});
    }
    EXPECT_TRUE(probe.transport.expired());
    endpoint.consume(context, servicelib::Payload<std::string>::make("reuse"));
    const auto reusedDeadline =
        std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (!probe.transport.expired() &&
           std::chrono::steady_clock::now() < reusedDeadline) {
      userver::engine::SleepFor(std::chrono::milliseconds{1});
    }
    EXPECT_TRUE(probe.transport.expired());
    endpoint.stop(servicelib::Context{});
    EXPECT_EQ(probe.opened, 2);
    EXPECT_EQ(probe.finalized, 2);
    EXPECT_EQ(probe.errors, failure == FinalizationFailure::Transport ||
                                   failure == FinalizationFailure::Response ? 1 : 0);
    EXPECT_EQ(probe.responses, failure == FinalizationFailure::Transport ? 1 : 2);
    EXPECT_TRUE(probe.transport.expired());
  }
}

UTEST_MT(GrpcFinalizationMatrix, ClientStreamingFailuresReentryAndReuse, 1) {
  CheckFinalizationFailureMatrix<false>();
}

UTEST_MT(GrpcFinalizationMatrix, BidiFailuresReentryAndReuse, 1) {
  CheckFinalizationFailureMatrix<true>();
}

}  // namespace

#pragma once

namespace {

template <bool Bidirectional>
void CheckFailedOpenReentry() {
  for (const bool failFinalizer : {false, true}) {
    SCOPED_TRACE(failFinalizer);
    TestEnvironment environment;
    FinalizationMatrixProbe probe;
    probe.failure = failFinalizer ? FinalizationFailure::Finalizer
                                 : FinalizationFailure::None;
    auto client = [&](userver::ugrpc::client::CallOptions) {
      if (++probe.opened == 1) throw std::runtime_error("RPC open failed");
      if constexpr (Bidirectional) {
        return FakeBidiStream{std::make_shared<FakeBidiState>()};
      } else {
        return FakeClientStream{std::make_shared<FakeClientStreamState>()};
      }
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
    const auto context = servicelib::MessageContext{}.withStreamId("failed-open");
    probe.reenter = [&] {
      endpoint.consume(context, servicelib::Payload<std::string>::make("reentry"));
    };
    auto first = userver::utils::Async("failed-open", [&] {
      endpoint.consume(context, servicelib::Payload<std::string>::make("first"));
    });
    EXPECT_EQ(probe.firstFinalizer.entered.WaitUntil(
                  userver::engine::Deadline::FromDuration(std::chrono::seconds{2})),
              userver::engine::FutureStatus::kReady);
    EXPECT_EQ(probe.opened, 1);
    probe.firstFinalizer.release.Send();
    first.Get();
    EXPECT_EQ(probe.finalized, 1);
    EXPECT_EQ(probe.errors, 1);
    endpoint.consume(context, servicelib::Payload<std::string>::make("reuse"));
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (probe.finalized != 2 && std::chrono::steady_clock::now() < deadline) {
      userver::engine::SleepFor(std::chrono::milliseconds{1});
    }
    EXPECT_EQ(probe.opened, 2);
    EXPECT_EQ(probe.finalized, 2);
    EXPECT_EQ(probe.errors, 1);
    EXPECT_EQ(probe.responses, 1);
    endpoint.stop(servicelib::Context{});
  }
}

UTEST_MT(GrpcFailedOpenReentry, ClientStreaming, 1) { CheckFailedOpenReentry<false>(); }
UTEST_MT(GrpcFailedOpenReentry, BidirectionalStreaming, 1) { CheckFailedOpenReentry<true>(); }

}  // namespace

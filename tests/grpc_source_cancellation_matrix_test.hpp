#pragma once

namespace {

template <int Mode>
void CheckSourceCancellationMatrix() {
  using Unary = servicelib::datasource::grpc::NoStreamingEndpoint<
      std::string, std::string, std::string, std::string, SourceCompletionHandler>;
  using Server = servicelib::datasource::grpc::ServerStreamingEndpoint<
      std::string, std::string, std::string, std::string, SourceCompletionHandler>;
  using Client = servicelib::datasource::grpc::ClientStreamingEndpoint<
      std::string, std::string, std::string, std::string, SourceCompletionHandler>;
  using Bidi = servicelib::datasource::grpc::BidirectionalStreamingEndpoint<
      std::string, std::string, std::string, std::string, SourceCompletionHandler>;
  using Endpoint = std::conditional_t<Mode == 1, Unary,
      std::conditional_t<Mode == 2, Server,
      std::conditional_t<Mode == 3, Client, Bidi>>>;
  // Primary/external tokens, deadline, and cancellation of the native RPC task.
  for (const int cancellation : {0, 1, 2, 3}) {
    SCOPED_TRACE(cancellation);
    TestEnvironment environment;
    SourceCompletionProbe probe;
    userver::engine::SingleUseEvent collected;
    userver::engine::SingleUseEvent finished;
    std::stop_source stop;
    Endpoint endpoint{environment, Mode, SourceCompletionHandler{&probe},
        [&](servicelib::MessageContext context, servicelib::Payload<std::string>) {
          probe.context = std::move(context);
          collected.Send();
        }, true};
    endpoint.start(servicelib::Context{});
    auto request = userver::utils::Async("source-cancellation-matrix", [&] {
      auto context = servicelib::MessageContext{}.withStreamId("cancelled-request");
      if (cancellation == 0) {
        context = context.withStopToken(stop.get_token());
      } else if (cancellation == 1) {
        context = context.withExternalCancellation(stop.get_token());
      } else if (cancellation == 2) {
        context = context.withDeadline(std::chrono::steady_clock::now() +
                                       std::chrono::milliseconds{250});
      }
      try {
        if constexpr (Mode == 1) {
          static_cast<void>(endpoint.handle(context, "request"));
        } else if constexpr (Mode == 2) {
          FakeWriter writer;
          endpoint.handle(context, "request", writer);
        } else if constexpr (Mode == 3) {
          FakeReader reader{{"request"}};
          static_cast<void>(endpoint.handle(context, reader));
        } else {
          FakeReaderWriter stream;
          stream.FakeReader::values = {"request"};
          endpoint.handle(context, stream);
        }
      } catch (const std::exception&) {
        // Transport error propagation is separate from callback ownership.
      }
      finished.Send();
    });
    EXPECT_EQ(collected.WaitUntil(userver::engine::Deadline::FromDuration(
                  std::chrono::seconds{2})), userver::engine::FutureStatus::kReady);
    if (cancellation < 2) stop.request_stop();
    if (cancellation == 3) request.RequestCancel();
    const auto completion = finished.WaitUntil(userver::engine::Deadline::FromDuration(
        std::chrono::seconds{2}));
    EXPECT_EQ(completion, userver::engine::FutureStatus::kReady);
    if (completion != userver::engine::FutureStatus::kReady) request.RequestCancel();
    try {
      request.Get();
    } catch (const userver::engine::TaskCancelledException&) {
      // Cleanup only after recording an unmet cancellation deadline above.
    }
    EXPECT_TRUE(probe.context.cancelled());
    EXPECT_EQ(probe.ended, 1);
    EXPECT_EQ(probe.callbacks, 0);
    EXPECT_TRUE(probe.capture.expired());
    ASSERT_TRUE(static_cast<bool>(probe.registerLate));
    probe.registerLate();
    EXPECT_TRUE(probe.lateCapture.expired());
    endpoint.consumeResult(probe.context,
                           servicelib::Payload<std::string>::make("delayed-result"));
    EXPECT_EQ(probe.callbacks, 0);
    probe.registerLate = {};
    endpoint.stop(servicelib::Context{});
    EXPECT_EQ(probe.ended, 1);
  }
}

UTEST_MT(GrpcSourceCancellationMatrix, Unary, 1) { CheckSourceCancellationMatrix<1>(); }
UTEST_MT(GrpcSourceCancellationMatrix, ServerStreaming, 1) { CheckSourceCancellationMatrix<2>(); }
UTEST_MT(GrpcSourceCancellationMatrix, ClientStreaming, 1) { CheckSourceCancellationMatrix<3>(); }
UTEST_MT(GrpcSourceCancellationMatrix, BidirectionalStreaming, 1) { CheckSourceCancellationMatrix<4>(); }

}  // namespace

#pragma once

namespace {

template <int Mode>
void CheckCancelledSourceDrainsAdmittedCallback() {
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
  TestEnvironment environment;
  SourceCompletionProbe probe;
  GrpcConsumeGate callback;
  userver::engine::SingleUseEvent finished;
  userver::engine::TaskWithResult<void> resultTask;
  std::stop_source stop;
  probe.beforeSend = [&] { callback.wait(); };
  Endpoint* pointer{};
  Endpoint endpoint{environment, Mode, SourceCompletionHandler{&probe},
      [&](servicelib::MessageContext context, servicelib::Payload<std::string> value) {
        probe.context = context;
        resultTask = userver::utils::Async("admitted-source-result",
            [&, context = std::move(context), value = std::move(value)]() mutable {
              pointer->consumeResult(std::move(context), std::move(value));
            });
      }, true};
  pointer = &endpoint;
  endpoint.start(servicelib::Context{});
  auto request = userver::utils::Async("cancel-with-active-result", [&] {
    const auto context = servicelib::MessageContext{}
        .withStreamId("active-result").withStopToken(stop.get_token());
    if constexpr (Mode == 1) {
      EXPECT_EQ(endpoint.handle(context, "request"), "reply:request");
    } else if constexpr (Mode == 2) {
      FakeWriter writer;
      endpoint.handle(context, "request", writer);
      EXPECT_EQ(writer.values, (std::vector<std::string>{"reply:request"}));
    } else if constexpr (Mode == 3) {
      FakeReader reader{{"request"}};
      EXPECT_EQ(endpoint.handle(context, reader), "reply:request");
    } else {
      FakeReaderWriter stream;
      stream.FakeReader::values = {"request"};
      endpoint.handle(context, stream);
      EXPECT_EQ(stream.FakeWriter::values,
                (std::vector<std::string>{"reply:request"}));
    }
    finished.Send();
  });
  EXPECT_EQ(callback.entered.WaitUntil(userver::engine::Deadline::FromDuration(
                std::chrono::seconds{2})), userver::engine::FutureStatus::kReady);
  stop.request_stop();
  EXPECT_EQ(finished.WaitUntil(userver::engine::Deadline::FromDuration(
                std::chrono::milliseconds{10})), userver::engine::FutureStatus::kTimeout);
  EXPECT_EQ(probe.ended, 0);
  EXPECT_FALSE(probe.capture.expired());
  callback.release.Send();
  EXPECT_NO_THROW(resultTask.Get());
  EXPECT_NO_THROW(request.Get());
  EXPECT_EQ(probe.ended, 1);
  EXPECT_FALSE(probe.endHadError);
  EXPECT_TRUE(probe.capture.expired());
  EXPECT_EQ(probe.callbacks, 1);
  EXPECT_TRUE(probe.context.cancelled());
  probe.registerLate = {};
  endpoint.stop(servicelib::Context{});
}

UTEST_MT(GrpcSourceCallbackDrain, Unary, 1) { CheckCancelledSourceDrainsAdmittedCallback<1>(); }
UTEST_MT(GrpcSourceCallbackDrain, ServerStreaming, 1) { CheckCancelledSourceDrainsAdmittedCallback<2>(); }
UTEST_MT(GrpcSourceCallbackDrain, ClientStreaming, 1) { CheckCancelledSourceDrainsAdmittedCallback<3>(); }
UTEST_MT(GrpcSourceCallbackDrain, BidirectionalStreaming, 1) { CheckCancelledSourceDrainsAdmittedCallback<4>(); }

}  // namespace

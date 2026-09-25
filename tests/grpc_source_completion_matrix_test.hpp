#pragma once

#include <functional>

namespace {

enum class SourceCompletionFailure { None, Consume, Finalizer };

struct SourceCompletionProbe final {
  SourceCompletionFailure failure{};
  int callbacks{};
  int ended{};
  bool endHadError{};
  servicelib::MessageContext context;
  std::weak_ptr<int> capture;
  std::weak_ptr<int> lateCapture;
  std::function<void()> registerLate;
  std::function<void()> beforeSend;
};

struct SourceCompletionHandler final {
  using State = int;
  SourceCompletionProbe* probe;

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext context, auto& stream, State&,
                      const std::string& value, auto result, auto&) {
    auto capture = std::make_shared<int>(1);
    probe->capture = capture;
    result.setResultCallback(
        "result", [result, capture, probe = probe](
                      servicelib::MessageContext, auto&, State&,
                      const std::string& output, auto& sender) mutable {
          static_cast<void>(capture);
          ++probe->callbacks;
          if (probe->beforeSend) probe->beforeSend();
          sender.send("reply:" + output);
          result.done();
          // Deliberately retain until request cleanup, not callback return.
          return false;
        });
    probe->registerLate = [result, probe = probe]() mutable {
      auto late = std::make_shared<int>(2);
      probe->lateCapture = late;
      result.setResultCallback(
          "result", [result, late, probe](servicelib::MessageContext,
                                        auto&, State&, const std::string&,
                                        auto&) mutable {
            static_cast<void>(late);
            ++probe->callbacks;
            result.done();
            return false;
          });
    };
    stream.collect(std::move(context), value);
    if (probe->failure == SourceCompletionFailure::Consume) {
      throw std::runtime_error("source consume failure");
    }
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const std::string&) {
    return "result";
  }
  void eof(servicelib::MessageContext, auto&, State&) {}
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) {
    ++probe->ended;
    probe->endHadError = static_cast<bool>(error);
    if (probe->failure == SourceCompletionFailure::Finalizer) {
      throw std::runtime_error("source finalizer failure");
    }
  }
};

template <int Mode>
void CheckSourceCompletionMatrix() {
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
  for (const auto failure : {SourceCompletionFailure::None,
                             SourceCompletionFailure::Consume,
                             SourceCompletionFailure::Finalizer}) {
    SCOPED_TRACE(static_cast<int>(failure));
    TestEnvironment environment;
    SourceCompletionProbe probe;
    probe.failure = failure;
    Endpoint* pointer{};
    Endpoint endpoint{environment, Mode, SourceCompletionHandler{&probe},
        [&](servicelib::MessageContext context, servicelib::Payload<std::string> value) {
          probe.context = context;
          if (failure != SourceCompletionFailure::Consume) {
            pointer->consumeResult(std::move(context), std::move(value));
          }
        }, true};
    pointer = &endpoint;
    endpoint.start(servicelib::Context{});
    const auto context = servicelib::MessageContext{}.withStreamId("source-completion");
    bool threw = false;
    try {
      if constexpr (Mode == 1) {
        const auto response = endpoint.handle(context, "request");
        if (failure == SourceCompletionFailure::None) {
          EXPECT_EQ(response, "reply:request");
        }
      } else if constexpr (Mode == 2) {
        FakeWriter writer;
        endpoint.handle(context, "request", writer);
        if (failure == SourceCompletionFailure::None) {
          EXPECT_EQ(writer.values, (std::vector<std::string>{"reply:request"}));
        }
      } else if constexpr (Mode == 3) {
        FakeReader reader{{"request"}};
        const auto response = endpoint.handle(context, reader);
        if (failure == SourceCompletionFailure::None) {
          EXPECT_EQ(response, "reply:request");
        }
      } else {
        FakeReaderWriter stream;
        stream.FakeReader::values = {"request"};
        endpoint.handle(context, stream);
        if (failure == SourceCompletionFailure::None) {
          EXPECT_EQ(stream.FakeWriter::values,
                    (std::vector<std::string>{"reply:request"}));
        }
      }
    } catch (const std::exception&) {
      threw = true;
    }
    if (failure == SourceCompletionFailure::None) {
      EXPECT_FALSE(threw);
    }
    EXPECT_EQ(probe.ended, 1);
    EXPECT_EQ(probe.endHadError, failure == SourceCompletionFailure::Consume);
    const int expectedCallbacks = failure == SourceCompletionFailure::Consume ? 0 : 1;
    EXPECT_EQ(probe.callbacks, expectedCallbacks);
    EXPECT_TRUE(probe.capture.expired());
    ASSERT_TRUE(static_cast<bool>(probe.registerLate));
    probe.registerLate();
    EXPECT_TRUE(probe.lateCapture.expired());
    endpoint.consumeResult(probe.context, servicelib::Payload<std::string>::make("late"));
    EXPECT_EQ(probe.callbacks, expectedCallbacks);
    probe.registerLate = {};
    endpoint.stop(servicelib::Context{});
    EXPECT_EQ(probe.ended, 1);
  }
}

UTEST(GrpcSourceCompletionMatrix, Unary) { CheckSourceCompletionMatrix<1>(); }
UTEST(GrpcSourceCompletionMatrix, ServerStreaming) { CheckSourceCompletionMatrix<2>(); }
UTEST(GrpcSourceCompletionMatrix, ClientStreaming) { CheckSourceCompletionMatrix<3>(); }
UTEST(GrpcSourceCompletionMatrix, BidirectionalStreaming) { CheckSourceCompletionMatrix<4>(); }

}  // namespace

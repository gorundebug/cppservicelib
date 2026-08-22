#pragma once

#include <functional>

#include <servicelib/datasink/grpc/common.hpp>

namespace servicelib::datasink::grpc {

template <typename Req, typename Res, typename T, typename R, typename Handler,
          typename ClientFunction, typename E = std::exception_ptr>
class ServerStreamingEndpoint final : public Endpoint<T, R, Handler, E> {
 public:
  ServerStreamingEndpoint(SinkEndpointStream<T, R, E>& stream,
                          Handler handler, ClientFunction client)
      : Endpoint<T, R, Handler, E>(stream,
                                   api::GrpcMethodType::kServerStreaming,
                                   std::move(handler)),
        client_(std::move(client)) {}

  void consume(MessageContext context, Payload<T> payload) {
    auto startedSpan = this->startTrace(context);
    std::optional<servicelib::BeginResult<typename Handler::State>> begin;
    try {
      begin.emplace(this->handler_.beginRequest(context, this->streamContext_));
    } catch (...) {
      const auto error = std::current_exception();
      this->traceError(startedSpan.span(), error, "begin_request.error");
      this->metrics_.beginRequestFailed(tracing::ExceptionMessage(error));
      return;
    }
    tracing::SpanEvent(startedSpan.span(), "begin_request");
    context = std::move(begin->context);
    const auto requestContext = this->newRequestStreamId(context);
    const auto startedAt = this->metrics_.requestStart();
    std::exception_ptr error;
    std::optional<Req> request;
    try {
      Sender<Req> sender{[&](Req value) { request.emplace(std::move(value)); }};
      this->handler_.consumeMessage(context, this->streamContext_, begin->state,
                                    payload.get(), sender, ResultContext{});
      if (!request) {
        throw std::runtime_error("gRPC sink handler sent no request");
      }
      tracing::SpanEvent(startedSpan.span(), "consume_message");
    } catch (...) {
      error = std::current_exception();
      this->traceError(startedSpan.span(), error, "consume_message.error");
    }
    if (!error) {
      try {
        telemetry::userver_adapter::SamplingScope samplingScope{
            this->tracingEnabled(), tracing::SamplingEnabled(requestContext),
            requestContext.trace().traceState};
        auto rpc = std::invoke(client_, std::move(*request),
                               callOptions(requestContext));
        tracing::SpanEvent(startedSpan.span(), "grpc_call");
        error =
            receiveResponses(context, begin->state, rpc, startedSpan.span());
      } catch (...) {
        error = std::current_exception();
        this->traceError(startedSpan.span(), error, "grpc_call.error");
      }
    }
    this->callEnd(context, error, begin->state);
    this->metrics_.requestEnd(startedAt, error);
  }

 private:
  template <typename Rpc>
  std::exception_ptr receiveResponses(const MessageContext& context,
                                      typename Handler::State& state, Rpc& rpc,
                                      tracing::Span* span) {
    Res response;
    std::int64_t messageCount = 0;
    for (;;) {
      bool received = false;
      try {
        received = rpc.Read(response);
      } catch (...) {
        const auto error = std::current_exception();
        this->traceError(span, error, "recv.error");
        return error;
      }
      if (!received) {
        tracing::SpanEvent(
            span, "eof",
            {tracing::Attribute::Int64("messages_received", messageCount)});
        return {};
      }
      try {
        this->handler_.handleResponse(context, this->streamContext_, state,
                                      response);
        ++messageCount;
      } catch (...) {
        const auto error = std::current_exception();
        this->traceError(span, error, "handle_response.error");
        return error;
      }
    }
  }

  ClientFunction client_;
};

}  // namespace servicelib::datasink::grpc

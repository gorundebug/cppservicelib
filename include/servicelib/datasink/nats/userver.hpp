#pragma once
#include <servicelib/runtime/detail/publication_metrics.hpp>
#include <atomic>
#include <optional>
#include <servicelib/runtime/nats/client.hpp>
#include <servicelib/runtime/datasink.hpp>
#include <servicelib/runtime/stream_tracing.hpp>
#include <servicelib/runtime/environment/environment.hpp>
namespace servicelib::datasink::nats {
using DeliveryResult = servicelib::nats::DeliveryResult;
template <typename R>
class SinkMessage final {
 public:
  using DeliveryCallback = std::function<R(const DeliveryResult&)>;
  using ResultFunction = std::function<void(MessageContext,R)>;
  SinkMessage(servicelib::nats::Client& client,MessageContext context,
              servicelib::nats::Headers headers,ResultFunction result, std::shared_ptr<servicelib::detail::PublicationMetrics> publication = {})
      : headers(std::move(headers)),client_(client),context_(std::move(context)),result_(std::move(result)), publication_(std::move(publication)) {}
  std::string value;
  servicelib::nats::Headers headers;
  const std::string& subject() const {return client_.subject();}
  void send(DeliveryCallback callback) {
    auto observed = publication_ ? std::make_shared<std::atomic<bool>>(false) : nullptr;
    try {
    client_.publish(context_,value,headers,[context=context_,result=result_,callback=std::move(callback),publication=publication_,observed](DeliveryResult delivery) mutable {
      if (observed && !observed->exchange(true)) observe(publication, delivery);
      result(std::move(context),callback(delivery));
    });
    } catch (...) {
      if (observed && !observed->exchange(true)) publication_->observe("error");
      throw;
    }
  }
  DeliveryResult sendSync() {
    DeliveryResult delivery;
    try { delivery=client_.publishSync(context_,value,headers); }
    catch (...) { if (publication_) publication_->observe("error"); throw; }
    observe(publication_,delivery);return delivery;
  }
  void out(R value) {result_(context_,std::move(value));}
  void skip(R value) {out(std::move(value));}
 private:
  static void observe(const std::shared_ptr<servicelib::detail::PublicationMetrics>& metrics, const DeliveryResult& result) {
    if (metrics) metrics->observe(result.error ? "error" : result.ack ? "acknowledged" : result.flushed ? "flushed" : "client-accepted");
  }
  servicelib::nats::Client& client_;
  MessageContext context_;
  ResultFunction result_;
  std::shared_ptr<servicelib::detail::PublicationMetrics> publication_;
};

template <typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class Endpoint final {
 public:
  using State = typename Handler::State;
  using StreamContext = SinkStreamContext<T, R, E>;

  Endpoint(SinkEndpointStream<T, R, E>& stream, servicelib::nats::Client& producer,
           Handler handler)
      : environment_(stream.environment()),
        endpointId_(stream.endpointId()),
        subject_(endpointConfig(environment_, stream.endpointId()).subject),
        endpointName_(endpointConfig(environment_, stream.endpointId()).name),
        streamIdentity_(resolveStreamIdentity(environment_, stream.streamConfigId())),
        serviceName_(resolveServiceName(environment_)),
        tracingEnabled_(environment_.getTracing() != nullptr),
        producer_(producer),
        handler_(std::move(handler)),
        streamContext_(stream.resultOutput(), stream.errorOutput()),
        metrics_(environment_.getMetrics(), environment_.getLogger(),
                 connectorConfig(environment_, stream.endpointId()).name,
                 endpointName_),
        publication_(std::make_shared<servicelib::detail::PublicationMetrics>(environment_.getMetrics(),
            connectorConfig(environment_, endpointId_).type == api::DataConnectorType::kJetStream ? "jetstream" : "nats",
            connectorConfig(environment_, endpointId_).name, endpointName_)) {}

  void start(Context context) {
    producer_.start(context, false, [this,context](std::exception_ptr error) {
      handler_.onError(MessageContext{context},streamContext_,error);
    });
    enabled_.store(endpointConfig(environment_,endpointId_).enabled);
  }
  void stop(Context context) {
    enabled_.store(false);
    producer_.stop(context);
  }

  void consume(MessageContext context, Payload<T> payload) {
    if (!enabled_.load(std::memory_order_acquire)) return;
    auto startedSpan = startTrace(context);
    auto streamId = handler_.getStreamId(context, payload.get());
    context = std::move(context).withStreamId(std::move(streamId));
    if (startedSpan.span()) {
      tracing::SpanAttrs(startedSpan.span(),
                         {tracing::Attribute::String(
                             "stream_id", std::string{context.streamId()})});
    }
    std::optional<BeginResult<State>> begin;
    try {
      begin.emplace(handler_.beginRequest(context, streamContext_));
    } catch (...) {
      const auto message = tracing::ExceptionMessage(std::current_exception());
      if (auto* traceSpan = startedSpan.span()) tracing::SpanError(traceSpan, message);
      metrics_.beginRequestFailed(message);
      return;
    }
    if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("begin_request");
    context = std::move(begin->context);
    const auto startedAt = metrics_.requestStart();
    std::exception_ptr error;
    try {
      servicelib::nats::Headers headers;
      servicelib::nats::InjectContext(context,headers,tracingEnabled_);
      SinkMessage<R> message{producer_,context,std::move(headers),
          [this](MessageContext resultContext,R result) {
            streamContext_.collect(std::move(resultContext),std::move(result));
          }, publication_};
      handler_.consumeMessage(context, streamContext_, begin->state,
                              payload.get(), message);
      if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("consume_message");
    } catch (...) {
      error = std::current_exception();
      if (auto* traceSpan = startedSpan.span()) {
        const auto message = tracing::ExceptionMessage(error);
        tracing::SpanError(traceSpan, message);
        traceSpan->addEvent(
            "consume_message.error",
            {tracing::Attribute::String("error", message)});
      }
      streamContext_.collectError(context, error);
    }
    try {
      handler_.endRequest(context, streamContext_, error, begin->state);
    } catch (...) {
      // endRequest is noexcept by contract.
    }
    metrics_.requestEnd(startedAt, error);
  }

 private:
  [[nodiscard]] tracing::ActiveSpan startTrace(MessageContext& context) {
    if (!tracingEnabled_ || !tracing::SamplingEnabled(context)) return {};
    auto* engine = environment_.getTracing();
    if (!engine) return {};
    auto tracer = engine->tracer(serviceName_);
    if (!tracer) return {};
    return tracing::StartSpanInPlace(
        context, tracer.get(), "nats.output",
        {
            tracing::Attribute::String("stream", streamIdentity_.name),
            tracing::Attribute::String("pipeline", streamIdentity_.pipeline),
            tracing::Attribute::String("component", streamIdentity_.component),
            tracing::Attribute::String("endpoint", endpointName_),
        });
  }

  [[nodiscard]] static StreamTraceIdentity resolveStreamIdentity(
      const IServiceEnvironment& environment, std::size_t streamConfigId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    if (!runtime || streamConfigId == 0) return {};
    const auto stream = runtime->GetStreamConfigByID(
        static_cast<int>(streamConfigId));
    return stream ? StreamTraceIdentity{stream->GetName(), stream->GetPipeline(),
                                        stream->GetComponent()}
                  : StreamTraceIdentity{};
  }

  [[nodiscard]] static std::string resolveServiceName(
      const IServiceEnvironment& environment) {
    const auto service = environment.getServiceConfigSnapshot();
    return service ? service->name : std::string{};
  }

  static config::NATSEndpointConfig endpointConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto value =
        runtime ? runtime->GetEndpointConfigByID(endpointId) : std::nullopt;
    const auto* result =
        value ? value->As<config::NATSEndpointConfig>() : nullptr;
    if (!result || result->subject.empty()) {
      throw std::invalid_argument(
          "NATS endpoint config not found or subject empty");
    }
    return *result;
  }

  static config::NATSDataConnectorConfig connectorConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpoint = endpointConfig(environment, endpointId);
    const auto value = runtime->GetDataConnectorByID(endpoint.idDataConnector);
    const auto* result =
        value ? value->As<config::NATSDataConnectorConfig>() : nullptr;
    if (!result)
      throw std::invalid_argument("NATS connector config not found");
    return *result;
  }

  IServiceEnvironment& environment_;
  int endpointId_;
  std::atomic<bool> enabled_{false};
  std::string subject_;
  std::string endpointName_;
  StreamTraceIdentity streamIdentity_;
  std::string serviceName_;
  bool tracingEnabled_{};
  servicelib::nats::Client& producer_;
  Handler handler_;
  StreamContext streamContext_;
  DataSinkEndpointMetrics metrics_;
  std::shared_ptr<servicelib::detail::PublicationMetrics> publication_;
};

}  // namespace servicelib::datasink::nats

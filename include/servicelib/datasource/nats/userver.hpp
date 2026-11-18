#pragma once
#include <servicelib/runtime/nats/client.hpp>
#include <servicelib/datasource/nats/detail/endpoint.hpp>
namespace servicelib::datasource::nats {
using ConsumerMessage = servicelib::nats::ConsumerMessage;
namespace detail {

class ProducerAdapter final {
 public:
  using Consumer = std::function<void(MessageContext, Payload<ConsumerMessage>)>;
  ProducerAdapter(servicelib::nats::Client& client, IServiceEnvironment& environment)
      : client_(client), tracingEnabled_(environment.getTracing() != nullptr) {}

  void start(Context owner, Consumer consumer,
             std::function<void(Context, std::exception_ptr)> onError) {
    client_.start(owner, true, [owner,onError=std::move(onError)](std::exception_ptr error) { onError(owner,error); });
    client_.subscribe([owner,tracingEnabled=tracingEnabled_,consumer=std::move(consumer)](ConsumerMessage message) {
      auto context=servicelib::nats::ContextFromHeaders(MessageContext{owner},message.headers(),tracingEnabled);
      consumer(std::move(context),Payload<ConsumerMessage>::make(std::move(message)));
    });
  }
  void stopAdmission(Context context) { client_.stopAdmission(std::move(context)); }
  bool waitForCallbacks(Context context) { return client_.waitForCallbacks(context); }

  void stop(Context context) { client_.stop(context); }

 private:
  servicelib::nats::Client& client_;
  bool tracingEnabled_{};
};

template <typename Handler, typename T, typename R, typename E>
class HandlerAdapter final {
 public:
  using State = typename Handler::State;
  explicit HandlerAdapter(Handler handler) : handler_(std::move(handler)) {}

  void onError(MessageContext context, SourceStreamContext<T, R, E>& stream,
               std::exception_ptr error) {
    handler_.onError(std::move(context), stream, std::move(error));
  }
  BeginResult<State> beginRequest(MessageContext context,
                                  SourceStreamContext<T, R, E>& stream) {
    return handler_.beginRequest(std::move(context), stream);
  }
  void consumeMessage(
      MessageContext context, SourceStreamContext<T, R, E>& stream,
      State& state, const ConsumerMessage& message,
      ResultContext<State, T, R, E> result) {
    handler_.consumeMessage(std::move(context), stream, state, message,
                            std::move(result));
  }
  std::string getMessageId(MessageContext context,
                           SourceStreamContext<T, R, E>& stream, State& state,
                           const R& value) {
    return handler_.getMessageId(std::move(context), stream, state, value);
  }
  void endRequest(MessageContext context, SourceStreamContext<T, R, E>& stream,
                  std::exception_ptr error, State& state) noexcept {
    handler_.endRequest(std::move(context), stream, error, state);
  }

 private:
  Handler handler_;
};

}  // namespace detail

template <typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class Endpoint final {
 public:
  using Adapter = detail::HandlerAdapter<Handler, T, R, E>;
  using Implementation =
      detail::EndpointState<T, R, Adapter, E, ConsumerMessage,
                            detail::ProducerAdapter>;
  using Output = typename SourceStreamContext<T, R, E>::Output;
  using ErrorOutput = typename SourceStreamContext<T, R, E>::ErrorOutput;

  template <typename InputStreamType>
  static std::shared_ptr<Endpoint> make(
      IServiceEnvironment& environment,
      InputStreamType& input, servicelib::nats::Client& consumer,
      Handler handler) {
    auto endpoint = std::shared_ptr<Endpoint>(new Endpoint(
        environment, input.getEndpointId(),
        static_cast<int>(input.getConfigId()), consumer, std::move(handler),
        [input = &input](MessageContext context, Payload<T> value) {
          input->consume(std::move(context), std::move(value));
        },
        input.getResultStream() != nullptr,
        [input = &input](MessageContext context, Payload<E> error) {
          input->consumeError(std::move(context), std::move(error));
        }));
    if (input.getResultStream() != nullptr) {
      auto* endpointObserver = endpoint.get();
      input.setResultConsumer(
          [endpointObserver](MessageContext context, Payload<R> result) {
            endpointObserver->consumeResult(std::move(context),
                                            std::move(result));
          });
    }
    return endpoint;
  }

 private:
  Endpoint(IServiceEnvironment& environment, int endpointId,
           servicelib::nats::Client& consumer, Handler handler, Output output,
           bool hasResult, ErrorOutput errorOutput = {})
      : Endpoint(environment, endpointId, 0, consumer, std::move(handler),
                 std::move(output), hasResult, std::move(errorOutput)) {}

  Endpoint(IServiceEnvironment& environment, int endpointId,
           int streamConfigId, servicelib::nats::Client& consumer, Handler handler,
           Output output, bool hasResult, ErrorOutput errorOutput = {})
      : producer_(consumer, environment),
        environment_(environment),
        endpointId_(endpointId),
        implementation_(
            environment, endpointId, streamConfigId, producer_,
            Adapter{std::move(handler)}, std::move(output), hasResult,
            connectorConfig(environment, endpointId).name,
            endpointConfig(environment, endpointId).name,
            std::move(errorOutput)) {
    consumer.setSourceMetrics(std::make_shared<servicelib::detail::SourceMetrics>(environment.getMetrics(),
        connectorConfig(environment, endpointId).type == api::DataConnectorType::kJetStream ? "jetstream" : "nats",
        connectorConfig(environment, endpointId).name, endpointConfig(environment, endpointId).name));
  }

 public:
  void start(Context context) {
    const auto endpoint = endpointConfig(environment_, endpointId_);
    if (!endpoint.enabled) return;
    implementation_.start(std::move(context));
    started_ = true;
  }
  void stop(Context context) {
    if (!started_) return;
    started_ = false;
    implementation_.stop(std::move(context));
  }
  void consumeResult(MessageContext context, Payload<R> payload) {
    implementation_.consumeResult(std::move(context), std::move(payload));
  }

 private:
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

  detail::ProducerAdapter producer_;
  IServiceEnvironment& environment_;
  int endpointId_{};
  bool started_{};
  Implementation implementation_;
};

}  // namespace servicelib::datasource::nats

#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include <userver/concurrent/background_task_storage.hpp>
#include <userver/components/component_context.hpp>
#include <userver/components/statistics_storage.hpp>
#include <userver/kafka/impl/broker_secrets.hpp>
#include <userver/kafka/impl/configuration.hpp>
#include <userver/kafka/producer.hpp>
#include <userver/utils/statistics/entry.hpp>
#include <userver/utils/statistics/storage.hpp>
#include <userver/utils/statistics/writer.hpp>

#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/datasink.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/environment/tracing/tracing.hpp>
#include <servicelib/runtime/detail/kafka_admin.hpp>

namespace servicelib::datasink::kafka {

struct DeliveryResult final {
  // userver does not expose the broker-assigned offset or partition in its
  // public producer API. partition is populated only when explicitly chosen.
  std::optional<std::uint32_t> partition;
  std::optional<std::int64_t> offset;
  std::exception_ptr error;
};

class ProducerClient {
 public:
  virtual ~ProducerClient() = default;
  [[nodiscard]] virtual std::optional<std::uint32_t> partitionCount(
      const config::KafkaDataConnectorConfig&,
      const std::string&) const {
    return std::nullopt;
  }
  virtual DeliveryResult send(std::string topic, std::string key,
                              std::string value,
                              std::optional<std::uint32_t> partition) = 0;
};

class UserverProducerClient final : public ProducerClient {
 public:
  explicit UserverProducerClient(const userver::kafka::Producer& producer)
      : producer_(producer) {}

  UserverProducerClient(
      const userver::components::ComponentContext& component_context,
      const config::KafkaDataConnectorConfig& connector_config)
      : owned_producer_(std::make_unique<userver::kafka::Producer>(
            "kafka-producer-" + connector_config.name,
            component_context.GetTaskProcessor("main-task-processor"),
            producerConfiguration(connector_config),
            producerSecret(connector_config))),
        producer_(*owned_producer_),
        metrics_registration_(
            component_context
                .FindComponent<userver::components::StatisticsStorage>()
                .GetStorage()
                .RegisterWriter(
                    "kafka_producer",
                    [producer = owned_producer_.get()](
                        userver::utils::statistics::Writer& writer) {
                      producer->DumpMetric(writer);
                    })) {}

  DeliveryResult send(std::string topic, std::string key, std::string value,
                      std::optional<std::uint32_t> partition) override {
    DeliveryResult result{partition, std::nullopt, {}};
    try {
      producer_.Send(topic, key, value, partition);
    } catch (...) {
      result.error = std::current_exception();
    }
    return result;
  }

  [[nodiscard]] std::optional<std::uint32_t> partitionCount(
      const config::KafkaDataConnectorConfig& connector,
      const std::string& topic) const override {
    return servicelib::detail::KafkaTopicPartitionCount(connector, topic);
  }

 private:
  static userver::kafka::impl::ProducerConfiguration producerConfiguration(
      const config::KafkaDataConnectorConfig& connector_config) {
    userver::kafka::impl::ProducerConfiguration result;
    if (connector_config.dialTimeout > 0.0F) {
      result.delivery_timeout = std::chrono::milliseconds{
          static_cast<std::int64_t>(connector_config.dialTimeout)};
    }
    return result;
  }

  static userver::kafka::impl::Secret producerSecret(
      const config::KafkaDataConnectorConfig& connector_config) {
    userver::kafka::impl::Secret result;
    result.brokers = connector_config.brokers;
    return result;
  }

  std::unique_ptr<userver::kafka::Producer> owned_producer_;
  const userver::kafka::Producer& producer_;
  // Must be destroyed before owned_producer_: the callback captures it.
  userver::utils::statistics::Entry metrics_registration_;
};

template <typename R>
class SinkMessage final {
 public:
  using DeliveryCallback = std::function<R(const DeliveryResult&)>;
  using SendFunction = std::function<DeliveryResult(
      std::string, std::string, std::optional<std::uint32_t>)>;
  using ResultFunction = std::function<void(MessageContext, R)>;
  using PartitionFunction =
      std::function<std::optional<std::uint32_t>()>;

  SinkMessage(std::string topic, MessageContext context,
              ResultFunction resultFunction,
              PartitionFunction partitionFunction, SendFunction sendFunction,
              userver::concurrent::BackgroundTaskStorage& tasks)
      : topic_(std::move(topic)),
        context_(std::move(context)),
        resultFunction_(std::move(resultFunction)),
        partitionFunction_(std::move(partitionFunction)),
        sendFunction_(std::move(sendFunction)),
        tasks_(tasks) {}

  std::string key;
  std::string value;

  [[nodiscard]] const std::string& topic() const noexcept { return topic_; }

  void send(DeliveryCallback callback) {
    auto keyCopy = key;
    auto valueCopy = value;
    auto sendFunction = sendFunction_;
    auto resultFunction = resultFunction_;
    auto partitionFunction = partitionFunction_;
    auto context = context_;
    tasks_.CriticalAsyncDetach(
        "servicelib-kafka-delivery",
        [sendFunction = std::move(sendFunction),
         resultFunction = std::move(resultFunction),
         partitionFunction = std::move(partitionFunction),
         context = std::move(context), key = std::move(keyCopy),
         value = std::move(valueCopy),
         callback = std::move(callback)]() mutable {
          DeliveryResult delivery;
          try {
            delivery = sendFunction(std::move(key), std::move(value),
                                    partitionFunction());
          } catch (...) {
            delivery.error = std::current_exception();
          }
          resultFunction(std::move(context), callback(delivery));
        });
  }

  DeliveryResult sendSync() {
    try {
      return sendFunction_(key, value, partitionFunction_());
    } catch (...) {
      return {std::nullopt, std::nullopt, std::current_exception()};
    }
  }

  void out(R result) { resultFunction_(context_, std::move(result)); }
  void skip(R result) { out(std::move(result)); }

 private:
  std::string topic_;
  MessageContext context_;
  ResultFunction resultFunction_;
  PartitionFunction partitionFunction_;
  SendFunction sendFunction_;
  userver::concurrent::BackgroundTaskStorage& tasks_;
};

template <typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class Endpoint final {
 public:
  using State = typename Handler::State;
  using StreamContext = SinkStreamContext<T, R, E>;

  Endpoint(SinkEndpointStream<T, R, E>& stream, ProducerClient& producer,
           Handler handler)
      : environment_(stream.environment()),
        endpointId_(stream.endpointId()),
        topic_(endpointConfig(environment_, stream.endpointId()).topic),
        partitionCount_(static_cast<std::uint32_t>(std::max(
            endpointConfig(environment_, stream.endpointId()).partitions, 1))),
        endpointName_(endpointConfig(environment_, stream.endpointId()).name),
        streamName_(resolveStreamName(environment_, stream.streamConfigId())),
        serviceName_(resolveServiceName(environment_)),
        producer_(producer),
        handler_(std::move(handler)),
        streamContext_(stream.resultOutput(), stream.errorOutput()),
        metrics_(environment_.getMetrics(), environment_.getLogger(),
                 connectorConfig(environment_, stream.endpointId()).name,
                 endpointName_) {}

  void start(Context) {
    const auto endpoint = endpointConfig(environment_, endpointId_);
    if (!endpoint.enabled) {
      enabled_.store(false, std::memory_order_release);
      return;
    }
    servicelib::detail::EnsureKafkaTopic(
        connectorConfig(environment_, endpointId_), endpoint);
    if constexpr (requires(Handler& h, const T& value,
                           std::uint32_t partitions) {
                    h.partition(value, partitions);
                  }) {
      const auto connector = connectorConfig(environment_, endpointId_);
      if (const auto actual = producer_.partitionCount(connector, topic_)) {
        partitionCount_ = *actual;
      }
    }
    enabled_.store(true, std::memory_order_release);
  }
  void stop(Context) {
    enabled_.store(false, std::memory_order_release);
    tasks_.CancelAndWait();
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
      tracing::SpanError(startedSpan.span(), message);
      metrics_.beginRequestFailed(message);
      return;
    }
    tracing::SpanEvent(startedSpan.span(), "begin_request");
    context = std::move(begin->context);
    const auto startedAt = metrics_.requestStart();
    std::exception_ptr error;
    try {
      SinkMessage<R> message{
          topic_, context,
          [this](MessageContext resultContext, R result) {
            streamContext_.collect(std::move(resultContext), std::move(result));
          },
          [this, partitionPayload = payload]() {
            return handlerPartition(partitionPayload.get());
          },
          [this](std::string key, std::string value,
                 std::optional<std::uint32_t> selected) {
            return producer_.send(topic_, std::move(key),
                                  std::move(value), selected);
          },
          tasks_};
      handler_.consumeMessage(context, streamContext_, begin->state,
                              payload.get(), message);
      tracing::SpanEvent(startedSpan.span(), "consume_message");
    } catch (...) {
      error = std::current_exception();
      const auto message = tracing::ExceptionMessage(error);
      tracing::SpanError(startedSpan.span(), message);
      tracing::SpanEvent(startedSpan.span(), "consume_message.error",
                         {tracing::Attribute::String("error", message)});
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
    if (!tracing::SamplingEnabled(context)) return {};
    auto* engine = environment_.getTracing();
    if (!engine) return {};
    auto tracer = engine->tracer(serviceName_);
    if (!tracer) return {};
    return tracing::StartSpanInPlace(
        context, tracer.get(), "kafka.output",
        {
            tracing::Attribute::String("stream", streamName_),
            tracing::Attribute::String("endpoint", endpointName_),
        });
  }

  [[nodiscard]] static std::string resolveStreamName(
      const IServiceEnvironment& environment, std::size_t streamConfigId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    if (!runtime || streamConfigId == 0) return {};
    const auto stream = runtime->GetStreamConfigByID(
        static_cast<int>(streamConfigId));
    return stream ? stream->GetName() : std::string{};
  }

  [[nodiscard]] static std::string resolveServiceName(
      const IServiceEnvironment& environment) {
    const auto service = environment.getServiceConfigSnapshot();
    return service ? service->name : std::string{};
  }

  std::optional<std::uint32_t> handlerPartition(const T& value) {
    if constexpr (requires(Handler& h) {
                    h.partition(value, partitionCount_);
                  }) {
      return handler_.partition(value, partitionCount_);
    } else {
      return std::nullopt;
    }
  }

  static config::KafkaEndpointConfig endpointConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto value =
        runtime ? runtime->GetEndpointConfigByID(endpointId) : std::nullopt;
    const auto* result =
        value ? value->As<config::KafkaEndpointConfig>() : nullptr;
    if (!result || result->topic.empty()) {
      throw std::invalid_argument(
          "Kafka endpoint config not found or topic empty");
    }
    return *result;
  }

  static config::KafkaDataConnectorConfig connectorConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpoint = endpointConfig(environment, endpointId);
    const auto value = runtime->GetDataConnectorByID(endpoint.idDataConnector);
    const auto* result =
        value ? value->As<config::KafkaDataConnectorConfig>() : nullptr;
    if (!result)
      throw std::invalid_argument("Kafka connector config not found");
    return *result;
  }

  IServiceEnvironment& environment_;
  int endpointId_;
  std::atomic<bool> enabled_{false};
  std::string topic_;
  std::uint32_t partitionCount_;
  std::string endpointName_;
  std::string streamName_;
  std::string serviceName_;
  ProducerClient& producer_;
  Handler handler_;
  StreamContext streamContext_;
  DataSinkEndpointMetrics metrics_;
  // Last: asynchronous delivery callbacks are joined before endpoint fields.
  userver::concurrent::BackgroundTaskStorage tasks_;
};

}  // namespace servicelib::datasink::kafka

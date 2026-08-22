#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <userver/engine/async.hpp>
#include <userver/engine/task/task_with_result.hpp>
#include <userver/kafka/consumer_scope.hpp>

#include <servicelib/datasource/localsource/custom.hpp>
#include <servicelib/runtime/detail/kafka_admin.hpp>

namespace servicelib::datasource::kafka {

// The payload is copied out of userver::kafka::Message because the original
// view is valid only for the ConsumerScope batch callback.
class ConsumerMessage final {
 public:
  ConsumerMessage(std::string key, std::string value, std::string topic,
                  std::uint32_t partition, std::int64_t offset,
                  std::function<void()> commit = {})
      : key_(std::move(key)),
        value_(std::move(value)),
        topic_(std::move(topic)),
        partition_(partition),
        offset_(offset),
        commit_(std::move(commit)) {}

  [[nodiscard]] const std::string& key() const noexcept { return key_; }
  [[nodiscard]] const std::string& value() const noexcept { return value_; }
  [[nodiscard]] const std::string& topic() const noexcept { return topic_; }
  [[nodiscard]] std::uint32_t partition() const noexcept { return partition_; }
  [[nodiscard]] std::int64_t offset() const noexcept { return offset_; }

  // userver exposes assignment-level asynchronous commit. Unlike Sarama,
  // per-message MarkMessage(metadata) is not available in its public API.
  void commit() const {
    if (commit_) commit_();
  }

 private:
  std::string key_;
  std::string value_;
  std::string topic_;
  std::uint32_t partition_{};
  std::int64_t offset_{};
  std::function<void()> commit_;
};

class ConsumerClient {
 public:
  using Callback = std::function<void(ConsumerMessage)>;
  virtual ~ConsumerClient() = default;
  virtual void start(Callback callback) = 0;
  virtual void stop() noexcept = 0;
};

class UserverConsumerClient final : public ConsumerClient {
 public:
  UserverConsumerClient(userver::kafka::ConsumerScope& consumer,
                        std::string topic)
      : consumer_(consumer), topic_(std::move(topic)) {}

  void start(Callback callback) override {
    const auto& topics = consumer_.GetTopics();
    if (std::find(topics.begin(), topics.end(), topic_) == topics.end()) {
      throw std::invalid_argument(
          "userver Kafka consumer is not configured for topic " + topic_);
    }
    consumer_.Start([this, callback = std::move(callback)](
                        userver::kafka::MessageBatchView batch) {
      struct QueuedMessage final {
        std::string key;
        std::string value;
        std::string topic;
        std::uint32_t partition{};
        std::int64_t offset{};
      };
      std::map<std::uint32_t, std::vector<QueuedMessage>> partitions;
      for (const auto& message : batch) {
        const auto partition =
            static_cast<std::uint32_t>(message.GetPartition());
        partitions[partition].push_back(
            {std::string{message.GetKey()},
             std::string{message.GetPayload()}, message.GetTopic(), partition,
             message.GetOffset()});
      }

      std::vector<userver::engine::TaskWithResult<bool>> tasks;
      tasks.reserve(partitions.size());
      for (auto& [partition, messages] : partitions) {
        static_cast<void>(partition);
        tasks.push_back(userver::engine::AsyncNoTracing(
            [callback, messages = std::move(messages)]() mutable {
              bool commit = false;
              for (auto& message : messages) {
                auto commitRequested =
                    std::make_shared<std::atomic<bool>>(false);
                callback(ConsumerMessage{
                    std::move(message.key), std::move(message.value),
                    std::move(message.topic), message.partition,
                    message.offset, [commitRequested] {
                      commitRequested->store(true,
                                             std::memory_order_release);
                    }});
                commit = commit || commitRequested->load(
                                       std::memory_order_acquire);
              }
              return commit;
            }));
      }
      bool commit = false;
      for (auto& task : tasks) {
        commit = task.Get() || commit;
      }
      // ConsumerScope is not thread-safe. Keep the actual transport operation
      // on the original batch callback task after all partition lanes finish.
      if (commit) {
        consumer_.AsyncCommit();
      }
    });
  }

  void stop() noexcept override { consumer_.Stop(); }

 private:
  userver::kafka::ConsumerScope& consumer_;
  std::string topic_;
};

namespace detail {

class ProducerAdapter final
    : public datasource::localsource::DataProducer<ConsumerMessage> {
 public:
  explicit ProducerAdapter(ConsumerClient& client) : client_(client) {}

  void start(Context, Consumer consumer) override {
    client_.start([consumer = std::move(consumer)](ConsumerMessage message) {
      consumer(MessageContext{},
               Payload<ConsumerMessage>::make(std::move(message)));
    });
  }
  void stop(Context) override { client_.stop(); }

 private:
  ConsumerClient& client_;
};

template <typename Handler, typename T, typename R, typename E>
class HandlerAdapter final {
 public:
  using State = typename Handler::State;
  explicit HandlerAdapter(Handler handler) : handler_(std::move(handler)) {}

  int concurrency(SourceStreamContext<T, R, E>& context) {
    return handler_.concurrency(context);
  }
  BeginResult<State> beginRequest(MessageContext context,
                                  SourceStreamContext<T, R, E>& stream) {
    return handler_.beginRequest(std::move(context), stream);
  }
  void consumeMessage(
      MessageContext context, SourceStreamContext<T, R, E>& stream,
      State& state, const ConsumerMessage& message,
      datasource::localsource::ResultContext<State, T, R, E> result) {
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
      datasource::localsource::Endpoint<T, R, Adapter, E, ConsumerMessage>;
  using Output = typename SourceStreamContext<T, R, E>::Output;
  using ErrorOutput = typename SourceStreamContext<T, R, E>::ErrorOutput;

  template <typename InputStreamType>
  static std::shared_ptr<Endpoint> make(
      IServiceEnvironment& environment,
      InputStreamType& input, ConsumerClient& consumer,
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
           ConsumerClient& consumer, Handler handler, Output output,
           bool hasResult, ErrorOutput errorOutput = {})
      : Endpoint(environment, endpointId, 0, consumer, std::move(handler),
                 std::move(output), hasResult, std::move(errorOutput)) {}

  Endpoint(IServiceEnvironment& environment, int endpointId,
           int streamConfigId, ConsumerClient& consumer, Handler handler,
           Output output, bool hasResult, ErrorOutput errorOutput = {})
      : producer_(consumer),
        environment_(environment),
        endpointId_(endpointId),
        implementation_(
            environment, endpointId, streamConfigId, producer_,
            Adapter{std::move(handler)}, std::move(output), hasResult,
            connectorConfig(environment, endpointId).name,
            endpointConfig(environment, endpointId).name,
            // ConsumerScope and AsyncCommit are not thread-safe. As in Go's
            // ConsumeClaim path, finish the per-message lifecycle before the
            // transport callback releases its message/session scope.
            std::move(errorOutput), true, "kafka.input") {}

 public:
  void start(Context context) {
    const auto endpoint = endpointConfig(environment_, endpointId_);
    if (!endpoint.enabled) return;
    servicelib::detail::EnsureKafkaTopic(
        connectorConfig(environment_, endpointId_), endpoint);
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

  detail::ProducerAdapter producer_;
  IServiceEnvironment& environment_;
  int endpointId_{};
  bool started_{};
  Implementation implementation_;
};

}  // namespace servicelib::datasource::kafka

#include <atomic>
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <userver/engine/single_use_event.hpp>
#include <userver/utest/utest.hpp>

#include <servicelib/datasink/kafka/userver.hpp>
#include <servicelib/datasink/localsink/custom.hpp>
#include <servicelib/datasource/kafka/userver.hpp>
#include <servicelib/datasource/localsource/custom.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#if __has_include(<librdkafka/rdkafka_mock.h>)
#include <librdkafka/rdkafka_mock.h>
#else
#include <rdkafka_mock.h>
#endif

#include "test_sink_endpoint_stream.hpp"

namespace {

class MockKafkaCluster final {
 public:
  explicit MockKafkaCluster(int partitions) {
    char error[512]{};
    handle_ = rd_kafka_new(RD_KAFKA_PRODUCER, rd_kafka_conf_new(), error,
                           sizeof(error));
    if (!handle_) throw std::runtime_error(error);
    cluster_ = rd_kafka_mock_cluster_new(handle_, 1);
    if (!cluster_) throw std::runtime_error("failed to create mock cluster");
    const auto status =
        rd_kafka_mock_topic_create(cluster_, "events", partitions, 1);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR) {
      throw std::runtime_error(rd_kafka_err2str(status));
    }
  }

  ~MockKafkaCluster() {
    if (cluster_) rd_kafka_mock_cluster_destroy(cluster_);
    if (handle_) rd_kafka_destroy(handle_);
  }

  [[nodiscard]] std::string brokers() const {
    return rd_kafka_mock_cluster_bootstraps(cluster_);
  }

 private:
  rd_kafka_t* handle_{};
  rd_kafka_mock_cluster_t* cluster_{};
};

class TestConfig final : public servicelib::config::IConfig {
 public:
  TestConfig() {
    customEndpoint.id = 1;
    customEndpoint.name = "custom-messages";
    customEndpoint.idDataConnector = 2;
    customConnector.id = 2;
    customConnector.name = "custom";

    kafkaEndpoint.id = 3;
    kafkaEndpoint.name = "kafka-messages";
    kafkaEndpoint.idDataConnector = 4;
    kafkaEndpoint.enabled = true;
    kafkaEndpoint.topic = "events";
    kafkaEndpoint.partitions = 4;
    kafkaEndpoint.consumerGroup = "tests";
    kafkaConnector.id = 4;
    kafkaConnector.name = "kafka";
    kafkaConnector.brokers = "localhost:9092";
  }

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {customConnector, kafkaConnector};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {customEndpoint, kafkaEndpoint};
  }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override {
    return {};
  }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override {
    return {};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override {
    return {};
  }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override {
    return {};
  }

  servicelib::config::CustomEndpointConfig customEndpoint;
  servicelib::config::CustomDataConnectorConfig customConnector;
  servicelib::config::KafkaEndpointConfig kafkaEndpoint;
  servicelib::config::KafkaDataConnectorConfig kafkaConnector;
};

class TestEnvironment final : public servicelib::IRuntimeEnvironment {
 public:
  TestEnvironment() : runtimeConfig_(config_) {
    service_.name = "endpoint-test";
  }
  servicelib::pool::ITaskPool* getTaskPool(const std::string&) override {
    return nullptr;
  }
  servicelib::pool::IPriorityTaskPool* getPriorityTaskPool(
      const std::string&) override {
    return nullptr;
  }
  std::shared_ptr<const servicelib::config::RuntimeConfig>
  getRuntimeConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::RuntimeConfig>(
        runtimeConfig_);
  }
  std::shared_ptr<const servicelib::config::ServiceConfig>
  getServiceConfigSnapshot() const override {
    serviceConfigReads_.fetch_add(1, std::memory_order_relaxed);
    return std::make_shared<const servicelib::config::ServiceConfig>(service_);
  }
  servicelib::log::Logger& getLogger() override { return log_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }
  [[nodiscard]] std::size_t serviceConfigReads() const noexcept {
    return serviceConfigReads_.load(std::memory_order_relaxed);
  }

 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtimeConfig_;
  servicelib::config::ServiceConfig service_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
  mutable std::atomic<std::size_t> serviceConfigReads_{};
};

class OneValueProducer final
    : public servicelib::datasource::localsource::DataProducer<std::string> {
 public:
  void start(servicelib::Context, Consumer consumer) override {
    consumer(servicelib::MessageContext{},
             servicelib::Payload<std::string>::make("input"));
  }
  void stop(servicelib::Context) override {}
};

struct CustomSourceHandler final {
  using State = int;
  userver::engine::SingleUseEvent* done;
  std::string* observed;
  int concurrency(auto&) { return 1; }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 1};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto result) {
    *observed = value;
    result.done();
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) {
    return "result";
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
    done->Send();
  }
};

UTEST(CustomDataSource, RunsProducerAndHandlerLifecycle) {
  TestEnvironment environment;
  OneValueProducer producer;
  userver::engine::SingleUseEvent done;
  std::string observed;
  using Endpoint =
      servicelib::datasource::localsource::Endpoint<std::string, int,
                                                    CustomSourceHandler>;
  Endpoint endpoint{
      environment,
      1,
      producer,
      CustomSourceHandler{&done, &observed},
      [](servicelib::MessageContext, servicelib::Payload<std::string>) {},
      false};
  endpoint.start(servicelib::Context{});
  done.Wait();
  endpoint.stop(servicelib::Context{});
  EXPECT_EQ(observed, "input");
}

struct CorrelatingSourceHandler final {
  using State = int;
  userver::engine::SingleUseEvent* done;
  int* observedResult;
  int concurrency(auto&) { return 0; }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 5};
  }
  void consumeMessage(servicelib::MessageContext context, auto& stream, State&,
                      const std::string&, auto result) {
    result.setResultCallback(
        "answer", [result, observed = observedResult](
                      servicelib::MessageContext, auto&, State& state,
                      const int& value) mutable {
          *observed = state + value;
          result.done();
          return true;
        });
    stream.collect(std::move(context), std::string{"request"});
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) {
    return "answer";
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
    done->Send();
  }
};

UTEST(CustomDataSource, CorrelatesPipelineResultUsingStreamContext) {
  TestEnvironment environment;
  OneValueProducer producer;
  userver::engine::SingleUseEvent done;
  int observedResult = 0;
  using Endpoint =
      servicelib::datasource::localsource::Endpoint<std::string, int,
                                                    CorrelatingSourceHandler>;
  Endpoint* endpointPtr = nullptr;
  Endpoint endpoint{environment,
                    1,
                    producer,
                    CorrelatingSourceHandler{&done, &observedResult},
                    [&](servicelib::MessageContext context,
                        servicelib::Payload<std::string> value) {
                      EXPECT_EQ(value.get(), "request");
                      endpointPtr->consumeResult(
                          std::move(context),
                          servicelib::Payload<int>::make(37));
                    },
                    true};
  endpointPtr = &endpoint;
  endpoint.start(servicelib::Context{});
  done.Wait();
  endpoint.stop(servicelib::Context{});
  EXPECT_EQ(observedResult, 42);
}

struct CustomSinkHandler final {
  using State = int;
  std::string* observed;
  std::string getStreamId(servicelib::MessageContext, const std::string&) {
    return "sid";
  }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 2};
  }
  void consumeMessage(servicelib::MessageContext context, auto& stream, State&,
                      const std::string& value) {
    *observed = std::string{context.streamId()} + ":" + value;
    stream.collect(context, 42);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
  }
};

UTEST(CustomDataSink, PreservesLifecycleAndCollectsResult) {
  TestEnvironment environment;
  std::string observed;
  int result = 0;
  TestSinkEndpointStream<std::string, int> stream{
      environment, 1,
      [&](servicelib::MessageContext context, servicelib::Payload<int> value) {
        EXPECT_EQ(context.streamId(), "sid");
        result = value.get();
      }};
  servicelib::datasink::localsink::Endpoint<std::string, int, CustomSinkHandler>
      endpoint{stream, CustomSinkHandler{&observed}};
  bool callbackCalled = false;
  endpoint.setSinkCallback([&](servicelib::MessageContext,
                               const std::string& value,
                               std::exception_ptr error) {
    EXPECT_EQ(value, "value");
    EXPECT_FALSE(error);
    callbackCalled = true;
  });
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("value"));
  EXPECT_EQ(observed, "sid:value");
  EXPECT_EQ(result, 42);
  EXPECT_TRUE(callbackCalled);
}

UTEST(CustomDataSink, UnsampledRequestDoesNotResolveTracingConfiguration) {
  TestEnvironment environment;
  std::string observed;
  TestSinkEndpointStream<std::string, int> stream{environment, 1};
  servicelib::datasink::localsink::Endpoint<std::string, int, CustomSinkHandler>
      endpoint{stream, CustomSinkHandler{&observed}};
  const auto before = environment.serviceConfigReads();
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("value"));
  EXPECT_EQ(environment.serviceConfigReads(), before);
  EXPECT_EQ(observed, "sid:value");
}

class FakeKafkaProducer final
    : public servicelib::datasink::kafka::ProducerClient {
 public:
  [[nodiscard]] std::optional<std::uint32_t> partitionCount(
      const servicelib::config::KafkaDataConnectorConfig&,
      const std::string&) const override {
    return actualPartitionCount;
  }
  servicelib::datasink::kafka::DeliveryResult send(
      std::string topic, std::string key, std::string value,
      std::optional<std::uint32_t> partition) override {
    observed = topic + ":" + key + ":" + value;
    return {partition, 17, {}};
  }
  servicelib::datasink::kafka::DeliveryResult sendWithHeaders(
      std::string topic, std::string key, std::string value,
      std::optional<std::uint32_t> partition,
      const servicelib::detail::KafkaHeaders& headers) override {
    observedHeaders = headers;
    return send(std::move(topic), std::move(key), std::move(value), partition);
  }
  std::optional<std::uint32_t> actualPartitionCount;
  std::string observed;
  servicelib::detail::KafkaHeaders observedHeaders;
};

struct KafkaSinkHandler final {
  using State = int;
  std::uint32_t expectedPartitionCount{4};
  std::string getStreamId(servicelib::MessageContext, const std::string&) {
    return "kafka-sid";
  }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  std::optional<std::uint32_t> partition(const std::string&,
                                         std::uint32_t partitionCount) {
    EXPECT_EQ(partitionCount, expectedPartitionCount);
    return partitionCount - 1;
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& message) {
    message.key = "key";
    message.value = value;
    const auto delivery = message.sendSync();
    if (delivery.error) std::rethrow_exception(delivery.error);
    message.out(static_cast<int>(*delivery.offset));
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
  }
};

UTEST(KafkaDataSink, SendsThroughAdapterAndCollectsDeliveryResult) {
  TestEnvironment environment;
  FakeKafkaProducer producer;
  producer.actualPartitionCount = 6;
  int result = 0;
  TestSinkEndpointStream<std::string, int> stream{
      environment, 3,
      [&](servicelib::MessageContext, servicelib::Payload<int> value) {
        result = value.get();
      }};
  servicelib::datasink::kafka::Endpoint<std::string, int, KafkaSinkHandler>
      endpoint{stream, producer, KafkaSinkHandler{6}};
  endpoint.start(servicelib::Context{});
  servicelib::tracing::SpanContext trace{
      "0123456789abcdef0123456789abcdef", "0123456789abcdef", true,
      "vendor=value", "tenant=test"};
  endpoint.consume(servicelib::MessageContext{}
                       .withStreamId("incoming-stream")
                       .withSampling(true)
                       .withTrace(std::move(trace)),
                   servicelib::Payload<std::string>::make("payload"));
  EXPECT_EQ(producer.observed, "events:key:payload");
  EXPECT_EQ(producer.observedHeaders.at("x-stream-id"), "kafka-sid");
  EXPECT_EQ(producer.observedHeaders.at("traceparent"),
            "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01");
  EXPECT_EQ(producer.observedHeaders.at("tracestate"), "vendor=value");
  EXPECT_EQ(producer.observedHeaders.at("baggage"), "tenant=test");
  EXPECT_EQ(producer.observedHeaders.at("x-trace"), "1");
  EXPECT_EQ(result, 17);
  endpoint.stop(servicelib::Context{});
}

UTEST(KafkaContext, RestoresCanonicalPropagationHeaders) {
  const servicelib::detail::KafkaHeaders headers{
      {"x-stream-id", "stream-42"},
      {"traceparent",
       "00-0123456789abcdef0123456789abcdef-0123456789abcdef-00"},
      {"tracestate", "vendor=value"},
      {"baggage", "tenant=test"}};
  const auto context = servicelib::detail::ContextFromKafkaHeaders(headers);
  EXPECT_EQ(context.streamId(), "stream-42");
  EXPECT_FALSE(context.samplingEnabled());
  ASSERT_TRUE(context.trace().isValid());
  EXPECT_EQ(context.trace().traceId, "0123456789abcdef0123456789abcdef");
  EXPECT_EQ(context.trace().spanId, "0123456789abcdef");
  EXPECT_EQ(context.trace().traceState, "vendor=value");
  EXPECT_EQ(context.trace().baggage, "tenant=test");
}

struct SkippingKafkaSinkHandler final {
  using State = int;
  bool* partitionCalled;
  std::string getStreamId(servicelib::MessageContext, const std::string&) {
    return "skip-kafka-sid";
  }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  std::optional<std::uint32_t> partition(const std::string&,
                                         std::uint32_t) {
    *partitionCalled = true;
    return 0;
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string&, auto& message) {
    message.skip(42);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
  }
};

UTEST(KafkaDataSink, DoesNotSelectPartitionForSkippedMessage) {
  TestEnvironment environment;
  FakeKafkaProducer producer;
  bool partitionCalled = false;
  int result = 0;
  TestSinkEndpointStream<std::string, int> stream{
      environment, 3,
      [&](servicelib::MessageContext, servicelib::Payload<int> value) {
        result = value.get();
      }};
  servicelib::datasink::kafka::Endpoint<std::string, int,
                                        SkippingKafkaSinkHandler>
      endpoint{stream, producer, SkippingKafkaSinkHandler{&partitionCalled}};
  endpoint.start(servicelib::Context{});
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("payload"));
  EXPECT_FALSE(partitionCalled);
  EXPECT_EQ(result, 42);
  EXPECT_TRUE(producer.observed.empty());
  endpoint.stop(servicelib::Context{});
}

struct ThrowingPartitionKafkaSinkHandler final {
  using State = int;
  std::string getStreamId(servicelib::MessageContext, const std::string&) {
    return "partition-error-kafka-sid";
  }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  std::optional<std::uint32_t> partition(const std::string&,
                                         std::uint32_t) {
    throw std::runtime_error("partition failed");
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto& message) {
    message.key = "key";
    message.value = value;
    const auto delivery = message.sendSync();
    message.out(delivery.error ? 43 : -1);
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
  }
};

UTEST(KafkaDataSink, PassesPartitionFailureAsDeliveryError) {
  TestEnvironment environment;
  FakeKafkaProducer producer;
  int result = 0;
  TestSinkEndpointStream<std::string, int> stream{
      environment, 3,
      [&](servicelib::MessageContext, servicelib::Payload<int> value) {
        result = value.get();
      }};
  servicelib::datasink::kafka::Endpoint<std::string, int,
                                        ThrowingPartitionKafkaSinkHandler>
      endpoint{stream, producer, ThrowingPartitionKafkaSinkHandler{}};
  endpoint.start(servicelib::Context{});
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("payload"));
  EXPECT_EQ(result, 43);
  EXPECT_TRUE(producer.observed.empty());
  endpoint.stop(servicelib::Context{});
}

class FailingKafkaProducer final
    : public servicelib::datasink::kafka::ProducerClient {
 public:
  servicelib::datasink::kafka::DeliveryResult send(
      std::string, std::string, std::string,
      std::optional<std::uint32_t> partition) override {
    return {partition, std::nullopt,
            std::make_exception_ptr(
                std::runtime_error("Kafka delivery: broker unavailable"))};
  }
};

struct AsyncFailingKafkaSinkHandler final {
  using State = int;
  std::string getStreamId(servicelib::MessageContext, const std::string&) {
    return "async-failed-kafka-sid";
  }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string&, auto& message) {
    message.send([](const auto& delivery) {
      return delivery.error ? 41 : 0;
    });
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
  }
};

UTEST(KafkaDataSink, PassesAsyncDeliveryFailureToCallback) {
  TestEnvironment environment;
  FailingKafkaProducer producer;
  userver::engine::SingleUseEvent delivered;
  int observed = 0;
  TestSinkEndpointStream<std::string, int> stream{
      environment, 3,
      [&](servicelib::MessageContext, servicelib::Payload<int> result) {
        observed = result.get();
        delivered.Send();
      },
      [&](servicelib::MessageContext,
          servicelib::Payload<std::exception_ptr>) { FAIL(); }};
  servicelib::datasink::kafka::Endpoint<
      std::string, int, AsyncFailingKafkaSinkHandler>
      endpoint{stream, producer, AsyncFailingKafkaSinkHandler{}};
  endpoint.start(servicelib::Context{});
  endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("payload"));
  delivered.Wait();
  endpoint.stop(servicelib::Context{});
  EXPECT_EQ(observed, 41);
}

class FakeKafkaConsumer final
    : public servicelib::datasource::kafka::ConsumerClient {
 public:
  void start(Callback callback) override {
    callback(servicelib::datasource::kafka::ConsumerMessage{
        "key", "payload", "events", 2, 9, [this] { committed = true; }});
  }
  void stop() noexcept override {}
  std::atomic<bool> committed{false};
};

template <typename T, typename R, typename E = std::exception_ptr>
class TestSourceInput final {
 public:
  explicit TestSourceInput(bool hasResult) : hasResult_(hasResult) {}

  int getEndpointId() const noexcept { return 3; }
  std::size_t getConfigId() const noexcept { return 30; }
  const TestSourceInput* getResultStream() const noexcept {
    return hasResult_ ? this : nullptr;
  }
  void consume(servicelib::MessageContext, servicelib::Payload<T>) {}
  void consumeError(servicelib::MessageContext, servicelib::Payload<E>) {}
  void setResultConsumer(
      std::function<void(servicelib::MessageContext, servicelib::Payload<R>)>
          consumer) {
    resultConsumer_ = std::move(consumer);
  }

 private:
  bool hasResult_;
  std::function<void(servicelib::MessageContext, servicelib::Payload<R>)>
      resultConsumer_;
};

struct KafkaSourceHandler final {
  using State = int;
  userver::engine::SingleUseEvent* done;
  std::string* observed;
  int concurrency(auto&) { return 1; }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(
      servicelib::MessageContext, auto&, State&,
      const servicelib::datasource::kafka::ConsumerMessage& message,
      auto result) {
    *observed = message.topic() + ":" + message.key() + ":" + message.value();
    message.commit();
    result.done();
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) {
    return "result";
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_FALSE(error);
    done->Send();
  }
};

UTEST(KafkaDataSource, CopiesRecordAndExposesUserverCommit) {
  TestEnvironment environment;
  FakeKafkaConsumer consumer;
  userver::engine::SingleUseEvent done;
  std::string observed;
  TestSourceInput<std::string, int> input{false};
  auto endpoint = servicelib::datasource::kafka::Endpoint<
      std::string, int, KafkaSourceHandler>::make(
      environment, input, consumer, KafkaSourceHandler{&done, &observed});
  endpoint->start(servicelib::Context{});
  done.Wait();
  endpoint->stop(servicelib::Context{});
  EXPECT_EQ(observed, "events:key:payload");
  EXPECT_TRUE(consumer.committed.load());
}

struct WaitingKafkaSourceHandler final {
  using State = int;
  userver::engine::SingleUseEvent* entered;
  userver::engine::SingleUseEvent* ended;
  int concurrency(auto&) { return 1; }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&,
                      const servicelib::datasource::kafka::ConsumerMessage&,
                      auto) {
    entered->Send();
  }
  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) {
    return "result";
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) noexcept {
    EXPECT_TRUE(error);
    ended->Send();
  }
};

UTEST(KafkaDataSource, StopCancelsPendingInlineMessageBeforeConsumerStop) {
  TestEnvironment environment;
  FakeKafkaConsumer consumer;
  userver::engine::SingleUseEvent entered;
  userver::engine::SingleUseEvent ended;
  TestSourceInput<std::string, int> input{true};
  auto endpoint = servicelib::datasource::kafka::Endpoint<
      std::string, int, WaitingKafkaSourceHandler>::make(
      environment, input, consumer,
      WaitingKafkaSourceHandler{&entered, &ended});
  endpoint->start(servicelib::Context{});
  entered.Wait();
  endpoint->stop(servicelib::Context{});
  ended.Wait();
}

UTEST(KafkaAdmin, DisabledEndpointDoesNotContactBroker) {
  servicelib::config::KafkaDataConnectorConfig connector;
  servicelib::config::KafkaEndpointConfig endpoint;
  endpoint.enabled = false;
  endpoint.createTopic = true;
  endpoint.topic = "must-not-be-created";

  EXPECT_NO_THROW(servicelib::detail::EnsureKafkaTopic(connector, endpoint));
}

UTEST(KafkaAdmin, CreateTopicFalseDoesNotContactBroker) {
  servicelib::config::KafkaDataConnectorConfig connector;
  servicelib::config::KafkaEndpointConfig endpoint;
  endpoint.enabled = true;
  endpoint.createTopic = false;
  endpoint.topic = "must-not-be-created";

  EXPECT_NO_THROW(servicelib::detail::EnsureKafkaTopic(connector, endpoint));
}

UTEST(KafkaAdmin, ValidatesTopicAndBrokersBeforeConnecting) {
  servicelib::config::KafkaDataConnectorConfig connector;
  servicelib::config::KafkaEndpointConfig endpoint;
  endpoint.enabled = true;
  endpoint.createTopic = true;
  EXPECT_THROW(servicelib::detail::EnsureKafkaTopic(connector, endpoint),
               std::invalid_argument);

  endpoint.topic = "events";
  EXPECT_THROW(servicelib::detail::EnsureKafkaTopic(connector, endpoint),
               std::invalid_argument);
}

UTEST(KafkaAdmin, ReadsActualPartitionCountFromBrokerMetadata) {
  MockKafkaCluster cluster{2};
  servicelib::config::KafkaDataConnectorConfig connector;
  connector.brokers = cluster.brokers();
  connector.dialTimeout = 5000;

  EXPECT_EQ(servicelib::detail::KafkaTopicPartitionCount(connector, "events"),
            2);
}

}  // namespace

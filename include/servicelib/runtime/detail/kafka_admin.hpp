#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>

#if __has_include(<librdkafka/rdkafka.h>)
#include <librdkafka/rdkafka.h>
#else
#include <rdkafka.h>
#endif

#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>

namespace servicelib::detail {

inline void ApplyKafkaSecurity(
    rd_kafka_conf_t* kafkaConfig,
    const config::KafkaDataConnectorConfig& connector) {
  const auto setConfig = [kafkaConfig](const char* name,
                                       const std::string& value) {
    char error[512]{};
    if (rd_kafka_conf_set(kafkaConfig, name, value.c_str(), error,
                          sizeof(error)) != RD_KAFKA_CONF_OK) {
      throw std::invalid_argument(std::string{"Kafka config "} + name +
                                  ": " + error);
    }
  };
  switch (connector.securityProtocol) {
    case api::KafkaSecurityProtocol::kSASLPLAINTEXT:
      setConfig("security.protocol", "SASL_PLAINTEXT");
      break;
    case api::KafkaSecurityProtocol::kSASLSSL:
      setConfig("security.protocol", "SASL_SSL");
      break;
    case api::KafkaSecurityProtocol::kPLAINTEXT:
      setConfig("security.protocol", "PLAINTEXT");
      return;
  }
  if (connector.username.empty() || connector.password.empty()) {
    throw std::invalid_argument(
        "Kafka SASL username and password must both be configured");
  }
  switch (connector.saslMechanism) {
    case api::KafkaSaslMechanism::kSCRAMSHA256:
      setConfig("sasl.mechanism", "SCRAM-SHA-256");
      break;
    case api::KafkaSaslMechanism::kSCRAMSHA512:
      setConfig("sasl.mechanism", "SCRAM-SHA-512");
      break;
    case api::KafkaSaslMechanism::kPLAIN:
      setConfig("sasl.mechanism", "PLAIN");
      break;
  }
  setConfig("sasl.username", connector.username);
  setConfig("sasl.password", connector.password);
}

inline void EnsureKafkaTopic(
    const config::KafkaDataConnectorConfig& connector,
    const config::KafkaEndpointConfig& endpoint) {
  if (!endpoint.enabled || !endpoint.createTopic) return;
  if (endpoint.topic.empty()) {
    throw std::invalid_argument("Kafka topic to create is empty");
  }
  if (connector.brokers.empty()) {
    throw std::invalid_argument("Kafka brokers are empty");
  }

  struct KafkaDeleter final {
    void operator()(rd_kafka_t* value) const noexcept {
      if (value) rd_kafka_destroy(value);
    }
  };
  struct TopicDeleter final {
    void operator()(rd_kafka_NewTopic_t* value) const noexcept {
      if (value) rd_kafka_NewTopic_destroy(value);
    }
  };
  struct OptionsDeleter final {
    void operator()(rd_kafka_AdminOptions_t* value) const noexcept {
      if (value) rd_kafka_AdminOptions_destroy(value);
    }
  };
  struct QueueDeleter final {
    void operator()(rd_kafka_queue_t* value) const noexcept {
      if (value) rd_kafka_queue_destroy(value);
    }
  };
  struct EventDeleter final {
    void operator()(rd_kafka_event_t* value) const noexcept {
      if (value) rd_kafka_event_destroy(value);
    }
  };

  const auto timeout = connector.dialTimeout > 0
                           ? std::chrono::milliseconds{static_cast<std::int64_t>(
                                 connector.dialTimeout)}
                           : std::chrono::seconds{30};
  auto* kafkaConfig = rd_kafka_conf_new();
  const auto setConfig = [&kafkaConfig](const char* name,
                                        const std::string& value) {
    char error[512]{};
    if (rd_kafka_conf_set(kafkaConfig, name, value.c_str(), error,
                          sizeof(error)) != RD_KAFKA_CONF_OK) {
      throw std::invalid_argument(std::string{"Kafka config "} + name +
                                  ": " + error);
    }
  };
  try {
    setConfig("bootstrap.servers", connector.brokers);
    ApplyKafkaSecurity(kafkaConfig, connector);
    setConfig("socket.timeout.ms", std::to_string(timeout.count()));
    if (!connector.version.empty()) {
      setConfig("broker.version.fallback", connector.version);
    }
  } catch (...) {
    rd_kafka_conf_destroy(kafkaConfig);
    throw;
  }

  char error[512]{};
  std::unique_ptr<rd_kafka_t, KafkaDeleter> kafka{
      rd_kafka_new(RD_KAFKA_PRODUCER, kafkaConfig, error, sizeof(error))};
  if (!kafka) {
    throw std::runtime_error(std::string{"Kafka admin: "} + error);
  }

  std::unique_ptr<rd_kafka_NewTopic_t, TopicDeleter> topic{
      rd_kafka_NewTopic_new(endpoint.topic.c_str(),
                            std::max(endpoint.partitions, 1),
                            std::max(endpoint.replicationFactor, 1), error,
                            sizeof(error))};
  if (!topic) {
    throw std::invalid_argument(std::string{"Kafka new topic: "} + error);
  }
  std::unique_ptr<rd_kafka_AdminOptions_t, OptionsDeleter> options{
      rd_kafka_AdminOptions_new(kafka.get(), RD_KAFKA_ADMIN_OP_CREATETOPICS)};
  if (!options) throw std::runtime_error("Kafka admin options creation failed");
  if (rd_kafka_AdminOptions_set_request_timeout(
          options.get(), static_cast<int>(timeout.count()), error,
          sizeof(error)) != RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw std::invalid_argument(std::string{"Kafka admin timeout: "} + error);
  }
  std::unique_ptr<rd_kafka_queue_t, QueueDeleter> queue{
      rd_kafka_queue_new(kafka.get())};
  if (!queue) throw std::runtime_error("Kafka admin queue creation failed");

  auto* topicRequest = topic.get();
  rd_kafka_CreateTopics(kafka.get(), &topicRequest, 1, options.get(),
                        queue.get());
  std::unique_ptr<rd_kafka_event_t, EventDeleter> event{
      rd_kafka_queue_poll(queue.get(), static_cast<int>(timeout.count()))};
  if (!event) {
    throw std::runtime_error("Kafka create topic timed out for " +
                             endpoint.topic);
  }
  if (const auto status = rd_kafka_event_error(event.get());
      status != RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw std::runtime_error(
        "Kafka create topic failed for " + endpoint.topic + ": " +
        rd_kafka_event_error_string(event.get()));
  }
  const auto* result = rd_kafka_event_CreateTopics_result(event.get());
  if (!result) {
    throw std::runtime_error("Kafka create topic returned no result for " +
                             endpoint.topic);
  }
  std::size_t count{};
  const auto** topics = rd_kafka_CreateTopics_result_topics(result, &count);
  if (count != 1 || !topics || !topics[0]) {
    throw std::runtime_error("Kafka create topic returned an invalid result for " +
                             endpoint.topic);
  }
  const auto status = rd_kafka_topic_result_error(topics[0]);
  if (status != RD_KAFKA_RESP_ERR_NO_ERROR &&
      status != RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS) {
    throw std::runtime_error(
        "Kafka create topic failed for " + endpoint.topic + ": " +
        rd_kafka_topic_result_error_string(topics[0]));
  }
}

inline std::uint32_t KafkaTopicPartitionCount(
    const config::KafkaDataConnectorConfig& connector,
    const std::string& topicName) {
  if (topicName.empty()) {
    throw std::invalid_argument("Kafka metadata topic is empty");
  }
  if (connector.brokers.empty()) {
    throw std::invalid_argument("Kafka brokers are empty");
  }

  struct KafkaDeleter final {
    void operator()(rd_kafka_t* value) const noexcept {
      if (value) rd_kafka_destroy(value);
    }
  };
  struct TopicDeleter final {
    void operator()(rd_kafka_topic_t* value) const noexcept {
      if (value) rd_kafka_topic_destroy(value);
    }
  };
  struct MetadataDeleter final {
    void operator()(const rd_kafka_metadata_t* value) const noexcept {
      if (value) rd_kafka_metadata_destroy(value);
    }
  };

  const auto timeout = connector.dialTimeout > 0
                           ? std::chrono::milliseconds{static_cast<std::int64_t>(
                                 connector.dialTimeout)}
                           : std::chrono::seconds{30};
  auto* kafkaConfig = rd_kafka_conf_new();
  const auto setConfig = [&kafkaConfig](const char* name,
                                        const std::string& value) {
    char error[512]{};
    if (rd_kafka_conf_set(kafkaConfig, name, value.c_str(), error,
                          sizeof(error)) != RD_KAFKA_CONF_OK) {
      throw std::invalid_argument(std::string{"Kafka config "} + name +
                                  ": " + error);
    }
  };
  try {
    setConfig("bootstrap.servers", connector.brokers);
    setConfig("socket.timeout.ms", std::to_string(timeout.count()));
    if (!connector.version.empty()) {
      setConfig("broker.version.fallback", connector.version);
      setConfig("api.version.request", "false");
    }
  } catch (...) {
    rd_kafka_conf_destroy(kafkaConfig);
    throw;
  }

  char error[512]{};
  std::unique_ptr<rd_kafka_t, KafkaDeleter> kafka{
      rd_kafka_new(RD_KAFKA_PRODUCER, kafkaConfig, error, sizeof(error))};
  if (!kafka) {
    throw std::runtime_error(std::string{"Kafka metadata: "} + error);
  }
  std::unique_ptr<rd_kafka_topic_t, TopicDeleter> topic{
      rd_kafka_topic_new(kafka.get(), topicName.c_str(), nullptr)};
  if (!topic) {
    throw std::runtime_error("Kafka metadata topic creation failed for " +
                             topicName);
  }

  const rd_kafka_metadata_t* rawMetadata{};
  const auto status = rd_kafka_metadata(
      kafka.get(), 0, topic.get(), &rawMetadata,
      static_cast<int>(timeout.count()));
  std::unique_ptr<const rd_kafka_metadata_t, MetadataDeleter> metadata{
      rawMetadata};
  if (status != RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw std::runtime_error("Kafka metadata failed for " + topicName + ": " +
                             rd_kafka_err2str(status));
  }
  if (!metadata || metadata->topic_cnt != 1 ||
      metadata->topics[0].err != RD_KAFKA_RESP_ERR_NO_ERROR ||
      metadata->topics[0].partition_cnt <= 0) {
    throw std::runtime_error("Kafka metadata returned no partitions for " +
                             topicName);
  }
  return static_cast<std::uint32_t>(metadata->topics[0].partition_cnt);
}

}  // namespace servicelib::detail

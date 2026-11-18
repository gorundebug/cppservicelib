#pragma once
#include <boost/asio/ip/address_v6.hpp>
#include <chrono>
#include <optional>
#include <regex>
#include <servicelib/api/serviceapi_parse.hpp>
#include <servicelib/runtime/config/config_parse_common.hpp>
#include <userver/formats/parse/common_containers.hpp>
#include <vector>
namespace servicelib::config {
struct PulsarAddress {
  std::string scheme, host, port;
};
inline PulsarAddress ParsePulsarAddress(const std::string& value, bool admin) {
  static const std::regex pattern(
      R"(^([a-z+]+)://(\[[0-9a-fA-F:.]+\]|[A-Za-z0-9_.-]+)(:([0-9]+))?$)");
  std::smatch match;
  if (!std::regex_match(value, match, pattern))
    throw std::invalid_argument("Invalid Pulsar URL");
  PulsarAddress a{match[1], match[2], match[4]};
  if (admin ? (a.scheme != "http" && a.scheme != "https")
            : (a.scheme != "pulsar" && a.scheme != "pulsar+ssl"))
    throw std::invalid_argument("Invalid Pulsar URL scheme");
  if (a.port.empty())
    a.port = admin ? (a.scheme == "https" ? "443" : "80")
                   : (a.scheme == "pulsar+ssl" ? "6651" : "6650");
  try {
    auto n = std::stoll(a.port);
    if (n < 1 || n > 65535) throw std::invalid_argument("");
  } catch (...) {
    throw std::invalid_argument("Invalid Pulsar URL port");
  }
  if (a.host.front() == '[') {
    a.host = a.host.substr(1, a.host.size() - 2);
    boost::system::error_code error;
    boost::asio::ip::make_address_v6(a.host, error);
    if (error) throw std::invalid_argument("Invalid Pulsar IPv6 address");
  }
  return a;
}
struct PulsarDataConnectorConfig {
  int id{};
  std::string name;
  api::DataConnectorImplementation implementation{
      api::DataConnectorImplementation::kPulsarCpp};
  api::ProgrammingLanguage programmingLanguage{};
  std::vector<std::string> servers;
  std::string adminUrl, token, adminToken, tlsCaFile, tlsCertFile, tlsKeyFile;
  std::int64_t connectTimeoutMs{2000}, operationTimeoutMs{5000};
  PropertiesMap properties;
  auto GetType() const noexcept { return api::DataConnectorType::kPulsar; }
  const userver::formats::yaml::Value* GetProperty(
      const std::string& key) const {
    return detail::FindProperty(properties, key);
  }
  void validate() const {
    if (servers.size() != 1 ||
        implementation != api::DataConnectorImplementation::kPulsarCpp)
      throw std::invalid_argument(
          "Pulsar requires one URL and official C++ client");
    const auto service = ParsePulsarAddress(servers[0], false),
               admin = ParsePulsarAddress(adminUrl, true);
    if (service.scheme == "pulsar+ssl" && admin.scheme != "https")
      throw std::invalid_argument("Pulsar TLS requires HTTPS admin");
    if ((!tlsCaFile.empty() || !tlsCertFile.empty() || !tlsKeyFile.empty()) &&
        (service.scheme != "pulsar+ssl" || admin.scheme != "https"))
      throw std::invalid_argument("Pulsar TLS files require secure URLs");
    if (tlsCertFile.empty() != tlsKeyFile.empty() ||
        (!tlsCertFile.empty() && (!token.empty() || !adminToken.empty())))
      throw std::invalid_argument("Pulsar token and TLS identity conflict");
    if (token.find_first_of("\r\n") != std::string::npos ||
        adminToken.find_first_of("\r\n") != std::string::npos)
      throw std::invalid_argument("Invalid Pulsar token header");
    if (connectTimeoutMs < 0 || operationTimeoutMs < 0 ||
        connectTimeoutMs > 2147483647 || operationTimeoutMs > 2147483647)
      throw std::invalid_argument("Invalid Pulsar timeout");
    for (auto key : {"username", "password", "credentialsFile", "tlsEnabled",
                     "tlsServerName"})
      if (GetProperty(key))
        throw std::invalid_argument("Unsupported Pulsar authentication field");
  }
  auto connectTimeout() const {
    return std::chrono::milliseconds(connectTimeoutMs ? connectTimeoutMs
                                                      : 2000);
  }
  auto operationTimeout() const {
    return std::chrono::milliseconds(operationTimeoutMs ? operationTimeoutMs
                                                        : 5000);
  }
};
struct PulsarEndpointConfig {
  int id{}, idDataConnector{};
  std::string name, topic, subscriptionName, subscriptionType{"Shared"},
      subscriptionInitialPosition;
  bool enabled{true}, tracingEnabled{}, createTopic{}, createSubscription{};
  int partitions{}, receiverQueueSize{256}, publisherMaxInFlight{256},
      negativeAckRedeliveryDelayMs{60000};
  std::string functionName, functionPackage, functionModule,
      functionDescription, functionInitializerGroup;
  bool publicFunction{};
  PropertiesMap properties;
  auto GetType() const noexcept { return api::DataConnectorType::kPulsar; }
  const userver::formats::yaml::Value* GetProperty(
      const std::string& key) const {
    return detail::FindProperty(properties, key);
  }
  void validate(bool source) const {
    static const std::regex t(
        R"(^persistent://[^/%?#\\\s]+/[^/%?#\\\s]+/[^/%?#\\\s]+$)");
    for (unsigned char ch : topic)
      if (ch < 32 || ch == 127)
        throw std::invalid_argument("Invalid Pulsar topic character");
    for (unsigned char ch : subscriptionName)
      if (ch < 32 || ch == 127)
        throw std::invalid_argument("Invalid Pulsar subscription character");
    if (!std::regex_match(topic, t) ||
        topic.find("/../") != std::string::npos || topic.ends_with("/..") ||
        topic.find("/./") != std::string::npos || topic.ends_with("/."))
      throw std::invalid_argument("Invalid Pulsar persistent topic");
    if (partitions < 0 || partitions > 1000 || (partitions && !createTopic))
      throw std::invalid_argument(
          "Pulsar partitions require explicit creation");
    if (receiverQueueSize < 1 || receiverQueueSize > 4096 ||
        publisherMaxInFlight < 1 || publisherMaxInFlight > 4096 ||
        negativeAckRedeliveryDelayMs < 1)
      throw std::invalid_argument("Invalid Pulsar admission bounds");
    if (source) {
      if (subscriptionName.empty() ||
          subscriptionName.find_first_of("/%?#\\\r\n") != std::string::npos)
        throw std::invalid_argument("Invalid Pulsar subscription");
      if (subscriptionType != "Shared" && subscriptionType != "Exclusive" &&
          subscriptionType != "Failover")
        throw std::invalid_argument("Unsupported Pulsar subscription type");
      if (!subscriptionInitialPosition.empty() &&
          (!createSubscription || (subscriptionInitialPosition != "Earliest" &&
                                   subscriptionInitialPosition != "Latest")))
        throw std::invalid_argument(
            "Pulsar initial position requires explicit subscription creation");
    } else if (!subscriptionName.empty() || createSubscription ||
               !subscriptionInitialPosition.empty())
      throw std::invalid_argument("Pulsar sink cannot configure subscription");
  }
};
inline PulsarDataConnectorConfig Parse(
    const userver::formats::yaml::Value& v,
    userver::formats::parse::To<PulsarDataConnectorConfig>) {
  PulsarDataConnectorConfig c;
  c.id = v["id"].As<int>(0);
  c.name = v["name"].As<std::string>("");
  c.implementation = v["implementation"].As<api::DataConnectorImplementation>(
      c.implementation);
  c.programmingLanguage = v["programmingLanguage"].As<api::ProgrammingLanguage>(
      c.programmingLanguage);
  c.servers = v["servers"].As<std::vector<std::string>>({});
#define READ_STRING(field) c.field = v[#field].As<std::string>("")
  READ_STRING(adminUrl);
  READ_STRING(token);
  READ_STRING(adminToken);
  READ_STRING(tlsCaFile);
  READ_STRING(tlsCertFile);
  READ_STRING(tlsKeyFile);
#undef READ_STRING
  c.connectTimeoutMs = v["connectTimeoutMs"].As<std::int64_t>(2000);
  c.operationTimeoutMs = v["operationTimeoutMs"].As<std::int64_t>(5000);
  detail::ParseRemainingProperties(
      v,
      {"id", "name", "type", "implementation", "programmingLanguage", "servers",
       "adminUrl", "token", "adminToken", "tlsCaFile", "tlsCertFile",
       "tlsKeyFile", "connectTimeoutMs", "operationTimeoutMs"},
      c.properties);
  return c;
}
inline PulsarEndpointConfig Parse(
    const userver::formats::yaml::Value& v,
    userver::formats::parse::To<PulsarEndpointConfig>) {
  PulsarEndpointConfig c;
  c.id = v["id"].As<int>(0);
  c.idDataConnector = v["idDataConnector"].As<int>(0);
#define READ_STRING(field) c.field = v[#field].As<std::string>(c.field)
  READ_STRING(name);
  READ_STRING(topic);
  READ_STRING(subscriptionName);
  READ_STRING(subscriptionType);
  READ_STRING(subscriptionInitialPosition);
#undef READ_STRING
#define READ_FIELD(field, type) c.field = v[#field].As<type>(c.field)
  READ_FIELD(enabled, bool);
  READ_FIELD(tracingEnabled, bool);
  READ_FIELD(createTopic, bool);
  READ_FIELD(createSubscription, bool);
  READ_FIELD(partitions, int);
  READ_FIELD(receiverQueueSize, int);
  READ_FIELD(publisherMaxInFlight, int);
  READ_FIELD(negativeAckRedeliveryDelayMs, int);
#undef READ_FIELD
  detail::ParseFunctionFields(v, c);
  detail::ParseRemainingProperties(v,
                                   {"id",
                                    "name",
                                    "type",
                                    "idDataConnector",
                                    "topic",
                                    "subscriptionName",
                                    "subscriptionType",
                                    "subscriptionInitialPosition",
                                    "enabled",
                                    "tracingEnabled",
                                    "createTopic",
                                    "createSubscription",
                                    "partitions",
                                    "receiverQueueSize",
                                    "publisherMaxInFlight",
                                    "negativeAckRedeliveryDelayMs",
                                    "functionName",
                                    "functionPackage",
                                    "functionModule",
                                    "functionDescription",
                                    "functionInitializerGroup",
                                    "publicFunction"},
                                   c.properties);
  return c;
}
}  // namespace servicelib::config

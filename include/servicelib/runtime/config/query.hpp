#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <servicelib/api/serviceapi.hpp>
#include <servicelib/api/serviceapi_parse.hpp>
#include <servicelib/runtime/config/config_parse_common.hpp>

namespace servicelib::config {

struct PostgreSQLUserverDataConnectorConfig {
  int id{};
  std::string name;
  api::DataConnectorImplementation implementation{api::DataConnectorImplementation::kUserverPostgreSQL};
  std::string connectionString;
  int connectionsCount{10};
  int operationTimeoutMs{30000};
  PropertiesMap properties;
  api::DataConnectorType GetType() const noexcept { return api::DataConnectorType::kPostgreSQLUserver; }
  const userver::formats::yaml::Value* GetProperty(const std::string& name) const {
    return detail::FindProperty(properties, name);
  }
  void Validate() const {
    if (implementation != api::DataConnectorImplementation::kUserverPostgreSQL)
      throw std::invalid_argument("PostgreSQLUserver requires userver/postgresql");
    // Do not include a DSN or parser input in diagnostics: it may contain secrets.
    if (connectionString.empty()) throw std::invalid_argument("PostgreSQL connectionString is required");
    if (connectionsCount <= 0 || operationTimeoutMs < 0)
      throw std::invalid_argument("PostgreSQL pool size must be positive and timeouts nonnegative");
  }
};

// S3Userver selects the native library through its connector type, like NATS.
// Credentials are maker dependencies, not public configuration properties.
struct S3UserverDataConnectorConfig {
  int id{};
  std::string name;
  api::DataConnectorImplementation implementation{api::DataConnectorImplementation::kUndefined};
  std::string address;
  std::string region;
  bool forcePathStyle{};
  int operationTimeoutMs{30000};
  PropertiesMap properties;
  api::DataConnectorType GetType() const noexcept { return api::DataConnectorType::kS3Userver; }
  const userver::formats::yaml::Value* GetProperty(const std::string& name) const {
    return detail::FindProperty(properties, name);
  }
  bool UseTLS() const { return address.starts_with("https://"); }
  std::string Server() const {
    const auto prefix = UseTLS() ? 8u : 7u;
    auto server = address.substr(prefix);
    if (server.ends_with('/')) server.pop_back();
    return server;
  }
  void Validate() const {
    if (implementation != api::DataConnectorImplementation::kUndefined)
      throw std::invalid_argument("S3Userver selects the native userver client");
    if (!address.starts_with("https://") && !address.starts_with("http://"))
      throw std::invalid_argument("S3 address requires HTTP or HTTPS");
    const auto server = Server();
    // The native client constructs bucket.host; credentials, URL paths and
    // IPv6 literals cannot be used as a virtual-host authority here.
    if (server.empty() || server.find_first_of("/@?#\\ \t\r\n[]") != std::string::npos)
      throw std::invalid_argument("S3 address requires a host and optional port only");
    const auto colon = server.find(':');
    if (colon != std::string::npos) {
      const auto port = server.substr(colon + 1);
      if (!colon || port.empty() || port.size() > 5 ||
          port.find_first_not_of("0123456789") != std::string::npos ||
          std::stoi(port) < 1 || std::stoi(port) > 65535)
        throw std::invalid_argument("S3 address has an invalid port");
    }
    if (region.empty()) throw std::invalid_argument("S3 signing region is required");
    if (forcePathStyle) throw std::invalid_argument("S3Userver does not support forcePathStyle");
    if (operationTimeoutMs <= 0) throw std::invalid_argument("S3 operation timeout must be positive");
  }
};

struct QueryEndpointConfig {
  int id{};
  std::string name;
  int idDataConnector{};
  api::DataConnectorType type{api::DataConnectorType::kPostgreSQLUserver};
  bool enabled{true};
  bool tracingEnabled{};
  std::string queryOperation;
  std::string sqlFile;
  std::uint32_t fetchSize{256};
  std::string bucket;
  std::string functionName;
  std::string functionPackage;
  bool publicFunction{};
  std::string functionDescription;
  std::string functionInitializerGroup;
  std::string functionModule;
  PropertiesMap properties;
  api::DataConnectorType GetType() const noexcept { return type; }
  const userver::formats::yaml::Value* GetProperty(const std::string& name) const {
    return detail::FindProperty(properties, name);
  }
  void Validate() const {
    if (type == api::DataConnectorType::kPostgreSQLUserver) {
      if (queryOperation != "Rows" && queryOperation != "Execute")
        throw std::invalid_argument("PostgreSQL Query requires Rows or Execute");
    } else if (type == api::DataConnectorType::kS3Userver) {
      if (queryOperation != "GetObject" && queryOperation != "HeadObject" &&
          queryOperation != "ListObjects" && queryOperation != "PutObject" &&
          queryOperation != "DeleteObject")
        throw std::invalid_argument("Unsupported S3 Query operation");
      // userver clients are bucket-bound; the runtime override must be resolved
      // before endpoint construction, including for initially disabled endpoints.
      if (bucket.empty() || bucket.find_first_of("/:@?#\\ \t\r\n") != std::string::npos)
        throw std::invalid_argument("S3Userver Query requires a nonempty runtime bucket name");
    } else {
      throw std::invalid_argument("Unsupported userver Query connector type");
    }
    if (!fetchSize) throw std::invalid_argument("Query fetchSize must be positive");
  }
};

inline PostgreSQLUserverDataConnectorConfig Parse(const userver::formats::yaml::Value& value,
    userver::formats::parse::To<PostgreSQLUserverDataConnectorConfig>) {
  PostgreSQLUserverDataConnectorConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.implementation = value["implementation"].As<api::DataConnectorImplementation>(result.implementation);
  result.connectionString = value["connectionString"].As<std::string>("");
  result.connectionsCount = value["connectionsCount"].As<int>(result.connectionsCount);
  if (!value["connectTimeoutMs"].IsMissing())
    throw std::invalid_argument("connectTimeoutMs must be omitted: PostgreSQLUserver has no independent native connection timeout");
  result.operationTimeoutMs = value["operationTimeoutMs"].As<int>(result.operationTimeoutMs);
  detail::ParseRemainingProperties(value, {"id", "name", "type", "implementation",
      "programmingLanguage", "connectionString", "connectionsCount", "connectTimeoutMs",
      "operationTimeoutMs"}, result.properties);
  // Parsing also serves partial configuration overlays; RuntimeConfig validates
  // the complete configuration after defaults and environment are applied.
  return result;
}

inline S3UserverDataConnectorConfig Parse(const userver::formats::yaml::Value& value,
    userver::formats::parse::To<S3UserverDataConnectorConfig>) {
  S3UserverDataConnectorConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  if (!value["implementation"].IsMissing() &&
      value["implementation"].As<std::string>() != "userver/s3")
    throw std::invalid_argument("S3Userver requires userver/s3");
  result.address = value["address"].As<std::string>("");
  result.region = value["region"].As<std::string>("");
  result.forcePathStyle = value["forcePathStyle"].As<bool>(false);
  result.operationTimeoutMs = value["operationTimeoutMs"].As<int>(result.operationTimeoutMs);
  detail::ParseRemainingProperties(value, {"id", "name", "type", "implementation",
      "programmingLanguage", "address", "region", "forcePathStyle", "operationTimeoutMs"},
      result.properties);
  // Parsing also serves partial configuration overlays; RuntimeConfig validates
  // the complete configuration after defaults and environment are applied.
  return result;
}

inline QueryEndpointConfig Parse(const userver::formats::yaml::Value& value,
    userver::formats::parse::To<QueryEndpointConfig>) {
  QueryEndpointConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.idDataConnector = value["idDataConnector"].As<int>(0);
  result.type = value["type"].As<api::DataConnectorType>(result.type);
  result.enabled = value["enabled"].As<bool>(true);
  result.tracingEnabled = value["tracingEnabled"].As<bool>(false);
  result.queryOperation = value["queryOperation"].As<std::string>("");
  result.sqlFile = value["sqlFile"].As<std::string>("");
  result.fetchSize = value["fetchSize"].As<std::uint32_t>(result.fetchSize);
  result.bucket = value["bucket"].As<std::string>("");
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(value, {"id", "name", "type", "idDataConnector",
      "enabled", "tracingEnabled", "queryOperation", "sqlFile", "fetchSize", "bucket",
      "functionName", "functionPackage", "publicFunction", "functionDescription",
      "functionInitializerGroup", "functionModule"}, result.properties);
  // Parsing also serves partial configuration overlays; RuntimeConfig validates
  // the complete configuration after defaults and environment are applied.
  return result;
}
}  // namespace servicelib::config

#include <servicelib/runtime/status/status.hpp>
#include <userver/formats/yaml/serialize.hpp>
#include <userver/utest/utest.hpp>

namespace {
using namespace servicelib::config;
struct QueryConfig final : IConfig {
  ServiceConfig service{.id = 1, .name = "Query service"};
  QueryStreamConfig query{.id = 2, .name = "Read catalog", .idService = 1,
                          .idEndpoint = 7, .valueType = "CatalogRow"};
  QueryStreamConfig duplicate{.id = 3, .name = "Read another catalog", .idService = 1,
                              .idEndpoint = 7, .valueType = "CatalogRow"};
  PostgreSQLUserverDataConnectorConfig connector{.id = 8, .name = "Database",
      .connectionString = "postgresql://user:private-secret@db/catalog"};
  QueryEndpointConfig endpoint{.id = 7, .name = "Select", .idDataConnector = 8,
      .enabled = false, .queryOperation = "Rows", .fetchSize = 32};
  std::vector<const ServiceConfig*> GetServices() const override { return {&service}; }
  std::vector<StreamConfigRef> GetStreams() const override { return {query, duplicate}; }
  std::vector<DataConnectorConfigRef> GetDataConnectors() const override { return {connector}; }
  std::vector<EndpointConfigRef> GetEndpoints() const override { return {endpoint}; }
  std::vector<const PoolConfig*> GetPools() const override { return {}; }
  std::vector<const LinkConfig*> GetLinks() const override { return {}; }
  std::vector<const ModuleConfig*> GetModules() const override { return {}; }
  std::vector<const TypeConfig*> GetTypes() const override { return {}; }
};

TEST(QueryStatus, PublishesEndpointOnceWithoutCredentialsOrApplicationProperties) {
  QueryConfig config;
  config.connector.properties.emplace("applicationToken", userver::formats::yaml::FromString("private-token"));
  RuntimeConfig runtime(config);
  const auto text = servicelib::status::MakeGraphYaml(runtime);
  const auto yaml = userver::formats::yaml::FromString(text);
  EXPECT_EQ(yaml["streams"][0]["type"].As<std::string>(), "QUERY");
  EXPECT_EQ(yaml["streams"][0]["idEndpoint"].As<int>(), 7);
  EXPECT_EQ(yaml["streams"][0]["valueType"].As<std::string>(), "CatalogRow");
  EXPECT_EQ(yaml["endpoints"].GetSize(), 1);
  EXPECT_FALSE(yaml["endpoints"][0]["enabled"].As<bool>());
  EXPECT_EQ(yaml["endpoints"][0]["queryOperation"].As<std::string>(), "Rows");
  EXPECT_EQ(yaml["dataConnectors"].GetSize(), 1);
  EXPECT_EQ(yaml["dataConnectors"][0]["type"].As<std::string>(), "PostgreSQLUserver");
  EXPECT_EQ(text.find("private-secret"), std::string::npos);
  EXPECT_EQ(text.find("private-token"), std::string::npos);
  EXPECT_EQ(text.find("connectionString"), std::string::npos);
  EXPECT_EQ(config.connector.connectionString, "postgresql://user:private-secret@db/catalog");
}
}  // namespace

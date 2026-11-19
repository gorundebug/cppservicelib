#include <servicelib/runtime/config/config.hpp>
#include <userver/formats/yaml/serialize.hpp>
#include <userver/utest/utest.hpp>

namespace {
using namespace servicelib::config;
using userver::formats::yaml::FromString;

TEST(QueryConfig, PartialOverlayDefersValidationUntilEnvironmentIsApplied) {
  auto connector = FromString("connectionString: ''\napplicationSetting: value\n").As<PostgreSQLUserverDataConnectorConfig>();
  EXPECT_NE(connector.GetProperty("applicationSetting"), nullptr);
  EXPECT_THROW(connector.Validate(), std::invalid_argument);
  connector.connectionString = "postgresql://localhost/catalog";
  EXPECT_NO_THROW(connector.Validate());
  auto endpoint = FromString("enabled: false\n").As<QueryEndpointConfig>();
  EXPECT_FALSE(endpoint.enabled);
  EXPECT_THROW(endpoint.Validate(), std::invalid_argument);
  endpoint.queryOperation = "Rows";
  EXPECT_NO_THROW(endpoint.Validate());
}

TEST(QueryConfig, PostgreSQLParsesSettingsAndKeepsCustomProperties) {
  auto connector = FromString(R"(
id: 7
name: catalog
implementation: userver/postgresql
connectionString: postgresql://localhost/catalog
connectionsCount: 4
operationTimeoutMs: 2000
applicationSetting: custom
)").As<PostgreSQLUserverDataConnectorConfig>();
  EXPECT_EQ(connector.connectionsCount, 4);
  EXPECT_EQ(connector.operationTimeoutMs, 2000);
  ASSERT_NE(connector.GetProperty("applicationSetting"), nullptr);
  EXPECT_EQ(connector.GetProperty("applicationSetting")->As<std::string>(), "custom");
  EXPECT_EQ(connector.GetProperty("connectionString"), nullptr);
  const DataConnectorConfigRef ref(connector);
  EXPECT_EQ(ref.GetType(), servicelib::api::DataConnectorType::kPostgreSQLUserver);
}

TEST(QueryConfig, PostgreSQLZeroTimeoutsAndNegativeOverrides) {
  PostgreSQLUserverDataConnectorConfig connector;
  connector.connectionString = "postgresql://localhost/catalog";
  connector.operationTimeoutMs = 0;
  EXPECT_NO_THROW(connector.Validate());
  connector.operationTimeoutMs = -1;
  EXPECT_THROW(connector.Validate(), std::invalid_argument);
  connector.operationTimeoutMs = 0;
  EXPECT_THROW(FromString("connectTimeoutMs: 1000").As<PostgreSQLUserverDataConnectorConfig>(), std::invalid_argument);
}

TEST(QueryConfig, QueryDefaultsAndExplicitDisable) {
  auto endpoint = FromString("id: 1\nidDataConnector: 7\nqueryOperation: Rows\n").As<QueryEndpointConfig>();
  EXPECT_TRUE(endpoint.enabled);
  EXPECT_GT(endpoint.fetchSize, 0);
  endpoint = FromString("id: 1\nidDataConnector: 7\nqueryOperation: Execute\nenabled: false\n").As<QueryEndpointConfig>();
  EXPECT_FALSE(endpoint.enabled);
  const EndpointConfigRef ref(endpoint);
  EXPECT_EQ(ref.GetIdDataConnector(), 7);
}

TEST(QueryConfig, RejectsWrongOperationsAndInvalidLimits) {
  for (const auto* yaml : {"queryOperation: PutObject", "queryOperation: Rows\nfetchSize: 0",
                           "queryOperation: Rows\nfetchSize: -1"}) {
    EXPECT_THROW(FromString(yaml).As<QueryEndpointConfig>().Validate(), std::exception);
  }
  PostgreSQLUserverDataConnectorConfig connector;
  EXPECT_THROW(connector.Validate(), std::invalid_argument);
  connector.connectionString = "postgresql://localhost/catalog";
  connector.connectionsCount = 0;
  EXPECT_THROW(connector.Validate(), std::invalid_argument);
}
TEST(QueryConfig, S3ParsesTransportAndKeepsApplicationProperties) {
  auto connector = FromString(R"(
id: 9
name: objects
implementation: userver/s3
address: https://objects.example.test:9443/
region: eu-west-1
operationTimeoutMs: 2500
applicationSetting: custom
)").As<S3UserverDataConnectorConfig>();
  EXPECT_EQ(connector.Server(), "objects.example.test:9443");
  EXPECT_TRUE(connector.UseTLS());
  EXPECT_EQ(connector.operationTimeoutMs, 2500);
  ASSERT_NE(connector.GetProperty("applicationSetting"), nullptr);
  EXPECT_EQ(connector.GetProperty("address"), nullptr);
  EXPECT_EQ(connector.GetProperty("region"), nullptr);
  const DataConnectorConfigRef ref(connector);
  EXPECT_EQ(ref.GetType(), servicelib::api::DataConnectorType::kS3Userver);
  EXPECT_EQ(ref.As<S3UserverDataConnectorConfig>(), &connector);
}

TEST(QueryConfig, S3RejectsUnsupportedOrAmbiguousTransport) {
  S3UserverDataConnectorConfig connector;
  connector.region = "eu-west-1";
  for (const auto* address : {"", "objects.example.test", "ftp://objects.example.test",
      "https://", "https://user:secret@objects.example.test", "https://objects.example.test/path",
      "https://objects.example.test?secret=x", "https://objects.example.test#fragment",
      "https://objects.example.test:0", "https://objects.example.test:65536",
      "https://objects.example.test:abc", "https://[::1]:9000"}) {
    connector.address = address;
    EXPECT_THROW(connector.Validate(), std::invalid_argument);
  }
  connector.address = "http://localhost:9000";
  EXPECT_NO_THROW(connector.Validate());
  EXPECT_FALSE(connector.UseTLS());
  connector.forcePathStyle = true;
  EXPECT_THROW(connector.Validate(), std::invalid_argument);
  connector.forcePathStyle = false;
  connector.operationTimeoutMs = 0;
  EXPECT_THROW(connector.Validate(), std::invalid_argument);
  connector.operationTimeoutMs = 1000;
  connector.region.clear();
  EXPECT_THROW(connector.Validate(), std::invalid_argument);
}

TEST(QueryConfig, S3RequiresBucketForEveryOperationBeforeEndpointConstruction) {
  QueryEndpointConfig endpoint;
  endpoint.type = servicelib::api::DataConnectorType::kS3Userver;
  for (const auto* operation : {"GetObject", "HeadObject", "ListObjects", "PutObject", "DeleteObject"}) {
    endpoint.queryOperation = operation;
    for (const bool enabled : {true, false}) {
      endpoint.enabled = enabled;
      for (const auto* invalid : {"", "bucket/path", "https://bucket", "bucket name"}) {
        endpoint.bucket = invalid;
        EXPECT_THROW(endpoint.Validate(), std::invalid_argument);
      }
      endpoint.bucket = "catalog-snapshots";
      EXPECT_NO_THROW(endpoint.Validate());
    }
  }
}

}  // namespace

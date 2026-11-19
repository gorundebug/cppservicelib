// Native interface coverage only; HTTP behavior is tested separately.
#include <servicelib/dataquery/s3/operations.hpp>
#include <servicelib/dataquery/s3/connector.hpp>
#include <userver/s3api/authenticators/signature_v4.hpp>

namespace {
using namespace servicelib;
namespace s3 = dataquery::s3;
struct Get {
  s3::GetObjectRequest buildRequest(MessageContext, int&, const int&) {
    return {"key", std::nullopt, {}};
  }
  void handleResult(MessageContext ctx, int&, const s3::GetObjectResult& result,
                    dataquery::Collector<std::string>& out) { out.out(ctx, result.body); }
};
struct Put {
  s3::PutObjectRequest buildRequest(MessageContext, int&, const int&) {
    return {"key", "body", std::nullopt, "text/plain", std::nullopt, std::nullopt};
  }
  void handleResult(MessageContext ctx, int&, const std::string& result,
                    dataquery::Collector<std::string>& out) { out.out(ctx, result); }
};
struct Delete {
  s3::DeleteObjectRequest buildRequest(MessageContext, int&, const int&) { return {"key"}; }
  void handleResult(MessageContext ctx, int&, const s3::DeleteObjectResult&,
                    dataquery::Collector<std::string>& out) { out.out(ctx, "deleted"); }
};
struct List {
  s3::ListObjectsRequest buildRequest(MessageContext, int&, const int&) {
    return {"prefix/", "", 100};
  }
  void handlePage(MessageContext ctx, int&, const std::string& xml,
                  dataquery::Collector<std::string>& out) { out.out(ctx, xml); }
};
struct Head {
  s3::HeadObjectRequest buildRequest(MessageContext, int&, const int&) {
    return {"key", std::nullopt};
  }
  void handleResult(MessageContext ctx, int&, const userver::clients::http::Response& result,
                    dataquery::Collector<std::string>& out) {
    out.out(ctx, std::to_string(static_cast<int>(result.status_code())));
  }
};
[[maybe_unused]] void instantiateHead(std::shared_ptr<s3::Connector> connector,
    MessageContext ctx, dataquery::Collector<std::string>& out) {
  int state = 0; Head handler;
  s3::HeadObject(std::move(connector), "bucket").execute(ctx, state, 1, handler, out);
}
[[maybe_unused]] void instantiate(userver::s3api::ClientPtr client, MessageContext ctx,
                                  dataquery::Collector<std::string>& out) {
  int state = 0;
  Get get; Put put; Delete remove; List list;
  s3::GetObject(client).execute(ctx, state, 1, get, out);
  s3::PutObject(client).execute(ctx, state, 1, put, out);
  s3::DeleteObject(client).execute(ctx, state, 1, remove, out);
  s3::ListObjects(client).execute(ctx, state, 1, list, out);
}
[[maybe_unused]] userver::s3api::ClientPtr createClient(
    userver::clients::http::Client& http,
    userver::s3api::authenticators::AuthenticatorPtr authenticator,
    MessageContext context) {
  s3::Connector connector(context, http, "objects.example.test",
      userver::s3api::S3ConnectionType::kHttps, std::move(authenticator),
      std::chrono::milliseconds(1000));
  return connector.makeClient(context, "query-bucket");
}

// Credentials are supplied by the application maker, never embedded in a
// generated config or resolved through an invented AWS provider chain.
[[maybe_unused]] std::shared_ptr<s3::Connector> createV4Connector(
    userver::clients::http::Client& http, MessageContext context,
    std::string accessKey, userver::s3api::Secret secretKey,
    std::string region) {
  config::S3UserverDataConnectorConfig settings;
  settings.address = "https://objects.example.test";
  settings.region = std::move(region);
  settings.operationTimeoutMs = 1000;
  return s3::makeConnector(context, http, settings,
                           std::move(accessKey), std::move(secretKey));
}

}

#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/config/query.hpp>
#include <userver/s3api/authenticators/signature_v4.hpp>
#include <userver/clients/http/client.hpp>
#include <userver/clients/http/response.hpp>
#include <userver/http/url.hpp>
#include <userver/http/common_headers.hpp>
#include <userver/s3api/models/request.hpp>
#include <userver/s3api/clients/s3api.hpp>

namespace servicelib::dataquery::s3 {

// The userver HttpClient component owns network resources and outlives Query
// endpoints. The maker supplies authentication explicitly: the native AccessKey
// authenticator uses Signature V2, not an AWS default credential-provider chain.
// One connection/configuration is shared by bucket-bound endpoint clients.
class Connector final {
 public:
  Connector(const MessageContext& context, userver::clients::http::Client& http,
            std::string server, userver::s3api::S3ConnectionType protocol,
            userver::s3api::authenticators::AuthenticatorPtr authenticator,
            std::chrono::milliseconds requestTimeout)
      : authenticator_(std::move(authenticator)), http_(http), server_(server),
        protocol_(protocol), requestTimeout_(requestTimeout) {
    if (context.cancelled()) throw OperationCancelledError("S3 connector creation cancelled");
    if (!authenticator_) throw std::invalid_argument("S3 authenticator is required");
    if (requestTimeout <= std::chrono::milliseconds::zero())
      throw std::invalid_argument("S3 native request timeout must be positive");
    // Native MakeS3Connection expects an authority, and constructs bucket.host.
    // Do not accept credentials, a path or signed query in this setting.
    if (server.empty() || server.find_first_of("/@?#\\ \t\r\n") != std::string::npos)
      throw std::invalid_argument("S3 server must be a host with optional port");
    if (protocol != userver::s3api::S3ConnectionType::kHttp &&
        protocol != userver::s3api::S3ConnectionType::kHttps)
      throw std::invalid_argument("S3 connection protocol must be HTTP or HTTPS");
    // userver retry(1) means one total attempt, not one additional retry.
    connection_ = userver::s3api::MakeS3Connection(
        http, protocol, std::move(server), userver::s3api::ConnectionCfg(requestTimeout, 1));
  }

  userver::s3api::ClientPtr makeClient(const MessageContext& context,
                                      const std::string& bucket) const {
    if (context.cancelled()) throw OperationCancelledError("S3 endpoint creation cancelled");
    validateBucket(bucket);
    return userver::s3api::GetS3Client(connection_, authenticator_, bucket);
  }

  // The public native GetObjectHead loses every error as nullopt. Use the same
  // native authenticator and coroutine-aware HTTP client for an actual HEAD,
  // preserving its response status. No GET fallback or additional retry.
  std::shared_ptr<userver::clients::http::Response> head(
      const MessageContext& context, const std::string& bucket,
      const std::string& key, const std::optional<std::string>& version = std::nullopt) const {
    if (context.cancelled()) throw OperationCancelledError("S3 HEAD cancelled");
    validateBucket(bucket);
    userver::s3api::Request native;
    native.method = userver::clients::http::HttpMethod::kHead;
    native.bucket = bucket;
    native.req = userver::http::EncodeS3Key(key);
    if (version) native.req += "?" + userver::http::MakeQuery({{"versionId", *version}});
    const auto host = bucket + "." + server_;
    native.headers[userver::http::headers::kHost] = host;
    // Include the final Host before signing; authenticators may sign it.
    // Do not implement signing or copy credentials into HTTP configuration.
    for (auto& [name, value] : authenticator_->Auth(native)) {
      if (native.headers.contains(name))
        throw std::invalid_argument("S3 authenticator returned a conflicting header");
      native.headers.emplace(name, std::move(value));
    }
    // Match the native client's documented/tested localhost connection route.
    const auto& authority = server_.starts_with("localhost:") ? server_ : host;
    const auto url = std::string(protocol_ == userver::s3api::S3ConnectionType::kHttps
                                   ? "https://" : "http://") + authority + "/" + native.req;
    auto response = http_.CreateNotSignedRequest().head(url).headers(native.headers)
                        .timeout(requestTimeout_).retry(1).perform();
    response->raise_for_status();
    if (context.cancelled()) throw OperationCancelledError("S3 HEAD cancelled");
    return response;
  }

 private:
  static void validateBucket(const std::string& bucket) {
    if (bucket.empty() || bucket.find_first_of("/:@?#\\ \t\r\n") != std::string::npos)
      throw std::invalid_argument("S3 bucket must be a nonempty bucket name");
  }
  std::shared_ptr<userver::s3api::S3Connection> connection_;
  userver::s3api::authenticators::AuthenticatorPtr authenticator_;
  userver::clients::http::Client& http_;
  std::string server_;
  userver::s3api::S3ConnectionType protocol_;
  std::chrono::milliseconds requestTimeout_;
};

// The application maker resolves secrets and supplies them explicitly. The SDK
// neither reads environment variables nor exposes credentials in runtime config.
inline std::shared_ptr<Connector> makeConnector(
    const MessageContext& context, userver::clients::http::Client& http,
    const config::S3UserverDataConnectorConfig& settings,
    std::string accessKey, userver::s3api::Secret secretKey) {
  settings.Validate();
  if (accessKey.empty() || secretKey.GetUnderlying().empty())
    throw std::invalid_argument("S3 signing credentials are required");
  auto authenticator = std::make_shared<userver::s3api::authenticators::SignatureV4>(
      std::move(accessKey), std::move(secretKey), settings.region);
  return std::make_shared<Connector>(context, http, settings.Server(),
      settings.UseTLS() ? userver::s3api::S3ConnectionType::kHttps
                        : userver::s3api::S3ConnectionType::kHttp,
      std::move(authenticator), std::chrono::milliseconds(settings.operationTimeoutMs));
}
}  // namespace servicelib::dataquery::s3

#pragma once

#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <servicelib/dataquery/endpoint.hpp>
#include <servicelib/dataquery/s3/list_page.hpp>
#include <servicelib/dataquery/s3/connector.hpp>
#include <userver/s3api/clients/s3api.hpp>

namespace servicelib::dataquery::s3 {
using Client = userver::s3api::Client;

struct GetObjectRequest final {
  std::string key;
  std::optional<std::string> version;
  Client::HeaderDataRequest headers;
};
struct GetObjectResult final {
  std::string body;
  Client::HeadersDataResponse headers;
};
struct PutObjectRequest final {
  std::string key;
  std::string body;
  std::optional<Client::Meta> metadata;
  std::string contentType{"application/octet-stream"};
  std::optional<std::string> contentDisposition;
  std::optional<std::vector<Client::Tag>> tags;
};
struct DeleteObjectRequest final { std::string key; };
struct DeleteObjectResult final {};

namespace detail {
inline void checkContext(const MessageContext& context) {
  if (context.cancelled()) throw OperationCancelledError("S3 Query cancelled");
  userver::engine::current_task::CancellationPoint();
}
inline userver::s3api::ClientPtr requireClient(userver::s3api::ClientPtr client) {
  if (!client) throw std::invalid_argument("S3 Query requires a native client");
  return client;
}
}  // namespace detail

// Native userver operations run in Endpoint's joined request task. The maker
// must configure ConnectionCfg with a single attempt: no replay is added here.
// Responses are borrowed by handleResult and released before EndRequest.
class GetObject final {
 public:
  explicit GetObject(userver::s3api::ClientPtr client)
      : client_(detail::requireClient(std::move(client))) {}
  template <typename State, typename T, typename Handler, typename R>
  void execute(MessageContext context, State& state, const T& input,
               Handler& handler, Collector<R>& out) {
    detail::checkContext(context);
    GetObjectRequest request = handler.buildRequest(context, state, input);
    detail::checkContext(context);
    GetObjectResult result;
    // GetObject catches all std::exceptions and returns nullopt. TryGetObject
    // preserves native 404 versus authentication/transport errors. Empty body
    // is successful data and must not be interpreted as absence.
    result.body = client_->TryGetObject(request.key, request.version,
                                        &result.headers, request.headers);
    detail::checkContext(context);
    handler.handleResult(context, state, result, out);
    detail::checkContext(context);
  }
 private:
  userver::s3api::ClientPtr client_;
};

class PutObject final {
 public:
  explicit PutObject(userver::s3api::ClientPtr client)
      : client_(detail::requireClient(std::move(client))) {}
  template <typename State, typename T, typename Handler, typename R>
  void execute(MessageContext context, State& state, const T& input,
               Handler& handler, Collector<R>& out) {
    detail::checkContext(context);
    PutObjectRequest request = handler.buildRequest(context, state, input);
    detail::checkContext(context);
    auto result = client_->PutObject(request.key, std::move(request.body), request.metadata,
                                    request.contentType, request.contentDisposition, request.tags);
    detail::checkContext(context);
    handler.handleResult(context, state, result, out);
    detail::checkContext(context);
  }
 private:
  userver::s3api::ClientPtr client_;
};

class DeleteObject final {
 public:
  explicit DeleteObject(userver::s3api::ClientPtr client)
      : client_(detail::requireClient(std::move(client))) {}
  template <typename State, typename T, typename Handler, typename R>
  void execute(MessageContext context, State& state, const T& input,
               Handler& handler, Collector<R>& out) {
    detail::checkContext(context);
    DeleteObjectRequest request = handler.buildRequest(context, state, input);
    detail::checkContext(context);
    client_->DeleteObject(request.key);
    detail::checkContext(context);
    DeleteObjectResult result;
    handler.handleResult(context, state, result, out);
    detail::checkContext(context);
  }
 private:
  userver::s3api::ClientPtr client_;
};
struct ListObjectsRequest final {
  std::string prefix;
  std::string delimiter;
  int pageSize{1000};
};

class ListObjects final {
 public:
  explicit ListObjects(userver::s3api::ClientPtr client)
      : client_(detail::requireClient(std::move(client))) {}
  template <typename State, typename T, typename Handler, typename R>
  void execute(MessageContext context, State& state, const T& input,
               Handler& handler, Collector<R>& out) {
    detail::checkContext(context);
    const ListObjectsRequest request = handler.buildRequest(context, state, input);
    if (request.pageSize < 1 || request.pageSize > 1000)
      throw std::invalid_argument("S3 list page size must be in 1..1000");
    std::string marker;
    for (;;) {
      detail::checkContext(context);
      auto page = client_->ListBucketContents(request.prefix, request.pageSize, marker,
                                              request.delimiter);
      // Native nullopt means an empty HTTP payload, not an empty object list.
      // A valid empty listing is an XML page with IsTruncated=false.
      if (!page) throw std::runtime_error("S3 listing returned an empty response");
      auto next = detail::nextListMarker(*page, marker);
      detail::checkContext(context);
      handler.handlePage(context, state, *page, out);
      detail::checkContext(context);
      if (!next) return;
      marker = std::move(*next);
    }
  }
 private:
  userver::s3api::ClientPtr client_;
};
struct HeadObjectRequest final {
  std::string key;
  std::optional<std::string> version;
};

class HeadObject final {
 public:
  HeadObject(std::shared_ptr<Connector> connector, std::string bucket)
      : connector_(std::move(connector)), bucket_(std::move(bucket)) {
    if (!connector_) throw std::invalid_argument("S3 HEAD requires a connector");
  }
  template <typename State, typename T, typename Handler, typename R>
  void execute(MessageContext context, State& state, const T& input,
               Handler& handler, Collector<R>& out) {
    detail::checkContext(context);
    const HeadObjectRequest request = handler.buildRequest(context, state, input);
    detail::checkContext(context);
    auto response = connector_->head(context, bucket_, request.key, request.version);
    detail::checkContext(context);
    handler.handleResult(context, state, *response, out);
    detail::checkContext(context);
  }
 private:
  std::shared_ptr<Connector> connector_;
  std::string bucket_;
};
}  // namespace servicelib::dataquery::s3

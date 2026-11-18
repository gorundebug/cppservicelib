#include <servicelib/pulsar/client.hpp>
#include <servicelib/pulsar/wait.hpp>
#include <userver/clients/http/request.hpp>
#include <userver/clients/http/response_future.hpp>
#include <userver/crypto/certificate.hpp>
#include <userver/crypto/private_key.hpp>
#include <userver/engine/wait_any.hpp>
#include <userver/formats/json.hpp>
#include <userver/fs/read.hpp>
namespace servicelib::pulsar {
namespace {
struct AdminError : std::runtime_error {
  unsigned status;
  explicit AdminError(unsigned code)
      : std::runtime_error("Pulsar administration HTTP " +
                           std::to_string(code)),
        status(code) {}
};
userver::formats::json::Value request(
    userver::clients::http::Client& admin,
    userver::engine::TaskProcessor& filesystem,
    config::PulsarDataConnectorConfig c, Context context, const char* method,
    std::string path, userver::formats::json::Value body = {}) {
  context = context.bounded(c.operationTimeout());
  if (context.cancelled())
    throw std::runtime_error("Pulsar administration cancelled");
  try {
    auto request = admin.CreateRequest();
    request.url(c.adminUrl + path)
        .method(userver::clients::http::HttpMethodFromString(method))
        .timeout(std::chrono::duration_cast<std::chrono::milliseconds>(
            *context.deadline() - std::chrono::steady_clock::now()))
        .retry(1, false)
        .verify(true)
        .follow_redirects(false);
    constexpr userver::http::headers::PredefinedHeader contentType{
        "Content-Type"},
        authorization{"Authorization"};
    userver::clients::http::Headers headers;
    headers[contentType] = "application/json";
    const auto& token = c.adminToken.empty() ? c.token : c.adminToken;
    if (!token.empty()) headers[authorization] = "Bearer " + token;
    request.headers(headers);
    if (!body.IsNull()) request.data(userver::formats::json::ToString(body));
    if (!c.tlsCaFile.empty()) request.ca_info(c.tlsCaFile);
    if (!c.tlsCertFile.empty())
      request.client_key_cert(
          userver::crypto::PrivateKey::LoadFromString(
              userver::fs::ReadFileContents(filesystem, c.tlsKeyFile)),
          userver::crypto::Certificate::LoadFromString(
              userver::fs::ReadFileContents(filesystem, c.tlsCertFile)));
    auto response = request.async_perform();
    auto cancelled = std::make_shared<detail::Event>();
    const auto wake = [cancelled] { cancelled->Send(); };
    std::stop_callback primary(context.stopToken(), wake);
    std::vector<std::unique_ptr<std::stop_callback<decltype(wake)>>> external;
    for (auto token : context.externalStopTokens())
      external.push_back(
          std::make_unique<std::stop_callback<decltype(wake)>>(token, wake));
    auto index = userver::engine::WaitAnyUntil(
        userver::engine::Deadline::FromTimePoint(*context.deadline()), response,
        cancelled->native());
    if (!index || *index != 0 || context.cancelled()) {
      response.Cancel();
      throw std::runtime_error("cancelled");
    }
    auto result = response.Get();
    auto status = result->status_code();
    if (status < 200 || status >= 300) throw AdminError(status);
    // Public userver 3.2 buffers the response; this bounds JSON parsing, not
    // libcurl's response allocation. No unbounded application streaming queue.
    if (result->body().size() > 4 * 1024 * 1024)
      throw std::runtime_error("response too large");
    if (result->body().empty()) return userver::formats::json::Value{};
    return userver::formats::json::FromString(result->body());
  } catch (const AdminError&) {
    throw;
  } catch (const std::exception&) {
    throw std::runtime_error("Pulsar admin request failed or cancelled");
  }
}
std::string pathEncode(std::string_view input, bool slash = false) {
  static constexpr char hex[] = "0123456789ABCDEF";
  std::string out;
  for (unsigned char ch : input) {
    if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
        (ch >= '0' && ch <= '9') || ch == '-' || ch == '.' || ch == '_' ||
        ch == '~' || (slash && ch == '/'))
      out += static_cast<char>(ch);
    else {
      out += '%';
      out += hex[ch >> 4];
      out += hex[ch & 15];
    }
  }
  return out;
}
int partitions(userver::clients::http::Client& admin,
               userver::engine::TaskProcessor& filesystem,
               config::PulsarDataConnectorConfig c, std::string path,
               Context context) {
  auto value = request(admin, filesystem, c, context, "GET",
                       path + "/partitions?checkAllowAutoCreation=false");
  auto count = value["partitions"];
  if (!count.IsInt64() || count.As<int64_t>() < 0 || count.As<int64_t>() > 1000)
    throw std::runtime_error("Invalid Pulsar partition metadata");
  const int n = static_cast<int>(count.As<int64_t>());
  if (n == 0) request(admin, filesystem, c, context, "GET", path + "/stats");
  return n;
}
}  // namespace
void EnsureResources(userver::clients::http::Client& admin,
                     userver::engine::TaskProcessor& filesystem,
                     config::PulsarDataConnectorConfig c,
                     config::PulsarEndpointConfig e, bool source,
                     Context context) {
  c.validate();
  e.validate(source);
  const auto topic =
      "/admin/v2/persistent/" + pathEncode(e.topic.substr(13), true);
  const auto ns =
      "/admin/v2/namespaces/" +
      pathEncode(e.topic.substr(13, e.topic.substr(13).rfind('/')), true);
  for (auto [policy, field] :
       {std::pair{"autoTopicCreation", "allowAutoTopicCreation"},
        std::pair{"autoSubscriptionCreation",
                  "allowAutoSubscriptionCreation"}}) {
    if (!source && std::string(policy) == "autoSubscriptionCreation") continue;
    auto value =
        request(admin, filesystem, c, context, "GET", ns + "/" + policy);
    auto flag = value[field];
    if (!flag.IsBool() || flag.As<bool>())
      throw std::runtime_error(
          "Pulsar namespace must explicitly disable auto creation");
  }
  int count = -1;
  bool missing = false;
  try {
    count = partitions(admin, filesystem, c, topic, context);
  } catch (const AdminError& error) {
    if (error.status != 404 || !e.createTopic) throw;
    missing = true;
  }
  if (missing) {
    try {
      request(admin, filesystem, c, context, "PUT",
              topic + (e.partitions ? "/partitions" : ""),
              e.partitions ? userver::formats::json::ValueBuilder(e.partitions)
                                 .ExtractValue()
                           : userver::formats::json::Value{});
    } catch (const AdminError& error) {
      if (error.status != 409) throw;
    }
    count = partitions(admin, filesystem, c, topic, context);
  }
  if (e.createTopic && count != e.partitions)
    throw std::runtime_error("Pulsar existing partition count differs");
  if (!source) return;
  auto subscriptions =
      request(admin, filesystem, c, context, "GET", topic + "/subscriptions");
  auto contains = [&] {
    if (!subscriptions.IsArray())
      throw std::runtime_error("Invalid Pulsar subscriptions response");
    for (auto v : subscriptions)
      if (v.IsString() && v.As<std::string>() == e.subscriptionName)
        return true;
    return false;
  };
  if (contains()) return;
  if (!e.createSubscription)
    throw std::runtime_error(
        "Pulsar subscription missing and creation disabled");
  const std::int64_t position =
      e.subscriptionInitialPosition == "Latest" ? INT64_MAX : -1;
  userver::formats::json::ValueBuilder cursor;
  cursor["ledgerId"] = position;
  cursor["entryId"] = position;
  cursor["partitionIndex"] = -1;
  try {
    request(admin, filesystem, c, context, "PUT",
            topic + "/subscription/" + pathEncode(e.subscriptionName),
            cursor.ExtractValue());
  } catch (const AdminError& error) {
    if (error.status != 409) throw;
  }
  subscriptions =
      request(admin, filesystem, c, context, "GET", topic + "/subscriptions");
  if (!contains())
    throw std::runtime_error("Pulsar created subscription not visible");
}
}  // namespace servicelib::pulsar

/*
 * userver HTTP datasink.
 *
 * The user handler builds a transport-neutral Request with Requester. The
 * production client adapter executes it through userver's coroutine-aware
 * HTTP client, which owns connection pooling, cancellation and tracing.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <userver/clients/http/client.hpp>
#include <userver/clients/http/request.hpp>
#include <userver/clients/http/response.hpp>
#include <userver/engine/task/cancel.hpp>
#include <userver/engine/task/current_task.hpp>
#include <userver/http/header_map.hpp>
#include <userver/utils/uuid7.hpp>

#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/datasink.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/environment/tracing/tracing.hpp>
#include <servicelib/runtime/telemetry/userver/sampling.hpp>

namespace servicelib::datasink::http {

class HttpEndpointError final : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

struct Request final {
  userver::clients::http::HttpMethod method{
      userver::clients::http::HttpMethod::kGet};
  std::string url;
  std::string body;
  userver::clients::http::Headers headers;
  std::optional<std::chrono::milliseconds> timeout;
};

class Requester final {
 public:
  Request& newRequest(std::string_view method, std::string url,
                      std::string body = {}) {
    request_.emplace();
    request_->method = userver::clients::http::HttpMethodFromString(method);
    request_->url = std::move(url);
    request_->body = std::move(body);
    return *request_;
  }

  [[nodiscard]] bool hasRequest() const noexcept {
    return request_.has_value();
  }

  Request takeRequest() {
    if (!request_) {
      throw HttpEndpointError("HTTP sink handler did not create a request");
    }
    return std::move(*request_);
  }

 private:
  std::optional<Request> request_;
};

struct Response final {
  userver::clients::http::Status status;
  std::string body;
  userver::clients::http::Headers headers;

  [[nodiscard]] bool isError() const noexcept {
    return static_cast<std::uint16_t>(status) >= 400;
  }
};

class Client {
 public:
  virtual ~Client() = default;
  virtual Response perform(Request request) = 0;
};

class UserverClient final : public Client {
 public:
  explicit UserverClient(userver::clients::http::Client& client)
      : client_(client) {}

  Response perform(Request request) override {
    auto userverRequest = client_.CreateRequest();
    userverRequest.method(request.method)
        .url(std::move(request.url))
        .data(std::move(request.body))
        .headers(request.headers);
    if (request.timeout) {
      userverRequest.timeout(*request.timeout);
    }
    auto response = userverRequest.perform();
    return Response{response->status_code(), std::move(*response).body(),
                    response->headers()};
  }

 private:
  userver::clients::http::Client& client_;
};

class IEndpoint {
 public:
  virtual ~IEndpoint() = default;
  [[nodiscard]] virtual int id() const noexcept = 0;
  virtual void start(Context context) = 0;
  virtual void stop(Context context) = 0;
};

template <typename T, typename R, typename E = std::exception_ptr>
class StreamContext final : public servicelib::SinkStreamContext<T, R, E> {
 public:
  using Base = servicelib::SinkStreamContext<T, R, E>;
  using ResultOutput = typename Base::ResultOutput;
  using ErrorOutput = typename Base::ErrorOutput;

  StreamContext(ResultOutput resultOutput, ErrorOutput errorOutput)
      : Base(std::move(resultOutput), std::move(errorOutput)) {}
};

// Handler requirements (State is Handler::State):
//   BeginResult<State> beginRequest(MessageContext, StreamContext&)
//   void consumeMessage(MessageContext, StreamContext&, State&, const T&,
//                       Requester&)
//   void handleResponse(MessageContext, StreamContext&, State&, const
//   Response&) void endRequest(MessageContext, StreamContext&,
//   std::exception_ptr,
//                   State&) noexcept
// The four methods run sequentially in the consuming userver coroutine.
template <typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class UserverEndpoint final : public IEndpoint {
 public:
  using State = typename Handler::State;
  using StreamContext = http::StreamContext<T, R, E>;

  UserverEndpoint(SinkEndpointStream<T, R, E>& stream, Client& client,
                  Handler handler)
      : environment_(stream.environment()),
        endpointId_(stream.endpointId()),
        streamName_(resolveStreamName(environment_, stream.streamConfigId())),
        endpointName_(endpointConfig(environment_, endpointId_).name),
        serviceName_(resolveServiceName(environment_)),
        client_(client),
        handler_(std::move(handler)),
        streamContext_(stream.resultOutput(), stream.errorOutput()),
        metrics_(environment_.getMetrics(), environment_.getLogger(),
                 connectorConfig(environment_, endpointId_).name,
                 endpointName_) {
    const auto& endpoint = endpointConfig();
    if (endpoint.httpMethodType != api::HTTPMethodType::kGET &&
        endpoint.httpMethodType != api::HTTPMethodType::kPOST) {
      throw std::invalid_argument("HTTP sink endpoint method is undefined");
    }
  }

  [[nodiscard]] int id() const noexcept override { return endpointId_; }
  void start([[maybe_unused]] Context context) override {}
  void stop([[maybe_unused]] Context context) override {}

  void consume(MessageContext context, Payload<T> payload) {
    auto startedSpan = startTrace(context);
    std::stop_source externalCancellation;
    context = std::move(context).withExternalCancellation(
        externalCancellation.get_token());
    std::optional<servicelib::BeginResult<State>> beginResult;
    try {
      beginResult.emplace(handler_.beginRequest(context, streamContext_));
    } catch (...) {
      const auto error = std::current_exception();
      traceError(startedSpan.span(), error, "begin_request.error");
      // Go/Python stop this message here and do not call EndRequest when
      // BeginRequest fails.
      metrics_.beginRequestFailed(tracing::ExceptionMessage(error));
      return;
    }
    tracing::SpanEvent(startedSpan.span(), "begin_request");
    auto begin = std::move(*beginResult);

    context = std::move(begin.context);
    const auto requestStreamId =
        userver::utils::generators::GenerateUuidV7();
    const auto startedAt = metrics_.requestStart();
    std::exception_ptr error;
    std::optional<Request> request;
    bool messageConsumed = false;
    try {
      Requester requester;
      handler_.consumeMessage(context, streamContext_, begin.state,
                              payload.get(), requester);
      tracing::SpanEvent(startedSpan.span(), "consume_message");
      messageConsumed = true;
      request.emplace(requester.takeRequest());
    } catch (...) {
      error = std::current_exception();
      traceError(
          startedSpan.span(), error,
          messageConsumed ? "no_request.error" : "consume_message.error");
    }
    if (!error) {
      request->headers[std::string{servicelib::kStreamIdHeader}] =
          requestStreamId;
      if (tracing::SamplingEnabled(context)) {
        request->headers[std::string{"X-Trace"}] = "1";
      }
      const auto& trace = context.trace();
      if (trace.isValid()) {
        request->headers[std::string{"traceparent"}] =
            "00-" + trace.traceId + "-" + trace.spanId +
            (tracing::SamplingEnabled(context) ? "-01" : "-00");
      }
      if (!trace.traceState.empty()) {
        request->headers[std::string{"tracestate"}] = trace.traceState;
      }
      if (!trace.baggage.empty()) {
        request->headers[std::string{"baggage"}] = trace.baggage;
      }
      std::optional<Response> response;
      try {
        telemetry::userver_adapter::SamplingScope samplingScope{
            environment_.getTracing() != nullptr,
            tracing::SamplingEnabled(context), context.trace().traceState};
        response.emplace(client_.perform(std::move(*request)));
        tracing::SpanEvent(
            startedSpan.span(), "http_call",
            {tracing::Attribute::Int64(
                "status_code", static_cast<std::uint16_t>(response->status))});
      } catch (...) {
        error = std::current_exception();
        traceError(startedSpan.span(), error, "http_call.error");
      }
      if (!error) {
        try {
          handler_.handleResponse(context, streamContext_, begin.state,
                                  *response);
          tracing::SpanEvent(startedSpan.span(), "handle_response");
        } catch (...) {
          error = std::current_exception();
          traceError(startedSpan.span(), error, "handle_response.error");
        }
      }
    }
    if (userver::engine::current_task::ShouldCancel()) {
      externalCancellation.request_stop();
    }

    userver::engine::TaskCancellationBlocker cancellationBlocker;
    try {
      handler_.endRequest(context, streamContext_, error, begin.state);
    } catch (...) {
      // EndRequest is noexcept by contract. Do not let a violation bypass
      // request metrics or tear down the consuming coroutine.
    }
    metrics_.requestEnd(startedAt, error);
  }

  [[nodiscard]] config::HttpEndpointConfig endpointConfig() const {
    return endpointConfig(environment_, endpointId_);
  }

  [[nodiscard]] config::HttpDataConnectorConfig connectorConfig() const {
    return connectorConfig(environment_, endpointId_);
  }

 private:
  [[nodiscard]] tracing::ActiveSpan startTrace(MessageContext& context) {
    if (!tracing::SamplingEnabled(context)) return {};
    auto* engine = environment_.getTracing();
    if (!engine) return {};
    auto tracer = engine->tracer(serviceName_);
    if (!tracer) return {};
    return tracing::StartSpanInPlace(
        context, tracer.get(), "http.output",
        {
            tracing::Attribute::String("stream", streamName_),
            tracing::Attribute::String("endpoint", endpointName_),
        });
  }

  static void traceError(tracing::Span* span, std::exception_ptr error,
                         std::string_view event) {
    const auto message = tracing::ExceptionMessage(error);
    tracing::SpanError(span, message);
    tracing::SpanEvent(span, event,
                       {tracing::Attribute::String("error", message)});
  }

  [[nodiscard]] static std::string resolveStreamName(
      const IServiceEnvironment& environment, std::size_t streamConfigId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    if (!runtime || streamConfigId == 0) return {};
    const auto stream = runtime->GetStreamConfigByID(
        static_cast<int>(streamConfigId));
    return stream ? stream->GetName() : std::string{};
  }

  [[nodiscard]] static std::string resolveServiceName(
      const IServiceEnvironment& environment) {
    const auto service = environment.getServiceConfigSnapshot();
    return service ? service->name : std::string{};
  }

  static config::HttpEndpointConfig endpointConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtimeConfig = environment.getRuntimeConfigSnapshot();
    if (!runtimeConfig) {
      throw std::invalid_argument("runtime config is null");
    }
    const auto endpoint = runtimeConfig->GetEndpointConfigByID(endpointId);
    const auto* httpEndpoint =
        endpoint ? endpoint->As<config::HttpEndpointConfig>() : nullptr;
    if (!httpEndpoint) {
      throw std::invalid_argument("HTTP endpoint config not found for id=" +
                                  std::to_string(endpointId));
    }
    return *httpEndpoint;
  }

  static config::HttpDataConnectorConfig connectorConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtimeConfig = environment.getRuntimeConfigSnapshot();
    if (!runtimeConfig) {
      throw std::invalid_argument("runtime config is null");
    }
    const auto endpoint = endpointConfig(environment, endpointId);
    const auto connector =
        runtimeConfig->GetDataConnectorByID(endpoint.idDataConnector);
    const auto* httpConnector =
        connector ? connector->template As<config::HttpDataConnectorConfig>()
                  : nullptr;
    if (!httpConnector) {
      throw std::invalid_argument(
          "HTTP data connector config not found for endpoint id=" +
          std::to_string(endpointId));
    }
    return *httpConnector;
  }

  IServiceEnvironment& environment_;
  int endpointId_;
  std::string streamName_;
  std::string endpointName_;
  std::string serviceName_;
  Client& client_;
  Handler handler_;
  StreamContext streamContext_;
  servicelib::DataSinkEndpointMetrics metrics_;
};

// Connector-level owner mirroring Go's netHTTPSinkDataSink. The actual HTTP
// connection pool is owned by components::HttpClient and shared by endpoints.
class UserverDataSink final {
 public:
  template <typename T, typename R, typename E>
  [[nodiscard]] static std::shared_ptr<UserverDataSink> make(
      SinkEndpointStream<T, R, E>& stream) {
    auto& environment = stream.environment();
    return std::shared_ptr<UserverDataSink>(new UserverDataSink(
        environment, connectorIdForEndpoint(environment, stream.endpointId())));
  }

  [[nodiscard]] int id() const noexcept { return connectorId_; }

  [[nodiscard]] config::HttpDataConnectorConfig config() const {
    const auto runtimeConfig = environment_.getRuntimeConfigSnapshot();
    const auto connector =
        runtimeConfig ? runtimeConfig->GetDataConnectorByID(connectorId_)
                      : std::nullopt;
    const auto* httpConnector =
        connector ? connector->template As<config::HttpDataConnectorConfig>()
                  : nullptr;
    if (!httpConnector) {
      throw std::invalid_argument("HTTP datasink config not found for id=" +
                                  std::to_string(connectorId_));
    }
    return *httpConnector;
  }

  void addEndpoint(std::shared_ptr<IEndpoint> endpoint) {
    if (!endpoint) {
      throw std::invalid_argument("HTTP datasink endpoint is null");
    }
    const auto endpointConfig =
        environment_.getRuntimeConfigSnapshot()->GetEndpointConfigByID(
            endpoint->id());
    if (!endpointConfig ||
        endpointConfig->GetIdDataConnector() != connectorId_) {
      throw std::invalid_argument(
          "HTTP datasink endpoint belongs to another connector");
    }
    if (!endpoints_.emplace(endpoint->id(), std::move(endpoint)).second) {
      throw std::invalid_argument("duplicate HTTP datasink endpoint id");
    }
  }

  [[nodiscard]] std::shared_ptr<IEndpoint> endpoint(int endpointId) const {
    const auto it = endpoints_.find(endpointId);
    return it == endpoints_.end() ? nullptr : it->second;
  }

  void start(Context context) {
    std::vector<IEndpoint*> started;
    try {
      for (const auto& [_, endpoint] : endpoints_) {
        endpoint->start(context);
        started.push_back(endpoint.get());
      }
    } catch (...) {
      for (auto it = started.rbegin(); it != started.rend(); ++it) {
        (*it)->stop(context);
      }
      throw;
    }
  }

  void stop(Context context) {
    for (const auto& [_, endpoint] : endpoints_) {
      endpoint->stop(context);
    }
  }

 private:
  UserverDataSink(IServiceEnvironment& environment, int connectorId)
      : environment_(environment), connectorId_(connectorId) {
    static_cast<void>(config());
  }

  [[nodiscard]] static int connectorIdForEndpoint(
      IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpoint =
        runtime ? runtime->GetEndpointConfigByID(endpointId) : std::nullopt;
    if (!endpoint) {
      throw std::invalid_argument("HTTP datasink endpoint config not found");
    }
    return endpoint->GetIdDataConnector();
  }

  IServiceEnvironment& environment_;
  int connectorId_;
  std::unordered_map<int, std::shared_ptr<IEndpoint>> endpoints_;
};

}  // namespace servicelib::datasink::http

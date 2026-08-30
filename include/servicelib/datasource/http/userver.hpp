/*
 * userver HTTP datasource.
 *
 * Mirrors servicelib/datasource/http/nethttp.go while leaving listener and
 * route ownership to userver components/static configuration.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <userver/components/component_context.hpp>
#include <userver/engine/single_use_event.hpp>
#include <userver/engine/task/cancel.hpp>
#include <userver/engine/task/current_task.hpp>
#include <userver/server/handlers/http_handler_base.hpp>
#include <userver/server/http/http_request.hpp>
#include <userver/server/http/http_response.hpp>
#include <userver/server/request/task_inherited_data.hpp>
#include <userver/tracing/manager.hpp>
#include <userver/tracing/span.hpp>
#include <userver/utils/uuid7.hpp>

#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/datasource.hpp>
#include <servicelib/runtime/environment/environment.hpp>

namespace servicelib {
template <typename T, typename R, typename E, typename Context>
class InputStream;
}
#include <servicelib/runtime/store/rotatingmap.hpp>

namespace servicelib::datasource::http {

inline constexpr auto kPendingRotationInterval = std::chrono::seconds{30};

class HttpRequestCancelledError final : public std::runtime_error {
 public:
  HttpRequestCancelledError() : std::runtime_error("HTTP request cancelled") {}
};

struct HandlerData final {
  const userver::server::http::HttpRequest& request;
  userver::server::http::HttpResponse& response;
  std::string responseBody;

  void setResponseBody(std::string body) { responseBody = std::move(body); }
};

template <typename HandlerState, typename ReqT, typename ResR, typename T,
          typename R, typename E>
struct PendingResult final {
  using StreamContext = servicelib::SourceStreamContext<T, R, E>;
  using Callback = std::function<bool(MessageContext, StreamContext&,
                                      HandlerState&, const R&, HandlerData&)>;

  PendingResult(HandlerState stateValue, HandlerData& handlerData,
                std::shared_ptr<tracing::Span> requestSpan)
      : state(std::move(stateValue)),
        data(handlerData),
        span(std::move(requestSpan)) {}

  HandlerState state;
  HandlerData& data;
  std::shared_ptr<tracing::Span> span;
  userver::engine::SingleUseEvent done;
  std::atomic<bool> doneSent{false};
  std::shared_mutex lifetimeMutex;
  std::mutex callbacksMutex;
  std::unordered_map<std::string, Callback> callbacks;
};

// Passed to EndpointHandler::consumeMessage. Callback returns true to remove
// itself after the invocation, matching the Go/Python contract.
template <typename HandlerState, typename ReqT, typename ResR, typename T,
          typename R, typename E>
class ResultContext final {
 public:
  using Pending = PendingResult<HandlerState, ReqT, ResR, T, R, E>;
  using Callback = typename Pending::Callback;

  explicit ResultContext(std::shared_ptr<Pending> result)
      : result_(std::move(result)) {}

  void setResultCallback(std::string messageId, Callback callback) {
    std::lock_guard lock(result_->callbacksMutex);
    result_->callbacks[std::move(messageId)] = std::move(callback);
  }

  void done() noexcept {
    tracing::SpanEvent(result_->span.get(), "done_called");
    bool expected = false;
    if (result_->doneSent.compare_exchange_strong(expected, true,
                                                  std::memory_order_acq_rel)) {
      result_->done.Send();
    }
  }

 private:
  std::shared_ptr<Pending> result_;
};

class IUserverEndpoint {
 public:
  virtual ~IUserverEndpoint() = default;
  [[nodiscard]] virtual int id() const noexcept = 0;
  virtual void start(Context context) = 0;
  virtual void stop(Context context) = 0;
  virtual std::string handle(
      const userver::server::http::HttpRequest& request) = 0;
};

// Handler requirements (State, Request and Response are nested Handler types):
//   BeginResult<State> beginRequest(MessageContext, StreamContext&,
//   HandlerData&) void consumeMessage(MessageContext, StreamContext&, State&,
//   HandlerData&,
//                       ResultContext<State, Request, Response, T, R, E>)
//   std::string getMessageId(MessageContext, StreamContext&, State&, const R&)
//   void endRequest(MessageContext, StreamContext&, std::exception_ptr,
//                   State&, HandlerData&) noexcept
// beginRequest failure is the only path on which endRequest is not called,
// exactly as in the Go and Python implementations.
template <typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class UserverEndpoint final : public IUserverEndpoint {
 public:
  using State = typename Handler::State;
  using Request = typename Handler::Request;
  using Response = typename Handler::Response;
  using StreamContext = servicelib::SourceStreamContext<T, R, E>;
  using Result = PendingResult<State, Request, Response, T, R, E>;
  using HandlerResultContext = ResultContext<State, Request, Response, T, R, E>;
  using Output = typename StreamContext::Output;
  using ErrorOutput = typename StreamContext::ErrorOutput;

  UserverEndpoint(IServiceEnvironment& environment, int endpointId,
                  Handler handler, Output output, bool hasResult,
                  ErrorOutput errorOutput = {})
      : UserverEndpoint(environment, endpointId, 0, std::move(handler),
                        std::move(output), hasResult, std::move(errorOutput)) {}

  UserverEndpoint(IServiceEnvironment& environment, int endpointId,
                  int streamConfigId, Handler handler, Output output,
                  bool hasResult, ErrorOutput errorOutput = {})
      : environment_(environment),
        endpointId_(endpointId),
        streamName_(resolveStreamName(environment, streamConfigId)),
        endpointName_(endpointConfig(environment, endpointId).name),
        method_(endpointConfig(environment, endpointId).httpMethodType),
        path_(endpointConfig(environment, endpointId).path),
        handler_(std::move(handler)),
        streamContext_(std::move(output), std::move(errorOutput)),
        hasResult_(hasResult),
        pending_(kPendingRotationInterval),
        metrics_(environment.getMetrics(), environment.getLogger(),
                 connectorConfig(environment, endpointId).name,
                 endpointName_) {
    if (method_ != api::HTTPMethodType::kGET &&
        method_ != api::HTTPMethodType::kPOST) {
      throw std::invalid_argument(
          "HTTP datasource endpoint method is undefined");
    }
    if (path_.empty()) {
      throw std::invalid_argument("HTTP datasource endpoint path is empty");
    }
  }

  [[nodiscard]] int id() const noexcept override { return endpointId_; }

  void start(Context context) override {
    if (hasResult_) {
      pending_.start(std::move(context));
    }
  }

  void stop(Context context) override {
    if (hasResult_) {
      pending_.stop(std::move(context));
    }
  }

  [[nodiscard]] config::HttpEndpointConfig endpointConfig() const {
    return endpointConfig(environment_, endpointId_);
  }

  [[nodiscard]] config::HttpDataConnectorConfig connectorConfig() const {
    return connectorConfig(environment_, endpointId_);
  }

  std::string handle(
      const userver::server::http::HttpRequest& request) override {
    auto* const tracingEngine = environment_.getTracing();
    const bool samplingRequested =
        endpointConfig().tracingEnabled ||
        !request.GetHeader("X-Trace").empty() ||
        tracing::SampledTraceParent(request.GetHeader("traceparent"));
    // userver treats a root span with no incoming trace flags as sampled.
    // Override that default before any downstream client can propagate it.
    if (tracingEngine) {
      if (auto* ambient = userver::tracing::Span::CurrentSpanUnchecked()) {
        ambient->SetSampled(samplingRequested);
      }
      userver::tracing::SetInheritedOtelTracingData(
          request.GetHeader("tracestate"), samplingRequested ? "01" : "00");
    }

    if (!methodMatches(request.GetMethod())) {
      metrics_.invalidHttpMethod(request.GetMethodStr(),
                                 request.GetRequestPath());
      request.GetHttpResponse().SetStatus(
          userver::server::http::HttpStatus::kMethodNotAllowed);
      return {};
    }

    MessageContext requestContext;
    tracing::SpanContext propagation;
    if (tracingEngine) {
      propagation.traceState = request.GetHeader("tracestate");
      propagation.baggage = request.GetHeader("baggage");
      if (!propagation.traceState.empty() || !propagation.baggage.empty()) {
        requestContext = std::move(requestContext).withTrace(propagation);
      }
    }
    std::shared_ptr<tracing::Tracer> tracer;
    // Match Go's opt-in contract. A userver root HTTP span is sampled by
    // default even when the request carries no tracing headers, so using the
    // ambient span here would make every ServiceLib request traced.
    if (samplingRequested) {
      requestContext = tracing::EnableSampling(std::move(requestContext));
      if (auto* tracing = tracingEngine) {
        tracer = tracing->tracer(environment_.getServiceName());
        if (tracer) {
          auto current = tracer->currentSpanContext();
          if (current.traceState.empty()) {
            current.traceState = propagation.traceState;
          }
          current.baggage = propagation.baggage;
          requestContext =
              std::move(requestContext).withTrace(std::move(current));
        }
      }
    }
    std::stop_source externalCancellation;
    requestContext = std::move(requestContext).withExternalCancellation(
        externalCancellation.get_token());
    const auto inheritedDeadline =
        userver::server::request::GetTaskInheritedDeadline();
    if (inheritedDeadline.IsReachable()) {
      requestContext = std::move(requestContext).withDeadline(
          inheritedDeadline.GetTimePoint());
    }
    const auto& incomingStreamId =
        request.GetHeader(servicelib::kStreamIdHeader);
    if (!incomingStreamId.empty()) {
      requestContext =
          std::move(requestContext).withStreamId(incomingStreamId);
    }
    HandlerData data{request, request.GetHttpResponse(), {}};
    tracing::ActiveSpan startedSpan;
    if (tracer) {
      startedSpan = tracing::StartSpanInPlace(
          requestContext, tracer.get(), "http.input",
          {
              tracing::Attribute::String("stream", streamName_),
              tracing::Attribute::String("endpoint", endpointName_),
              tracing::Attribute::String(
                  "method", method_ == api::HTTPMethodType::kGET
                                ? "GET"
                                : "POST"),
              tracing::Attribute::String("path", path_),
          });
    }

    std::optional<servicelib::BeginResult<State>> beginResult;
    try {
      beginResult.emplace(
          handler_.beginRequest(requestContext, streamContext_, data));
    } catch (...) {
      const auto error = std::current_exception();
      const auto message = tracing::ExceptionMessage(error);
      tracing::SpanError(startedSpan.span(), message);
      tracing::SpanEvent(startedSpan.span(), "begin_request.error",
                         {tracing::Attribute::String("error", message)});
      // As in Go/Python, BeginRequest owns its error response and is the only
      // failure path that does not call EndRequest.
      metrics_.beginRequestFailed(message);
      return std::move(data.responseBody);
    }
    tracing::SpanEvent(startedSpan.span(), "begin_request");
    auto begin = std::move(*beginResult);

    auto context = std::move(begin.context);
    if (context.streamId().empty()) {
      context = std::move(context).withStreamId(
          userver::utils::generators::GenerateUuidV7());
    }
    const std::string streamId{context.streamId()};
    if (startedSpan.span()) {
      tracing::SpanAttrs(startedSpan.span(),
                         {tracing::Attribute::String("stream_id", streamId),
                          tracing::Attribute::Bool("has_result", hasResult_)});
    }
    auto result = std::make_shared<Result>(std::move(begin.state), data,
                                           startedSpan.sharedSpan());
    const auto startedAt = metrics_.requestStart();

    std::exception_ptr error;
    bool pendingInserted = false;
    bool resultWaitFailed = false;
    bool doneReceived = false;
    try {
      if (hasResult_) {
        pending_.set(streamId, result);
        pendingInserted = true;
        metrics_.pendingAdd(streamId);
      }
      try {
        handler_.consumeMessage(context, streamContext_, result->state, data,
                                HandlerResultContext{result});
      } catch (...) {
        const auto consumeError = std::current_exception();
        const auto message = tracing::ExceptionMessage(consumeError);
        tracing::SpanError(startedSpan.span(), message);
        tracing::SpanEvent(startedSpan.span(), "consume_message.error",
                           {tracing::Attribute::String("error", message)});
        std::rethrow_exception(consumeError);
      }
      tracing::SpanEvent(startedSpan.span(), "consume_message");
      if (hasResult_) {
        try {
          result->done.Wait();
          doneReceived = true;
        } catch (...) {
          resultWaitFailed = true;
          throw;
        }
      }
    } catch (...) {
      if (userver::engine::current_task::ShouldCancel()) {
        externalCancellation.request_stop();
      }
      error = std::current_exception();
      if (!resultWaitFailed) {
        tracing::SpanError(startedSpan.span(),
                           tracing::ExceptionMessage(error));
      }
    }

    userver::engine::TaskCancellationBlocker cancellationBlocker;

    if (hasResult_) {
      // Prevent callbacks that already obtained the shared state from touching
      // HandlerData after this request coroutine returns.
      std::unique_lock lifetimeLock(result->lifetimeMutex);
      if (pendingInserted) {
        static_cast<void>(pending_.pop(streamId));
        metrics_.pendingRemove(streamId);
      }
      // Generated endpoint callbacks retain ResultContext so they can signal
      // completion. ResultContext in turn owns this PendingResult. A normal
      // response erases its callback in consumeResult(), but cancellation can
      // leave callbacks registered and therefore form a self-owning cycle.
      // No consumeResult() callback can be active while lifetimeMutex is held
      // exclusively, so this is the request boundary at which all remaining
      // callbacks must be released.
      {
        std::lock_guard callbacksLock(result->callbacksMutex);
        result->callbacks.clear();
      }
      if (resultWaitFailed && result->doneSent.load(std::memory_order_acquire)) {
        error = nullptr;
        doneReceived = true;
      } else if (resultWaitFailed) {
        const auto message = tracing::ExceptionMessage(error);
        tracing::SpanError(startedSpan.span(), message);
        tracing::SpanEvent(startedSpan.span(), "context_cancelled",
                           {tracing::Attribute::String("error", message)});
      }
      if (!result->done.IsReady() && !error) {
        error = std::make_exception_ptr(HttpRequestCancelledError{});
      }
      if (doneReceived) {
        tracing::SpanEvent(startedSpan.span(), "done_received");
      }
      callEndRequest(context, error, *result, data);
    } else {
      callEndRequest(context, error, *result, data);
    }

    // Go's net/http cancels Request.Context when ServeHTTP returns. Mirror
    // that lifecycle after EndRequest has observed the still-live request
    // context, so losing asynchronous branches (notably a soft Delay) are
    // expedited and suppress their downstream output.
    externalCancellation.request_stop();

    metrics_.requestEnd(startedAt, error);
    return std::move(data.responseBody);
  }

  void consumeResult(MessageContext context, Payload<R> payload) {
    if (context.streamId().empty()) {
      metrics_.missingStreamId();
      return;
    }
    const std::string streamId{context.streamId()};
    auto found = pending_.get(streamId);
    if (!found) {
      metrics_.lateResult(streamId);
      return;
    }
    auto result = *found;
    std::shared_lock lifetimeLock(result->lifetimeMutex);
    const auto current = pending_.get(streamId);
    if (!current || *current != result) {
      metrics_.lateResult(streamId);
      tracing::SpanEvent(result->span.get(), "late_result");
      return;
    }

    const std::string messageId = handler_.getMessageId(
        context, streamContext_, result->state, payload.get());
    typename Result::Callback callback;
    {
      std::lock_guard callbacksLock(result->callbacksMutex);
      const auto it = result->callbacks.find(messageId);
      if (it != result->callbacks.end()) callback = it->second;
    }
    if (!callback) {
      metrics_.unknownMessageId(streamId, messageId);
      tracing::SpanEvent(result->span.get(), "unknown_message_id",
                         {tracing::Attribute::String("message_id", messageId)});
      return;
    }

    if (callback(context, streamContext_, result->state, payload.get(),
                 result->data)) {
      bool duplicate = false;
      {
        std::lock_guard callbacksLock(result->callbacksMutex);
        duplicate = result->callbacks.erase(messageId) == 0;
      }
      if (duplicate) {
        metrics_.duplicateMessageId(streamId, messageId);
        tracing::SpanEvent(
            result->span.get(), "duplicate_message_id",
            {tracing::Attribute::String("message_id", messageId)});
      }
    }
    if (result->span) {
      tracing::SpanEvent(result->span.get(), "result_consumed",
                         {tracing::Attribute::String("message_id", messageId)});
    }
  }

 private:
  bool methodMatches(userver::server::http::HttpMethod method) const noexcept {
    return (method_ == api::HTTPMethodType::kGET &&
            method == userver::server::http::HttpMethod::kGet) ||
           (method_ == api::HTTPMethodType::kPOST &&
            method == userver::server::http::HttpMethod::kPost);
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

  static std::string resolveStreamName(
      const IServiceEnvironment& environment, int streamConfigId) {
    const auto runtimeConfig = environment.getRuntimeConfigSnapshot();
    const auto stream = runtimeConfig && streamConfigId != 0
                            ? runtimeConfig->GetStreamConfigByID(streamConfigId)
                            : std::nullopt;
    return stream ? stream->GetName() : std::string{};
  }

  void callEndRequest(MessageContext context, const std::exception_ptr& error,
                      Result& result, HandlerData& data) noexcept {
    try {
      handler_.endRequest(std::move(context), streamContext_, error,
                          result.state, data);
    } catch (...) {
      // EndRequest is noexcept by contract. Keep the server coroutine alive if
      // a user implementation violates that contract.
    }
  }

  IServiceEnvironment& environment_;
  int endpointId_;
  std::string streamName_;
  std::string endpointName_;
  api::HTTPMethodType method_;
  std::string path_;
  Handler handler_;
  StreamContext streamContext_;
  bool hasResult_;
  store::RotatingMap<std::string, std::shared_ptr<Result>> pending_;
  servicelib::DataSourceEndpointMetrics metrics_;
};

// Typed boundary object matching Go's DataSourceEndpointConsumer<T,R,E>.
// It is the only place where an HTTP endpoint is wired to an InputStream:
// request values enter T, handler errors enter E, and result-stream values R
// are correlated back to the pending HTTP request.
template <typename T, typename R, typename E, typename Context,
          typename Handler>
class UserverEndpointConsumer final {
 public:
  using Input = servicelib::InputStream<T, R, E, Context>;
  using Endpoint = UserverEndpoint<T, R, Handler, E>;

  static std::shared_ptr<UserverEndpointConsumer> make(
      IServiceEnvironment& environment, Input& input,
      Handler handler) {
    auto consumer =
        std::shared_ptr<UserverEndpointConsumer>(new UserverEndpointConsumer(
            environment, input, std::move(handler)));
    if (consumer->input_.getResultStream() != nullptr) {
      consumer->bindResult();
    }
    return consumer;
  }

  void consume(MessageContext context, Payload<T> payload) {
    input_.consume(std::move(context), std::move(payload));
  }

  [[nodiscard]] Input& stream() noexcept { return input_; }
  [[nodiscard]] const Input& stream() const noexcept { return input_; }
  [[nodiscard]] const std::shared_ptr<Endpoint>& endpoint() const noexcept {
    return endpoint_;
  }

 private:
  UserverEndpointConsumer(IServiceEnvironment& environment,
                          Input& input, Handler handler)
      : input_(input),
        endpoint_(std::make_shared<Endpoint>(
            environment, input_.getEndpointId(),
            static_cast<int>(input_.getConfigId()), std::move(handler),
            [input = &input_](MessageContext context, Payload<T> payload) {
              input->consume(std::move(context), std::move(payload));
            },
            input_.getResultStream() != nullptr,
            [input = &input_](MessageContext context, Payload<E> error) {
              input->consumeError(std::move(context), std::move(error));
            })) {}

  void bindResult() {
    auto* endpointObserver = endpoint_.get();
    input_.setResultConsumer([endpointObserver](
                                  MessageContext context, Payload<R> result) {
      endpointObserver->consumeResult(std::move(context), std::move(result));
    });
  }

  Input& input_;
  std::shared_ptr<Endpoint> endpoint_;
};

// Connector-level owner, equivalent to Go's netHTTPDataSource except that the
// TCP server lifecycle belongs to components::Server. It deliberately stores
// only connector id + environment; configs are always resolved via
// RuntimeConfig, just like InputDataSource.GetConfig in Go.
class UserverDataSource final {
 public:
  template <typename Input>
  [[nodiscard]] static std::shared_ptr<UserverDataSource> make(
      IServiceEnvironment& environment, const Input& input) {
    return std::shared_ptr<UserverDataSource>(new UserverDataSource(
        environment, connectorIdForEndpoint(environment, input.getEndpointId())));
  }

 private:
  UserverDataSource(IServiceEnvironment& environment, int connectorId)
      : environment_(environment), connectorId_(connectorId) {
    const auto& connectorConfig = config();
    if (connectorConfig.useDedicatedListener) {
      throw std::invalid_argument(
          "C++/userver HTTP datasource uses the native service server; "
          "useDedicatedListener must be false");
    }
  }

  [[nodiscard]] static int connectorIdForEndpoint(
      IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpoint =
        runtime ? runtime->GetEndpointConfigByID(endpointId) : std::nullopt;
    if (!endpoint) {
      throw std::invalid_argument("HTTP datasource endpoint config not found");
    }
    return endpoint->GetIdDataConnector();
  }

 public:

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
      throw std::invalid_argument("HTTP datasource config not found for id=" +
                                  std::to_string(connectorId_));
    }
    return *httpConnector;
  }

  void addEndpoint(std::shared_ptr<IUserverEndpoint> endpoint) {
    if (!endpoint) {
      throw std::invalid_argument("HTTP datasource endpoint is null");
    }
    const auto endpointConfig =
        environment_.getRuntimeConfigSnapshot()->GetEndpointConfigByID(
            endpoint->id());
    if (!endpointConfig ||
        endpointConfig->GetIdDataConnector() != connectorId_) {
      throw std::invalid_argument(
          "HTTP datasource endpoint belongs to another connector");
    }
    if (!endpoints_.emplace(endpoint->id(), std::move(endpoint)).second) {
      throw std::invalid_argument("duplicate HTTP datasource endpoint id");
    }
  }

  [[nodiscard]] std::shared_ptr<IUserverEndpoint> endpoint(
      int endpointId) const {
    const auto it = endpoints_.find(endpointId);
    return it == endpoints_.end() ? nullptr : it->second;
  }

  void start(Context context) {
    std::vector<IUserverEndpoint*> started;
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
  IServiceEnvironment& environment_;
  int connectorId_;
  std::unordered_map<int, std::shared_ptr<IUserverEndpoint>> endpoints_;
};

// Generated handlers derive from this base and obtain IUserverEndpoint from
// their generated service component. Route/path/method stay in userver static
// config, which is the native userver equivalent of Go's mux registration.
class UserverHandlerBase : public userver::server::handlers::HttpHandlerBase {
 public:
  UserverHandlerBase(
      const userver::components::ComponentConfig& config,
      const userver::components::ComponentContext& componentContext,
      std::shared_ptr<IUserverEndpoint> endpoint, bool isMonitor = false)
      : HttpHandlerBase(config, componentContext, isMonitor),
        endpoint_(std::move(endpoint)) {
    if (!endpoint_) {
      throw std::invalid_argument("HTTP datasource endpoint is null");
    }
  }

 protected:
  std::string HandleRequestThrow(
      const userver::server::http::HttpRequest& request,
      userver::server::request::RequestContext&) const override {
    return endpoint_->handle(request);
  }

 private:
  std::shared_ptr<IUserverEndpoint> endpoint_;
};

// Convenience base for generated handler components. Provider is the
// generated runtime userver component and must expose:
//   std::shared_ptr<IUserverEndpoint> httpDataSourceEndpoint(int endpointId)
// A generated concrete class only needs kName and `using Base::Base`.
// Native userver attaches HTTP handlers to the service server component.
template <typename Provider, int EndpointId>
class UserverHandlerComponentBase : public UserverHandlerBase {
 public:
  UserverHandlerComponentBase(
      const userver::components::ComponentConfig& config,
      const userver::components::ComponentContext& componentContext,
      bool isMonitor = false)
      : UserverHandlerBase(config, componentContext,
                           componentContext.template FindComponent<Provider>()
                               .httpDataSourceEndpoint(EndpointId),
                           isMonitor) {}
};

}  // namespace servicelib::datasource::http

#pragma once
#include <servicelib/runtime/detail/publication_metrics.hpp>
#include <servicelib/pulsar/lifecycle.hpp>
#include <servicelib/runtime/datasink.hpp>
#include <servicelib/runtime/stream_tracing.hpp>
#include <userver/engine/shared_mutex.hpp>
#include <userver/utils/async.hpp>
#include <userver/utils/uuid7.hpp>
namespace servicelib::datasink::pulsar {
using DeliveryResult = servicelib::pulsar::Delivery;
template <class R>
class SinkMessage {
 public:
  using Callback = std::function<R(const DeliveryResult&)>;
  using Output = std::function<void(MessageContext, R)>;
  using Error = std::function<void(MessageContext, std::exception_ptr)>;
  struct Request {
    std::mutex mutex;
    bool active{true};
    MessageContext context;
    std::shared_ptr<void> handlerState;
  };
  servicelib::pulsar::ProducerMessage message;
  SinkMessage(std::shared_ptr<Request> request,
              std::shared_ptr<servicelib::pulsar::Client> client,
              std::shared_ptr<servicelib::pulsar::detail::Gate> gate,
              std::shared_ptr<userver::concurrent::BackgroundTaskStorage> tasks,
              Output output, Error error,
              std::shared_ptr<servicelib::detail::PublicationMetrics> publication = {})
      : request_(std::move(request)),
        client_(std::move(client)),
        gate_(std::move(gate)),
        tasks_(std::move(tasks)),
        output_(std::move(output)),
        error_(std::move(error)), publication_(std::move(publication)) {}
  // Admission is synchronous and bounded; delivery and result run on the
  // userver application TaskProcessor. Native callbacks never invoke
  // application handlers directly.
  void send(Callback callback) {
    try {
      if (!callback)
        throw servicelib::pulsar::AdmissionRejected("Pulsar callback required");
      auto value = message;
      auto lease = admit(value);
      tasks_->AsyncDetach(
          "pulsar-delivery",
          [request = request_, client = client_, value = std::move(value),
           callback = std::move(callback), output = output_, error = error_,
           lease, publication = publication_]() mutable {
            deliver(request, client, std::move(value), std::move(callback),
                    output, error, lease, publication);
          });
    } catch (...) {
      if (publication_) publication_->observe("not-sent");
      throw;
    }
  }
  DeliveryResult sendSync() {
    auto value = message;
    std::shared_ptr<servicelib::pulsar::detail::Gate::Lease> lease;
    try {
      lease = admit(value);
    } catch (...) {
      if (publication_) publication_->observe("not-sent");
      return DeliveryResult{
          servicelib::pulsar::Outcome::NotSent, {}, std::current_exception()};
    }
    auto result = client_->publish(std::move(value), request_->context);
    if (publication_) publication_->observeOutcome(result.outcome);
    return result;
  }
  void out(R value) {
    check();
    output_(request_->context, std::move(value));
  }
  void skip(R value) { return out(std::move(value)); }
  void retire() {
    std::lock_guard lock(request_->mutex);
    request_->active = false;
  }

 private:
  void check() {
    std::lock_guard lock(request_->mutex);
    if (!request_->active || request_->context.cancelled())
      throw servicelib::pulsar::AdmissionRejected(
          "Pulsar sink request retired or cancelled");
  }
  std::shared_ptr<servicelib::pulsar::detail::Gate::Lease> admit(
      servicelib::pulsar::ProducerMessage& value) {
    std::lock_guard lock(request_->mutex);
    if (!request_->active || request_->context.cancelled())
      throw servicelib::pulsar::AdmissionRejected(
          "Pulsar sink request retired or cancelled");
    servicelib::pulsar::InjectContext(request_->context, value.properties);
    auto lease = gate_->acquire(servicelib::pulsar::MessageBytes(value));
    if (!lease)
      throw servicelib::pulsar::AdmissionRejected(
          "Pulsar sink delivery capacity exhausted");
    return lease;
  }
  static void deliver(
      std::shared_ptr<Request> request,
      std::shared_ptr<servicelib::pulsar::Client> client,
      servicelib::pulsar::ProducerMessage value, Callback callback,
      Output output, Error report,
      [[maybe_unused]] std::shared_ptr<servicelib::pulsar::detail::Gate::Lease>
          lease, std::shared_ptr<servicelib::detail::PublicationMetrics> publication) {
    std::exception_ptr error;
    try {
      auto result = client->publish(std::move(value), request->context);
      if (publication) publication->observeOutcome(result.outcome);
      auto converted = callback(result);
      if (!request->context.cancelled())
        output(request->context, std::move(converted));
    } catch (...) {
      error = std::current_exception();
    }
    if (error && !request->context.cancelled()) report(request->context, error);
  }
  std::shared_ptr<Request> request_;
  std::shared_ptr<servicelib::pulsar::Client> client_;
  std::shared_ptr<servicelib::pulsar::detail::Gate> gate_;
  std::shared_ptr<userver::concurrent::BackgroundTaskStorage> tasks_;
  Output output_;
  Error error_;
  std::shared_ptr<servicelib::detail::PublicationMetrics> publication_;
};
template <class T, class R, class Handler, class E = std::exception_ptr>
class Endpoint {
  using Gate = servicelib::pulsar::detail::Gate;
  struct Impl : std::enable_shared_from_this<Impl> {
    IServiceEnvironment& env;
    config::PulsarEndpointConfig endpoint;
    std::shared_ptr<servicelib::pulsar::Client> client;
    Handler handler;
    SinkStreamContext<T, R, E> stream;
    DataSinkEndpointMetrics metrics;
    std::shared_ptr<servicelib::detail::PublicationMetrics> publication;
    std::shared_ptr<Gate> requests, deliveries;
    std::shared_ptr<userver::concurrent::BackgroundTaskStorage> tasks =
        std::make_shared<userver::concurrent::BackgroundTaskStorage>();
    std::stop_source stop;
    std::atomic<bool> enabled{};
    Impl(SinkEndpointStream<T, R, E>& s,
         std::shared_ptr<servicelib::pulsar::Client> c, Handler h)
        : env(s.environment()),
          endpoint(
              servicelib::pulsar::detail::EndpointConfig(env, s.endpointId())),
          client(std::move(c)),
          handler(std::move(h)),
          stream(s.resultOutput(), s.errorOutput()),
          metrics(
              env.getMetrics(), env.getLogger(),
              servicelib::pulsar::detail::ConnectorConfig(env, s.endpointId())
                  .name,
              endpoint.name, "pulsar"),
          publication(std::make_shared<servicelib::detail::PublicationMetrics>(env.getMetrics(), "pulsar", servicelib::pulsar::detail::ConnectorConfig(env, s.endpointId()).name, endpoint.name)),
          requests(std::make_shared<Gate>(endpoint.publisherMaxInFlight)),
          deliveries(std::make_shared<Gate>(endpoint.publisherMaxInFlight)) {
      if (!client) throw std::invalid_argument("Pulsar client required");
    }
    void report(MessageContext context, std::exception_ptr error) {
      if constexpr (std::is_same_v<E, std::exception_ptr>)
        stream.collectError(context, error);
      else
        env.getLogger().error(
            "Pulsar sink transport failure (typed errors "
            "remain handler-owned)");
    }
  };

 public:
  Endpoint(SinkEndpointStream<T, R, E>& stream,
           std::shared_ptr<servicelib::pulsar::Client> client, Handler handler)
      : impl_(std::make_shared<Impl>(stream, std::move(client),
                                     std::move(handler))) {}
  void start(Context context) {
    auto s = impl_;
    if (!s->endpoint.enabled) return;
    s->client->start(s->endpoint, false, context);
    s->enabled = true;
  }
  void stop(Context context) {
    auto s = impl_;
    context = context.bounded(
        servicelib::pulsar::detail::ConnectorConfig(s->env, s->endpoint.id)
            .operationTimeout());
    userver::engine::TaskCancellationBlocker cancellationBlocker;
    s->enabled = false;
    s->requests->close();
    std::exception_ptr error;
    try {
      s->requests->wait(context, true);
      s->deliveries->close();
      s->deliveries->wait(context, true);
    } catch (...) {
      error = std::current_exception();
    }
    s->deliveries->close();
    s->stop.request_stop();
    try {
      s->client->stop(context);
    } catch (...) {
      if (!error) error = std::current_exception();
    }
    s->tasks->CancelAndWait();
    if (error) std::rethrow_exception(error);
  }
  void consume(MessageContext context, Payload<T> payload) {
    auto s = impl_;
    if (!s->enabled)
      throw servicelib::pulsar::AdmissionRejected(
          "Pulsar sink not started or stopping");
    auto lease = s->requests->acquire();
    if (!lease)
      throw servicelib::pulsar::AdmissionRejected(
          "Pulsar sink request capacity exhausted");
    context = context.withExternalCancellation(s->stop.get_token());
    auto id = s->handler.getStreamId(context, payload.get());
    context = std::move(context).withStreamId(std::move(id));
    tracing::ActiveSpan span;
    if (auto* engine = s->env.getTracing();
        engine && tracing::SamplingEnabled(context)) {
      auto tracer = engine->tracer(s->env.getServiceName());
      if (tracer)
        span = tracing::StartSpanInPlace(
            context, tracer.get(), "pulsar.output",
            {tracing::Attribute::String("endpoint", s->endpoint.name)});
    }
    std::optional<BeginResult<typename Handler::State>> begin;
    try {
      begin.emplace(s->handler.beginRequest(context, s->stream));
    } catch (...) {
      s->metrics.beginRequestFailed();
      return;
    }
    context =
        std::move(begin->context).withExternalCancellation(s->stop.get_token());
    auto state =
        std::make_shared<typename Handler::State>(std::move(begin->state));
    auto request = std::make_shared<typename SinkMessage<R>::Request>();
    request->context = context;
    request->handlerState = state;
    SinkMessage<R> message{request,
                           s->client,
                           s->deliveries,
                           s->tasks,
                           [s](MessageContext c, R r) {
                             return s->stream.collect(std::move(c),
                                                      std::move(r));
                           },
                           [s](MessageContext c, std::exception_ptr e) {
                             return s->report(std::move(c), e);
                           }, s->publication};
    auto at = s->metrics.requestStart();
    std::exception_ptr error;
    try {
      s->handler.consumeMessage(context, s->stream, *state, payload.get(),
                                message);
    } catch (...) {
      error = std::current_exception();
    }
    message.retire();
    userver::engine::TaskCancellationBlocker cancellationBlocker;
    try {
      s->handler.endRequest(context, s->stream, error, *state);
    } catch (...) {
    }
    s->metrics.requestEnd(at, error);
    if (error) s->report(context, error);
  }

 private:
  std::shared_ptr<Impl> impl_;
};
}  // namespace servicelib::datasink::pulsar

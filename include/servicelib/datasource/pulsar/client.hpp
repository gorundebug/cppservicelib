#pragma once
#include <servicelib/pulsar/lifecycle.hpp>
#include <servicelib/runtime/datasource.hpp>
#include <userver/engine/shared_mutex.hpp>
#include <userver/utils/async.hpp>
#include <userver/utils/uuid7.hpp>

#include <servicelib/runtime/stream_tracing.hpp>
#include <unordered_map>
namespace servicelib::datasource::pulsar {
template <class State, class T, class R, class E>
struct PendingResult {
  using Callback = std::function<bool(
      MessageContext, SourceStreamContext<T, R, E>&, State&, const R&)>;
  explicit PendingResult(State value) : state(std::move(value)) {}
  State state;
  servicelib::pulsar::detail::Event done;
  userver::engine::SharedMutex lifetime;
  std::mutex mutex;
  bool retired{}, completed{};
  std::unordered_map<std::string, std::shared_ptr<Callback>> callbacks;
};
template <class State, class T, class R, class E = std::exception_ptr>
class ResultContext {
 public:
  using Pending = PendingResult<State, T, R, E>;
  using Callback = typename Pending::Callback;
  explicit ResultContext(std::shared_ptr<Pending> value)
      : value_(std::move(value)) {}
  void setResultCallback(std::string id, Callback callback) {
    std::lock_guard lock(value_->mutex);
    if (value_->retired || value_->completed ||
        value_->callbacks.size() >= 4096 || id.size() > 4096)
      throw servicelib::pulsar::AdmissionRejected(
          "Pulsar result registration retired or full");
    if (!value_->callbacks
             .emplace(std::move(id),
                      std::make_shared<Callback>(std::move(callback)))
             .second)
      throw std::logic_error("Pulsar duplicate result registration");
  }
  // Logical completion only. Register before emitting, since emit may return a
  // result synchronously. Settlement is exclusively Message::ack/nack.
  void done() {
    std::lock_guard lock(value_->mutex);
    if (!value_->completed) {
      value_->completed = true;
      value_->done.Send();
    }
  }

 private:
  std::shared_ptr<Pending> value_;
};
template <class T, class R, class Handler, class E = std::exception_ptr>
class Endpoint
    : public std::enable_shared_from_this<Endpoint<T, R, Handler, E>> {
  using Self = Endpoint;
  using State = typename Handler::State;
  using Pending = PendingResult<State, T, R, E>;
  using Gate = servicelib::pulsar::detail::Gate;

 public:
  using StreamContext = SourceStreamContext<T, R, E>;
  using Output = typename StreamContext::Output;
  using ErrorOutput = typename StreamContext::ErrorOutput;
  template <class Input>
  static std::shared_ptr<Self> make(
      IServiceEnvironment& env, Input& input,
      std::shared_ptr<servicelib::pulsar::Client> client, Handler handler) {
    auto self = std::shared_ptr<Self>(new Self(
        env, input.getEndpointId(), std::move(client), std::move(handler),
        [p = &input](MessageContext c, Payload<T> v) {
          return p->consume(std::move(c), std::move(v));
        },
        input.getResultStream() != nullptr,
        [p = &input](MessageContext c, Payload<E> v) {
          return p->consumeError(std::move(c), std::move(v));
        }));
    if (input.getResultStream())
      input.setResultConsumer([weak = std::weak_ptr<Self>(self)](
                                  MessageContext c, Payload<R> v) -> void {
        if (auto s = weak.lock()) s->consumeResult(std::move(c), std::move(v));
      });
    return self;
  }
  // Direct construction factory is useful for applications with explicit
  // outputs.
  static std::shared_ptr<Self> make(
      IServiceEnvironment& env, int id,
      std::shared_ptr<servicelib::pulsar::Client> client, Handler handler,
      Output output, bool hasResult, ErrorOutput errors = {}) {
    return std::shared_ptr<Self>(new Self(env, id, std::move(client),
                                          std::move(handler), std::move(output),
                                          hasResult, std::move(errors)));
  }
  void start(Context context) {
    if (!endpoint_.enabled) return;
    auto n = handler_.concurrency(stream_);
    if (n < 1 || n > 4096)
      throw std::invalid_argument("Pulsar source concurrency must be 1..4096");
    if (started_.exchange(true))
      throw std::logic_error("Pulsar source already started");
    gate_ = std::make_shared<Gate>(
        std::min(static_cast<std::size_t>(n),
                 static_cast<std::size_t>(endpoint_.receiverQueueSize)));
    try {
      client_->start(endpoint_, true, context);
    } catch (...) {
      runDone_.Send();
      throw;
    }
    owner_ = context.withExternalCancellation(requestStop_.get_token());
    auto self = this->shared_from_this();
    inputTask_ = userver::utils::Async("pulsar-input", [self, context] {
      try {
        run(self,
            context.withExternalCancellation(self->inputStop_.get_token()));
      } catch (...) {
        self->runError_ = std::current_exception();
      }
      self->runDone_.Send();
    });
  }
  void stop(Context context) {
    context =
        context.bounded(servicelib::pulsar::detail::ConnectorConfig(env_, id_)
                            .operationTimeout());
    userver::engine::TaskCancellationBlocker cancellationBlocker;
    inputStop_.request_stop();
    if (gate_) gate_->close();
    std::exception_ptr error;
    try {
      if (started_) {
        runDone_.Wait(context);
        if (!runDone_.IsReady())
          throw std::runtime_error("Pulsar input shutdown expired");
        if (gate_) gate_->wait(context, true);
      }
    } catch (...) {
      error = std::current_exception();
    }
    requestStop_.request_stop();
    try {
      client_->stop(context);
    } catch (...) {
      if (!error) error = std::current_exception();
    }
    if (inputTask_.IsValid()) inputTask_.SyncCancel();
    tasks_.CancelAndWait();
    if (error) std::rethrow_exception(error);
  }
  void consumeResult(MessageContext context, Payload<R> payload) {
    auto result = context.localValue(key_);
    if (!result || context.cancelled()) {
      if (context.streamId().empty()) metrics_.missingStreamId();
      else metrics_.lateResult(context.streamId());
      return;
    }
    std::shared_lock lifetime(result->lifetime);
    {
      std::lock_guard lock(result->mutex);
      if (result->retired) { metrics_.lateResult(context.streamId()); return; }
    }
    auto id =
        handler_.getMessageId(context, stream_, result->state, payload.get());
    std::shared_ptr<typename Pending::Callback> callback;
    {
      std::lock_guard lock(result->mutex);
      auto it = result->callbacks.find(id);
      if (it == result->callbacks.end()) { metrics_.unknownMessageId(context.streamId(), id); return; }
      callback = it->second;
      result->callbacks.erase(it);
    }
    if (!(*callback)(context, stream_, result->state, payload.get())) {
      std::lock_guard lock(result->mutex);
      if (!result->retired) result->callbacks.emplace(id, callback);
    }
  }

 private:
  Endpoint(IServiceEnvironment& env, int id,
           std::shared_ptr<servicelib::pulsar::Client> client, Handler handler,
           Output output, bool hasResult, ErrorOutput errors)
      : env_(env),
        id_(id),
        endpoint_(servicelib::pulsar::detail::EndpointConfig(env, id)),
        client_(std::move(client)),
        handler_(std::move(handler)),
        stream_(std::move(output), std::move(errors)),
        hasResult_(hasResult),
        metrics_(env.getMetrics(), env.getLogger(),
                 servicelib::pulsar::detail::ConnectorConfig(env, id).name,
                 endpoint_.name, "pulsar") {
    if (!client_) throw std::invalid_argument("Pulsar client required");
    client_->setSourceMetrics(std::make_shared<servicelib::detail::SourceMetrics>(
        env.getMetrics(), "pulsar", servicelib::pulsar::detail::ConnectorConfig(env, id).name, endpoint_.name));
  }
  static void run(std::shared_ptr<Self> self, Context input) {
    std::exception_ptr error;
    try {
      while (!input.cancelled()) {
        self->gate_->wait(input);
        auto message = self->client_->receive(input);
        if (input.cancelled()) break;
        servicelib::pulsar::ProducerMessage measured;
        measured.data = message.native().getDataAsString();
        measured.key = message.native().getPartitionKey();
        measured.properties = message.native().getProperties();
        auto bytes = servicelib::pulsar::MessageBytes(measured);
        self->gate_->wait(input, false, bytes);
        auto lease = self->gate_->acquire(bytes);
        if (!lease)
          throw servicelib::pulsar::AdmissionRejected(
              "Pulsar source byte capacity exceeded");
        self->tasks_.AsyncDetach(
            "pulsar-request",
            [self, message = std::move(message), lease]() mutable {
              process(self, std::move(message), lease);
            });
      }
    } catch (...) {
      error = std::current_exception();
    }
    if (error && !input.cancelled())
      self->handler_.onError(MessageContext{self->owner_}, self->stream_,
                             error);
  }
  static void process(std::shared_ptr<Self> self,
                      servicelib::pulsar::Message message,
                      [[maybe_unused]] std::shared_ptr<Gate::Lease> lease) {
    struct Retire {
      servicelib::pulsar::Message m;
      ~Retire() { m.retire(); }
    } retire{message};
    auto context = servicelib::pulsar::ExtractContext(
        MessageContext{self->owner_}, message.native().getProperties(),
        self->env_.getTracing() != nullptr);
    context = ApplyDataSourceEndpointTracing(std::move(context), self->env_,
                                             self->id_);
    tracing::ActiveSpan span;
    auto* engine = self->env_.getTracing();
    if (engine && tracing::SamplingEnabled(context)) {
      auto tracer = engine->tracer(self->env_.getServiceName());
      if (tracer)
        span = tracing::StartSpanInPlace(
            context, tracer.get(), "pulsar.input",
            {tracing::Attribute::String("endpoint", self->endpoint_.name)});
    }
    std::optional<BeginResult<State>> begin;
    try {
      begin.emplace(self->handler_.beginRequest(context, self->stream_));
    } catch (...) {
      self->metrics_.beginRequestFailed();
      return;
    }
    context = std::move(begin->context)
                  .withExternalCancellation(self->requestStop_.get_token());
    if (context.streamId().empty())
      context = std::move(context).withStreamId(
          userver::utils::generators::GenerateUuidV7());
    auto result = std::make_shared<Pending>(std::move(begin->state));
    context = std::move(context).withLocalValue(self->key_, result);
    message.bind(context);
    auto at = self->metrics_.requestStart();
    const std::string pendingId{context.streamId()};
    if (self->hasResult_) self->metrics_.pendingAdd(pendingId);
    std::exception_ptr error;
    try {
      self->handler_.consumeMessage(context, self->stream_, result->state,
                                    message,
                                    ResultContext<State, T, R, E>{result});
      if (self->hasResult_) {
        result->done.Wait(context);
        if (!result->done.IsReady())
          throw std::runtime_error("Pulsar request completion cancelled");
      }
    } catch (...) {
      error = std::current_exception();
    }
    userver::engine::TaskCancellationBlocker cancellationBlocker;
    {
      std::unique_lock lifetime(result->lifetime);
      std::lock_guard lock(result->mutex);
      result->retired = true;
      result->callbacks.clear();
    }
    if (self->hasResult_) self->metrics_.pendingRemove(pendingId);
    try {
      self->handler_.endRequest(context, self->stream_, error, result->state);
    } catch (...) {
    }
    self->metrics_.requestEnd(at, error);
    if (error && !context.cancelled())
      self->handler_.onError(context, self->stream_, error);
  }
  IServiceEnvironment& env_;
  int id_;
  config::PulsarEndpointConfig endpoint_;
  std::shared_ptr<servicelib::pulsar::Client> client_;
  Handler handler_;
  StreamContext stream_;
  bool hasResult_;
  DataSourceEndpointMetrics metrics_;
  ContextKey<Pending> key_;
  std::shared_ptr<Gate> gate_;
  Context owner_;
  std::stop_source inputStop_, requestStop_;
  std::atomic<bool> started_{};
  servicelib::pulsar::detail::Event runDone_;
  std::exception_ptr runError_;
  userver::engine::TaskWithResult<void> inputTask_;
  userver::concurrent::BackgroundTaskStorage tasks_;
};
}  // namespace servicelib::datasource::pulsar

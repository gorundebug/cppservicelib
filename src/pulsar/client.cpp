#include <atomic>
#include <mutex>
#include <servicelib/pulsar/client.hpp>
#include <servicelib/pulsar/wait.hpp>
#include <servicelib/runtime/stream_tracing.hpp>
namespace servicelib::pulsar {
namespace {
struct Operation {
  std::mutex mutex;
  bool finished{};
  std::weak_ptr<detail::Event> waiter;
  ::pulsar::Result result{::pulsar::ResultUnknownError};
  ::pulsar::Message message;
  ::pulsar::MessageId id;
  void finish(::pulsar::Result value) {
    std::shared_ptr<detail::Event> event;
    {
      std::lock_guard lock(mutex);
      result = value;
      finished = true;
      event = waiter.lock();
    }
    if (event) event->Send();
  }
};
std::exception_ptr failure(const char* text) {
  return std::make_exception_ptr(std::runtime_error(text));
}
std::exception_ptr failure(::pulsar::Result result) {
  return failure(::pulsar::strResult(result));
}
void wait(std::shared_ptr<Operation> op, Context context) {
  auto event = std::make_shared<detail::Event>();
  {
    std::lock_guard lock(op->mutex);
    if (op->finished) return;
    if (!op->waiter.expired())
      throw std::logic_error("Pulsar concurrent operation waiter");
    op->waiter = event;
  }
  // The operation holds only a weak waiter. Repeated canceled receives cannot
  // accumulate retired SingleUseEvent registrations on a pending native read.
  event->Wait(context);
  {
    std::lock_guard lock(op->mutex);
    if (op->finished) return;
  }
  throw std::runtime_error("Pulsar operation cancelled or deadline exceeded");
}
Outcome classify(::pulsar::Result r) {
  switch (r) {
    case ::pulsar::ResultOk:
      return Outcome::Accepted;
    case ::pulsar::ResultProducerQueueIsFull:
    case ::pulsar::ResultMemoryBufferIsFull:
    case ::pulsar::ResultMessageTooBig:
    case ::pulsar::ResultInvalidMessage:
      return Outcome::NotSent;
    case ::pulsar::ResultAuthenticationError:
    case ::pulsar::ResultAuthorizationError:
    case ::pulsar::ResultTopicTerminated:
    case ::pulsar::ResultProducerFenced:
    case ::pulsar::ResultIncompatibleSchema:
    case ::pulsar::ResultNotAllowedError:
      return Outcome::Rejected;
    default:
      return Outcome::Unknown;
  }
}
}  // namespace
struct Client::State : std::enable_shared_from_this<Client::State> {
  servicelib::detail::SourceMetrics::Slot sourceMetrics;
  userver::clients::http::Client& admin;
  userver::engine::TaskProcessor& filesystem;
  config::PulsarDataConnectorConfig config;
  config::PulsarEndpointConfig endpoint;
  std::mutex mutex;
  std::shared_ptr<::pulsar::Client> client;
  ::pulsar::Producer producer;
  ::pulsar::Consumer consumer;
  bool started{}, ready{}, source{}, receiving{}, closing{};
  std::uint64_t epoch{};
  std::size_t pending{}, bytes{};
  std::shared_ptr<Operation> incoming, closed;
  State(userver::clients::http::Client& a, userver::engine::TaskProcessor& fs,
        config::PulsarDataConnectorConfig c)
      : admin(a), filesystem(fs), config(std::move(c)) {
    config.validate();
  }
  std::shared_ptr<Operation> close() {
    std::shared_ptr<::pulsar::Client> native;
    std::shared_ptr<Operation> op;
    {
      std::lock_guard lock(mutex);
      if (closed) return closed;
      op = closed = std::make_shared<Operation>();
      closing = true;
      ready = false;
      ++epoch;
      native = client;
    }
    if (native) {
      auto self = shared_from_this();
      native->closeAsync([self, op](auto r) { op->finish(r); });
    } else
      op->finish(::pulsar::ResultOk);
    return op;
  }
};
struct Message::State {
  std::shared_ptr<Client::State> session;
  ::pulsar::Message message;
  std::uint64_t epoch;
  std::atomic<bool> active{true}, settled{false};
  std::mutex mutex;
  Context owner;
};
void Client::setSourceMetrics(std::shared_ptr<servicelib::detail::SourceMetrics> metrics) {
  state_->sourceMetrics.store(std::move(metrics));
}
Client::Client(userver::clients::http::Client& a,
               userver::engine::TaskProcessor& fs,
               config::PulsarDataConnectorConfig c)
    : state_(std::make_shared<State>(a, fs, std::move(c))) {}
Client::~Client() { state_->close(); }
bool Client::ready() const {
  std::lock_guard lock(state_->mutex);
  return state_->ready && !state_->closing;
}
std::size_t Client::limit() const {
  std::lock_guard lock(state_->mutex);
  return static_cast<std::size_t>(state_->source
                                      ? state_->endpoint.receiverQueueSize
                                      : state_->endpoint.publisherMaxInFlight);
}
void Client::start(config::PulsarEndpointConfig ep, bool source,
                   Context context) {
  auto state = state_;
  ep.validate(source);
  {
    std::lock_guard lock(state->mutex);
    if (state->started || state->closing)
      throw std::logic_error("Pulsar session already started/stopped");
    state->started = true;
    state->source = source;
    state->endpoint = ep;
  }
  std::exception_ptr error;
  try {
    EnsureResources(state->admin, state->filesystem, state->config, ep, source,
                    context);
    if (context.cancelled())
      throw std::runtime_error("Pulsar startup cancelled");
    const auto& c = state->config;
    ::pulsar::ClientConfiguration cfg;
    // 4.2.0 HandlerBase reads the seconds getter. Preserve millisecond
    // caller deadlines while avoiding native zero-second startup timers.
    const auto nativeTimeout = std::min<std::int64_t>(
        2147483000, ((c.operationTimeout().count() + 999) / 1000) * 1000);
    cfg.setOperationTimeoutMs(static_cast<int>(nativeTimeout))
        .setConnectionTimeout(static_cast<int>(c.connectTimeout().count()))
        .setValidateHostName(true)
        .setTlsAllowInsecureConnection(false)
        .setMemoryLimit(64 * 1024 * 1024);
    if (!c.token.empty()) cfg.setAuth(::pulsar::AuthToken::create(c.token));
    if (!c.tlsCaFile.empty()) cfg.setTlsTrustCertsFilePath(c.tlsCaFile);
    if (!c.tlsCertFile.empty()) {
      cfg.setTlsCertificateFilePath(c.tlsCertFile)
          .setTlsPrivateKeyFilePath(c.tlsKeyFile);
      cfg.setAuth(::pulsar::AuthTls::create(c.tlsCertFile, c.tlsKeyFile));
    }
    std::shared_ptr<::pulsar::Client> native;
    {
      std::lock_guard lock(state->mutex);
      if (state->closing) throw std::runtime_error("Pulsar startup stopped");
      state->client = native =
          std::make_shared<::pulsar::Client>(c.servers[0], cfg);
    }
    auto op = std::make_shared<Operation>();
    if (source) {
      ::pulsar::ConsumerConfiguration cc;
      cc.setReceiverQueueSize(ep.receiverQueueSize);
      cc.setMaxTotalReceiverQueueSizeAcrossPartitions(ep.receiverQueueSize);
      cc.setConsumerType(
          ep.subscriptionType == "Shared"     ? ::pulsar::ConsumerShared
          : ep.subscriptionType == "Failover" ? ::pulsar::ConsumerFailover
                                              : ::pulsar::ConsumerExclusive);
      cc.setNegativeAckRedeliveryDelayMs(ep.negativeAckRedeliveryDelayMs);
      cc.setAckGroupingTimeMs(0);
      cc.setAckReceiptEnabled(true);
      native->subscribeAsyncV2(
          ep.topic, ep.subscriptionName, cc,
          [state,
           op](std::variant<::pulsar::Error, ::pulsar::Consumer> result) {
            if (auto e = std::get_if<::pulsar::Error>(&result)) {
              op->finish(e->result);
              return;
            }
            {
              std::lock_guard lock(state->mutex);
              state->consumer = std::get<::pulsar::Consumer>(result);
              if (!state->closing) {
                state->ready = true;
                ++state->epoch;
              }
            }
            op->finish(::pulsar::ResultOk);
          });
    } else {
      ::pulsar::ProducerConfiguration pc;
      pc.setBatchingEnabled(false)
          .setBlockIfQueueFull(false)
          .setMaxPendingMessages(ep.publisherMaxInFlight)
          .setMaxPendingMessagesAcrossPartitions(ep.publisherMaxInFlight)
          .setSendTimeout(static_cast<int>(c.operationTimeout().count()));
      native->createProducerAsyncV2(
          ep.topic, pc,
          [state,
           op](std::variant<::pulsar::Error, ::pulsar::Producer> result) {
            if (auto e = std::get_if<::pulsar::Error>(&result)) {
              op->finish(e->result);
              return;
            }
            {
              std::lock_guard lock(state->mutex);
              state->producer = std::get<::pulsar::Producer>(result);
              if (!state->closing) {
                state->ready = true;
                ++state->epoch;
              }
            }
            op->finish(::pulsar::ResultOk);
          });
    }
    wait(op, context.bounded(c.operationTimeout()));
    if (op->result != ::pulsar::ResultOk)
      std::rethrow_exception(failure(op->result));
  } catch (...) {
    error = std::current_exception();
  }
  if (error) {
    state->close();
    std::rethrow_exception(error);
  }
}
Message Client::receive(Context context) {
  auto state = state_;
  std::shared_ptr<Operation> op;
  ::pulsar::Consumer native;
  bool launch = false;
  std::uint64_t epoch;
  {
    std::lock_guard lock(state->mutex);
    if (!state->ready || state->closing || !state->source || state->receiving)
      throw std::runtime_error("Pulsar receive unavailable");
    state->receiving = true;
    epoch = state->epoch;
    native = state->consumer;
    if (!state->incoming) {
      state->incoming = std::make_shared<Operation>();
      launch = true;
    }
    op = state->incoming;
  }
  struct Release {
    std::shared_ptr<State> s;
    ~Release() {
      std::lock_guard l(s->mutex);
      s->receiving = false;
    }
  } release{state};
  if (launch)
    native.receiveAsync([op, state](auto r, const auto& message) {
      op->message = message;
      op->finish(r);
    });
  wait(op, context);
  {
    std::lock_guard lock(state->mutex);
    state->incoming.reset();
  }
  if (op->result != ::pulsar::ResultOk)
    std::rethrow_exception(failure(op->result));
  auto value = std::make_shared<Message::State>();
  value->session = state;
  value->message = op->message;
  value->epoch = epoch;
  value->owner = context;
  servicelib::detail::SourceMetrics::Observe(state->sourceMetrics, "received");
  if (value->message.getRedeliveryCount() > 0)
    servicelib::detail::SourceMetrics::Observe(state->sourceMetrics, "redelivered");
  return Message{value};
}
const ::pulsar::Message& Message::native() const { return state_->message; }
void Message::retire() const {
  std::lock_guard lock(state_->mutex);
  state_->active = false;
}
void Message::bind(Context c) const {
  std::lock_guard lock(state_->mutex);
  state_->owner = std::move(c);
}
static void settle(std::shared_ptr<Message::State> message, Context context,
                   bool nack) {
  auto state = message->session;
  std::unique_lock requestLock(message->mutex);
  {
    if (message->owner.cancelled())
      throw std::runtime_error("Pulsar request cancelled");
  }
  context = context.withExternalCancellation(message->owner.stopToken());
  for (auto token : message->owner.externalStopTokens())
    context = context.withExternalCancellation(token);
  if (message->owner.deadline() &&
      (!context.deadline() || *context.deadline() > *message->owner.deadline()))
    context = context.withDeadline(message->owner.deadline());
  if (context.cancelled() || !message->active ||
      message->settled.exchange(true))
    throw std::runtime_error("Pulsar settlement handle expired or used");
  ::pulsar::Consumer native;
  {
    std::lock_guard lock(state->mutex);
    if (state->closing || !state->ready || state->epoch != message->epoch)
      throw std::runtime_error("Pulsar settlement epoch expired");
    native = state->consumer;
  }
  if (nack) {
    native.negativeAcknowledge(message->message);
    return;
  }
  auto op = std::make_shared<Operation>();
  native.acknowledgeAsync(message->message,
                          [op](auto r) {
    op->finish(r);
  });
  requestLock.unlock();
  wait(op, context.bounded(state->config.operationTimeout()));
  if (op->result != ::pulsar::ResultOk)
    std::rethrow_exception(failure(op->result));
}
void Message::ack(Context c) const {
  return settle(state_, std::move(c), false);
}
void Message::nack(Context c) const {
  return settle(state_, std::move(c), true);
}
std::size_t MessageBytes(const ProducerMessage& m) {
  std::size_t bytes = m.data.size() + m.key.size();
  if (m.properties.size() > 4096)
    throw AdmissionRejected("Pulsar properties exceed bound");
  for (auto& [k, v] : m.properties) bytes += k.size() + v.size() + 32;
  if (bytes > kMessageBytes)
    throw AdmissionRejected("Pulsar message exceeds 1 MiB");
  return bytes;
}
Delivery Client::publish(ProducerMessage m, MessageContext context) {
  auto state = state_;
  Delivery out;
  ::pulsar::Message message;
  std::size_t bytes = 0;
  try {
    if (context.cancelled() || userver::engine::current_task::ShouldCancel())
      throw AdmissionRejected("Pulsar publication cancelled before handoff");
    InjectContext(context, m.properties);
    bytes = MessageBytes(m);
    ::pulsar::MessageBuilder builder;
    builder.setContent(m.data).setPartitionKey(m.key);
    for (auto& [k, v] : m.properties) builder.setProperty(k, v);
    if (m.eventTimestamp) builder.setEventTimestamp(*m.eventTimestamp);
    message = builder.build();
  } catch (...) {
    out.error = std::current_exception();
    return out;
  }
  ::pulsar::Producer native;
  {
    std::lock_guard lock(state->mutex);
    if (!state->ready || state->closing || state->source ||
        state->pending >= std::size_t(state->endpoint.publisherMaxInFlight) ||
        state->bytes + bytes > kPendingBytes) {
      out.error = std::make_exception_ptr(
          AdmissionRejected("Pulsar producer unavailable or full"));
      return out;
    }
    ++state->pending;
    state->bytes += bytes;
    native = state->producer;
  }
  auto op = std::make_shared<Operation>();
  try {
    native.sendAsync(message, [state, op, bytes](auto r, const auto& id) {
      {
        std::lock_guard lock(state->mutex);
        --state->pending;
        state->bytes -= bytes;
      }
      op->id = id;
      op->finish(r);
    });
  } catch (...) {
    std::lock_guard lock(state->mutex);
    --state->pending;
    state->bytes -= bytes;
    out.outcome = Outcome::Unknown;
    out.error = std::current_exception();
    return out;
  }
  try {
    wait(op, context.bounded(state->config.operationTimeout()));
    out.outcome = classify(op->result);
    out.id = op->id;
    if (op->result != ::pulsar::ResultOk) out.error = failure(op->result);
  } catch (...) {
    out.outcome = Outcome::Unknown;
    out.error = std::current_exception();
  }
  return out;
}
void Client::stop(Context context) {
  auto state = state_;
  auto op = state->close();
  wait(op, context.bounded(state->config.operationTimeout()));
  if (op->result != ::pulsar::ResultOk &&
      op->result != ::pulsar::ResultAlreadyClosed)
    std::rethrow_exception(failure(op->result));
}
MessageContext ExtractContext(MessageContext context, const Headers& rawHeaders,
                              bool tracingEnabled) {
  Headers headers;
  for (const auto& [rawKey, value] : rawHeaders) {
    auto key = rawKey;
    std::transform(key.begin(), key.end(), key.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    headers[key] = value;
  }
  if (auto it = headers.find("x-stream-id"); it != headers.end())
    context = std::move(context).withStreamId(it->second);
  if (!tracingEnabled) return context;
  if (auto it = headers.find("x-trace");
      it != headers.end() && !it->second.empty())
    context = std::move(context).withSampling(true);
  tracing::SpanContext trace;
  if (auto it = headers.find("traceparent"); it != headers.end()) {
    if (auto parsed = tracing::ParseTraceParent(it->second)) trace = *parsed;
    if (tracing::SampledTraceParent(it->second))
      context = std::move(context).withSampling(true);
  }
  if (auto it = headers.find("tracestate"); it != headers.end())
    trace.traceState = it->second;
  if (auto it = headers.find("baggage"); it != headers.end())
    trace.baggage = it->second;
  if (trace.isValid() || !trace.baggage.empty() || !trace.traceState.empty())
    context = std::move(context).withTrace(std::move(trace));
  return context;
}
void InjectContext(const MessageContext& context, Headers& headers,
                   bool tracingEnabled) {
  Headers inherited;
  if (!context.streamId().empty())
    inherited["x-stream-id"] = context.streamId();
  if (tracingEnabled) {
    const auto& trace = context.trace();
    if (trace.isValid())
      inherited["traceparent"] = "00-" + trace.traceId + "-" + trace.spanId +
                                 (context.samplingEnabled() ? "-01" : "-00");
    if (!trace.traceState.empty()) inherited["tracestate"] = trace.traceState;
    if (!trace.baggage.empty()) inherited["baggage"] = trace.baggage;
    if (context.samplingEnabled()) inherited["x-trace"] = "1";
  }
  for (auto& [key, value] : inherited) {
    for (auto it = headers.begin(); it != headers.end();) {
      auto lower = it->first;
      std::transform(lower.begin(), lower.end(), lower.begin(),
                     [](unsigned char c) { return std::tolower(c); });
      if (lower == key)
        it = headers.erase(it);
      else
        ++it;
    }
    headers[key] = value;
  }
}

}  // namespace servicelib::pulsar

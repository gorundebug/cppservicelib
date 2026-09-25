// Service-local callable graph. Results use the existing source stream.
#pragma once

#include <atomic>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <utility>
#include <vector>

#include <userver/engine/deadline.hpp>
#include <userver/engine/mutex.hpp>
#include <userver/engine/single_use_event.hpp>
#include <userver/engine/task/cancel.hpp>

#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/config/stream_types.hpp>
#include <servicelib/runtime/stream_tracing.hpp>

namespace servicelib {
namespace detail {

template <typename R>
class SubStreamCall final {
 public:
  SubStreamCall(MessageContext context,
                std::shared_ptr<SubStreamCollector<R>> collector)
      : context_(std::move(context)), collector_(std::move(collector)) {}

  void cancel() noexcept {
    if (!closed_.exchange(true, std::memory_order_acq_rel)) done_.Send();
  }

  void deliver(const R& value) {
    if (closed_.load(std::memory_order_acquire)) return;
    std::lock_guard lock(callbackMutex_);
    if (closed_.load(std::memory_order_acquire)) return;
    if (context_->cancelled()) {
      cancel();
      return;
    }
    try {
      if (collector_->out(*context_, value)) {
        // Cancellation stops new deliveries, not an admitted successful callback.
        completed_ = true;
        if (!closed_.exchange(true, std::memory_order_acq_rel)) done_.Send();
      }
    } catch (...) {
      error_ = std::current_exception();
      cancel();
    }
  }

  void wait(const Deadline& deadline) {
    if (deadline) {
      static_cast<void>(done_.WaitUntil(
          userver::engine::Deadline::FromTimePoint(*deadline)));
    } else {
      done_.Wait();
    }
    // On timeout, close before waiting for an in-flight collector.
    cancel();
    userver::engine::TaskCancellationBlocker cancellationBlocker;
    std::lock_guard lock(callbackMutex_);
    if (error_) std::rethrow_exception(error_);
    if (!completed_) throw OperationCancelledError("SubStream invocation cancelled");
  }

  void close() noexcept {
    cancel();
    // Cancellation must not interrupt draining an already running callback.
    userver::engine::TaskCancellationBlocker cancellationBlocker;
    std::lock_guard lock(callbackMutex_);
    collector_.reset();
    context_.reset();
    error_ = {};
  }

 private:
  std::atomic<bool> closed_{false};
  userver::engine::Mutex callbackMutex_;
  userver::engine::SingleUseEvent done_;
  std::optional<MessageContext> context_;
  std::shared_ptr<SubStreamCollector<R>> collector_;
  std::exception_ptr error_;
  bool completed_{false};
};

}  // namespace detail

template <typename T, typename R, typename Context>
class SubStream final : public Stream<T, StreamConsumer<T>, Context>,
                        public ISubStream<T, R> {
  using Base = Stream<T, StreamConsumer<T>, Context>;
  using Call = detail::SubStreamCall<R>;

  class ResultLink final : public StreamConsumer<R>, public StreamBase {
   public:
    explicit ResultLink(SubStream& entry) : key_(entry.key_) {
      this->copySettings(entry);
    }
    void consume(MessageContext context, Payload<R> value) override {
      if (auto call = context.localValue(key_)) call->deliver(value.get());
    }
    size_t getId() const noexcept override { return StreamBase::getId(); }
    size_t getConfigId() const noexcept override { return StreamBase::getConfigId(); }
    const std::string& getName() const noexcept override { return StreamBase::getName(); }

   private:
    bool hasConsumer() const noexcept override { return false; }
    const StreamBase& getConsumer() const override {
      throw StreamException("SubStream result link has no consumer");
    }
    StreamBase& getConsumer() override {
      throw StreamException("SubStream result link has no consumer");
    }
    const StreamBase& getBase() const noexcept override { return *this; }
    StreamBase& getBase() noexcept override { return *this; }
    const std::string_view& getType() const override {
      return StreamBuilderContext::getType<decltype(*this)>();
    }
    std::string getCode() const override { return {}; }
    size_t buildTopology(StreamBuilderContext& context, size_t id,
                         StreamBuilderContext::TIdsList* splitIds, bool) override {
      context.buildTopology(*this, id);
      if (splitIds) splitIds->push_back(id);
      if (this->getId() == 0) this->setId(id);
      return id;
    }
    void verifyTopology(StreamVerifyContext& context) const override {
      context.verify(*this);
    }
    void printTopology(TopologyPrinter& printer,
                       std::unordered_set<size_t>& visited) const override {
      if (visited.emplace(getId()).second) printer.printNode(printer.makeNode(*this));
    }
    ContextKey<Call> key_;
  };

 public:
  static std::shared_ptr<SubStream> make(
      const config::SubStreamConfig& config, IRuntimeEnvironment& environment) {
    auto entry = std::shared_ptr<SubStream>(new SubStream(config, environment));
    environment.registerStream(entry);
    return entry;
  }
  ~SubStream() override = default;

  void consume(MessageContext, Payload<T>) override {
    throw std::logic_error("SubStream invocation requires a collector");
  }

  void consume(MessageContext context, Payload<T> value,
               std::shared_ptr<SubStreamCollector<R>> collector) override {
    if (!collector) throw std::invalid_argument("SubStream collector is null");
    if (!resultSource_ || !this->hasConsumer()) {
      throw StreamException("SubStream body and result source must be configured");
    }
    if (context.cancelled()) throw OperationCancelledError("SubStream invocation cancelled");
    [[maybe_unused]] auto invocation = this->context().beginInputInvocation();
    auto call = std::make_shared<Call>(context, std::move(collector));
    struct Cleanup final {
      std::shared_ptr<Call> call;
      ~Cleanup() { call->close(); }
    } cleanup{call};
    auto cancel = [call] { call->cancel(); };
    std::stop_callback onStop(context.stopToken(), cancel);
    using StopCallback = std::stop_callback<decltype(cancel)>;
    std::vector<std::unique_ptr<StopCallback>> externalStops;
    externalStops.reserve(context.externalStopTokens().size());
    for (const auto& token : context.externalStopTokens()) {
      externalStops.emplace_back(std::make_unique<StopCallback>(token, cancel));
    }

    auto child = context.withLocalValue(key_, call);
    tracing::ActiveSpan activeSpan;
    if (this->getStreamTracer() && tracing::SamplingEnabled(child)) {
      activeSpan = tracing::StartStreamSpan(child, *this, "stream.substream");
    }
    this->context().template consume<T>(
        std::move(child), *this, *this->consumer(), std::move(value));
    call->wait(context.deadline());
  }

  template <typename Consumer, typename SourceContext>
  void setSource(Stream<R, Consumer, SourceContext>& source) {
    if (resultSource_) throw StreamException("SubStream result source is already set");
    if (static_cast<const StreamBase*>(&source) == this) {
      throw StreamException("SubStream cannot be its own result source");
    }
    if (source.getEnv() != this->getEnv()) {
      throw StreamException("SubStream result source belongs to another service");
    }
    source.setConsumer(typename StreamBase::template unique_ptr<ResultLink>(new ResultLink(*this)));
    resultSource_ = &source;
  }

 protected:
  size_t buildTopology(StreamBuilderContext& context, size_t id,
                       StreamBuilderContext::TIdsList*, bool skip) override {
    context.buildTopology(*this, id);
    return this->buildTopologyCommon(context, id, nullptr, skip);
  }

 private:
  SubStream(const config::SubStreamConfig& config, IRuntimeEnvironment& environment) {
    this->setConfigIdentity(config);
    this->resolveDefaultSerde();
    this->setEnv(&environment);
  }
  ContextKey<Call> key_;
  StreamBase* resultSource_{nullptr};
};

template <typename T, typename R, typename Context>
std::shared_ptr<SubStream<T, R, Context>> makeSubStream(
    const config::SubStreamConfig& config, IRuntimeEnvironment& environment) {
  return SubStream<T, R, Context>::make(config, environment);
}

}  // namespace servicelib

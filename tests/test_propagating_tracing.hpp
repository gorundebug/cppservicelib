#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <servicelib/runtime/environment/tracing/tracing.hpp>

// A propagation-focused double: unlike the event-recording TestTracing, every
// started span has a valid context and a distinct child span ID. Child contexts
// intentionally omit baggage/tracestate so the runtime must preserve them.
class PropagatingTestTracing final : public servicelib::tracing::Tracing {
 private:
  class Span final : public servicelib::tracing::Span {
   public:
    explicit Span(servicelib::tracing::SpanContext context)
        : context_(std::move(context)) {}
    void end() override {}
    void setAttributes(servicelib::tracing::AttributeView) override {}
    void recordError(std::string_view) override {}
    void setStatus(servicelib::tracing::StatusCode, std::string_view) override {}
    void addEvent(std::string_view, servicelib::tracing::AttributeView) override {}
    servicelib::tracing::SpanContext spanContext() const override {
      return context_;
    }

   private:
    servicelib::tracing::SpanContext context_;
  };

  class Tracer final : public servicelib::tracing::Tracer {
   public:
    explicit Tracer(const PropagatingTestTracing& owner) : owner_(owner) {}
    std::shared_ptr<servicelib::tracing::Span> start(
        std::string_view, servicelib::tracing::AttributeView) const override {
      return owner_.makeSpan({});
    }
    servicelib::tracing::SpanContext currentSpanContext() const override {
      return {};
    }
    std::shared_ptr<servicelib::tracing::Span> startChildOf(
        std::string_view, const servicelib::tracing::SpanContext& parent,
        servicelib::tracing::AttributeView) const override {
      return owner_.makeSpan(parent);
    }
    std::shared_ptr<servicelib::tracing::Span> startDetachedChildOf(
        std::string_view, const servicelib::tracing::SpanContext& parent,
        servicelib::tracing::AttributeView) const override {
      return owner_.makeSpan(parent);
    }

   private:
    const PropagatingTestTracing& owner_;
  };

  std::shared_ptr<servicelib::tracing::Span> makeSpan(
      const servicelib::tracing::SpanContext& parent) const {
    auto id = nextSpanId_.fetch_add(1, std::memory_order_relaxed);
    std::string spanId(16, '0');
    for (auto it = spanId.rbegin(); it != spanId.rend(); ++it) {
      *it = "0123456789abcdef"[id & 0xf];
      id >>= 4;
    }
    return std::make_shared<Span>(servicelib::tracing::SpanContext{
        parent.isValid() ? parent.traceId : "11111111111111111111111111111111",
        std::move(spanId), true, {}, {}});
  }

  mutable std::atomic<std::uint64_t> nextSpanId_{1};

 public:
  std::shared_ptr<servicelib::tracing::Tracer> tracer(
      std::string_view) const override {
    return std::make_shared<Tracer>(*this);
  }
};

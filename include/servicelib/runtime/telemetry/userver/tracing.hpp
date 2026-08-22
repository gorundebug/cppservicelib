/*
 * tracing.hpp
 * userver-backed implementation of servicelib::tracing::Tracing/Span
 *
 * Wraps tracing::Span (userver's own span type). OTLP export of spans is
 * not implemented here at all — userver rides spans on the same logging
 * pipeline as regular logs (see otlp::LoggerComponent), so registering
 * that component is sufficient; this adapter only needs to produce correct
 * tracing::Span objects.
 *
 * Normal tracing::Span objects must be created and destroyed within the same
 * coroutine — see runtime/tracing.hpp's currentSpanContext()/startChildOf()
 * pattern for task-pool and parallel dispatch. stream.delay uses userver's
 * explicit DetachFromCoroStack API because that logical span intentionally
 * starts before scheduling and ends in the timer coroutine.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <type_traits>
#include <variant>

#include <userver/logging/level.hpp>
#include <userver/logging/log_extra.hpp>
#include <userver/tracing/manager.hpp>
#include <userver/tracing/span.hpp>
#include <userver/tracing/span_event.hpp>

#include <servicelib/runtime/environment/tracing/tracing.hpp>

namespace servicelib::telemetry::userver_adapter {

// `logging` has no equivalent in our own `servicelib::` tree (that's
// `servicelib::log`), so unlike `tracing` below there's no name collision to
// worry about — just a missing prefix, since USERVER_NAMESPACE nests the
// real one under `::userver::logging`, not bare `::logging`.
namespace logging = ::userver::logging;

// `tracing::X` (unqualified) below means our own servicelib::tracing::X — this
// file's enclosing `servicelib` namespace has `servicelib::tracing` as a
// sibling, so plain `tracing::` already resolves there correctly. Userver's
// actual tracing types are always written `::userver::tracing::X` explicitly to
// disambiguate from that same-named sibling.
class UserverSpan final : public tracing::Span {
 public:
  explicit UserverSpan(::userver::tracing::Span&& span,
                       std::string traceState = {})
      : span_(std::move(span)), traceState_(std::move(traceState)) {}

  // Ends the span immediately by destroying the wrapped
  // ::userver::tracing::Span (which has no separate "end now" method — its
  // lifetime *is* its duration). Idempotent: calling end() twice is a no-op the
  // second time.
  void end() override {
    std::lock_guard lock(mutex_);
    span_.reset();
  }

  void setAttributes(std::initializer_list<tracing::Attribute> attrs) override {
    std::lock_guard lock(mutex_);
    if (!span_) {
      return;
    }
    for (const auto& a : attrs) {
      span_->AddTag(a.key(), toLogExtraValue(a));
    }
  }

  void recordError(std::string_view message) override {
    std::lock_guard lock(mutex_);
    if (!span_) {
      return;
    }
    span_->AddTag("error", true);
    if (!message.empty()) {
      span_->AddTag("error_message", std::string(message));
    }
  }

  void setStatus(tracing::StatusCode code,
                 std::string_view description) override {
    std::lock_guard lock(mutex_);
    if (!span_) {
      return;
    }
    if (code == tracing::StatusCode::kError) {
      // Ensures the span is written out even if below the ambient log
      // level threshold — matches userver's documented convention for
      // highlighting warning/error spans (see Span::SetLogLevel docs).
      span_->SetLogLevel(logging::Level::kError);
    }
    if (!description.empty()) {
      span_->AddTag("status_description", std::string(description));
    }
  }

  void addEvent(std::string_view name,
                std::initializer_list<tracing::Attribute> attrs) override {
    std::lock_guard lock(mutex_);
    if (!span_) {
      return;
    }
    if (attrs.size() == 0) {
      span_->AddEvent(name);
      return;
    }
    ::userver::tracing::SpanEvent::KeyValue kv;
    for (const auto& a : attrs) {
      kv.emplace(a.key(), toAnyValue(a));
    }
    span_->AddEvent(::userver::tracing::SpanEvent(name, std::move(kv)));
  }

  [[nodiscard]] tracing::SpanContext spanContext() const override {
    std::lock_guard lock(mutex_);
    if (!span_) {
      return {};
    }
    return tracing::SpanContext{
        std::string(span_->GetTraceId()), std::string(span_->GetSpanId()),
        true, traceState_, {}};
  }

 private:
  static logging::LogExtra::Value toLogExtraValue(const tracing::Attribute& a) {
    return std::visit(
        [](const auto& v) -> logging::LogExtra::Value {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, int64_t>) {
            return static_cast<long long>(v);
          } else {
            return v;
          }
        },
        a.value());
  }

  static ::userver::tracing::AnyValue toAnyValue(const tracing::Attribute& a) {
    return std::visit(
        [](const auto& v) -> ::userver::tracing::AnyValue {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, int64_t>) {
            return ::userver::tracing::AnyValue(static_cast<long long>(v));
          } else {
            return ::userver::tracing::AnyValue(v);
          }
        },
        a.value());
  }

  // Empty (nullopt) once end() has been called.
  mutable std::mutex mutex_;
  std::optional<::userver::tracing::Span> span_;
  std::string traceState_;
};

// Stateless — userver's tracing::Span has no notion of a named
// "instrumentation scope" the way OTel SDK Tracers do, so every name passed
// to Tracing::tracer() below maps to this single shared instance.
class UserverTracer final : public tracing::Tracer {
 public:
  std::shared_ptr<tracing::Span> start(
      std::string_view spanName,
      std::initializer_list<tracing::Attribute> attrs) const override {
    auto span = std::make_shared<UserverSpan>(
        ::userver::tracing::Span(std::string(spanName)),
        std::string{::userver::tracing::GetInheritedOtelTraceState()});
    span->setAttributes(attrs);
    return span;
  }

  [[nodiscard]] tracing::SpanContext currentSpanContext() const override {
    auto* current = ::userver::tracing::Span::CurrentSpanUnchecked();
    if (!current) {
      return {};
    }
    return tracing::SpanContext{
        std::string(current->GetTraceId()), std::string(current->GetSpanId()),
        current->IsSampled(),
        std::string{::userver::tracing::GetInheritedOtelTraceState()}, {}};
  }

  std::shared_ptr<tracing::Span> startChildOf(
      std::string_view spanName, const tracing::SpanContext& parent,
      std::initializer_list<tracing::Attribute> attrs) const override {
    if (!parent.valid) {
      return start(spanName, attrs);
    }
    auto span = std::make_shared<UserverSpan>(
        ::userver::tracing::Span::MakeSpan(std::string(spanName),
                                          parent.traceId, parent.spanId),
        parent.traceState);
    span->setAttributes(attrs);
    return span;
  }

  std::shared_ptr<tracing::Span> startDetachedChildOf(
      std::string_view spanName, const tracing::SpanContext& parent,
      std::initializer_list<tracing::Attribute> attrs) const override {
    auto userverSpan = ::userver::tracing::Span::MakeSpan(
        std::string(spanName), parent.valid ? parent.traceId : std::string{},
        parent.valid ? parent.spanId : std::string{});
    userverSpan.DetachFromCoroStack();
    auto span = std::make_shared<UserverSpan>(std::move(userverSpan),
                                              parent.traceState);
    span->setAttributes(attrs);
    return span;
  }

  static std::shared_ptr<tracing::Tracer> instance() {
    static const std::shared_ptr<UserverTracer> tracer =
        std::make_shared<UserverTracer>();
    return tracer;
  }
};

class UserverTracing final : public tracing::Tracing {
 public:
  std::shared_ptr<tracing::Tracer> tracer(
      std::string_view /*name*/) const override {
    return UserverTracer::instance();
  }

  static tracing::Tracing& instance() {
    static UserverTracing tracing;
    return tracing;
  }
};

}  // namespace servicelib::telemetry::userver_adapter

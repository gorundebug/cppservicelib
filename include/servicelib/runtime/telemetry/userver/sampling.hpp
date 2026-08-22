#pragma once

#include <optional>
#include <string>
#include <string_view>

#include <userver/tracing/manager.hpp>
#include <userver/tracing/span.hpp>

namespace servicelib::telemetry::userver_adapter {

// userver creates a sampled root span when a client call starts outside an
// ambient span. Put unsampled calls under an explicit unsampled parent so the
// transport propagates trace-flags=00 instead of silently starting a trace.
class SamplingScope final {
 public:
  explicit SamplingScope(bool tracingEnabled, bool sampled,
                         std::string_view traceState = {}) {
    if (!tracingEnabled) return;
    const auto* ambient = ::userver::tracing::Span::CurrentSpanUnchecked();
    // This is the normal server-request path after ServiceLib has marked the
    // userver root span unsampled. No parent, trace-state or inherited-flag
    // mutation is needed for the downstream client span.
    if (!sampled && ambient && !ambient->IsSampled() && traceState.empty()) {
      return;
    }
    if (!sampled || !traceState.empty()) {
      previousFlags_ = ::userver::tracing::GetInheritedOtelTraceFlags();
      previousTraceState_ = ::userver::tracing::GetInheritedOtelTraceState();
      restore_ = true;
      ::userver::tracing::SetInheritedOtelTracingData(std::string{traceState},
                                                      sampled ? "01" : "00");
    }
    // Server handlers already have an ambient userver span whose sampled bit
    // ServiceLib sets from the explicit opt-in headers. Reuse it instead of
    // allocating another unsampled parent for every downstream client call.
    if (!sampled && (!ambient || ambient->IsSampled())) {
      span_.emplace("servicelib.unsampled");
      span_->SetSampled(false);
    }
  }

  ~SamplingScope() {
    if (restore_) {
      ::userver::tracing::SetInheritedOtelTracingData(
          previousTraceState_,
          previousFlags_ == ::userver::tracing::OtelTraceFlags::kSampled
              ? "01"
              : "00");
    }
  }

 private:
  std::optional<::userver::tracing::Span> span_;
  bool restore_{};
  ::userver::tracing::OtelTraceFlags previousFlags_{
      ::userver::tracing::OtelTraceFlags::kNoTracing};
  std::string previousTraceState_;
};

}  // namespace servicelib::telemetry::userver_adapter

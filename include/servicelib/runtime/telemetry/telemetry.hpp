/*
 * telemetry.hpp
 * Backend-agnostic facade over the concrete telemetry engine(s).
 *
 * Go analog: runtime/telemetry/telemetry.go — thin pass-through functions
 * to runtime/telemetry/opentelemetry, so application code names this
 * facade rather than the concrete backend package. Only one backend
 * (userver) exists today; the functions below are the seam a second
 * backend would plug into without call sites changing.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <memory>

#include <userver/utils/statistics/storage.hpp>

#include <servicelib/runtime/environment/log/log.hpp>
#include <servicelib/runtime/environment/metrics/metrics.hpp>
#include <servicelib/runtime/environment/tracing/tracing.hpp>
#include <servicelib/runtime/telemetry/userver/userver.hpp>

namespace servicelib::telemetry {

inline log::Logger& createUserverLogger() {
  return userver_adapter::UserverLogger::instance();
}

inline tracing::Tracing& createUserverTracing() {
  return userver_adapter::UserverTracing::instance();
}

inline std::unique_ptr<metrics::Metrics> createUserverMetrics(
    ::userver::utils::statistics::Storage& storage) {
  return std::make_unique<userver_adapter::UserverMetrics>(storage);
}

}  // namespace servicelib::telemetry

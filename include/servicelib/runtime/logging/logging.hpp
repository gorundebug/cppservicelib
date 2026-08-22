/*
 * logging.hpp
 * Facade selecting a concrete log::Logger backend by type.
 *
 * Go analog: runtime/logging/logging.go (CreateLogsEngine). Go has two
 * independent backends behind this facade — Logrus (plain, no tracing
 * correlation) and the OTel SDK engine (runtime/telemetry/opentelemetry) —
 * because they're genuinely different client libraries. In userver there's
 * only one: LOG_*() is always the call, and OTLP export is toggled purely
 * by whether the app registers otlp::LoggerComponent — so kUserver is the
 * only case today. The enum/switch stays so a second backend (e.g. a
 * userver-independent logger for tooling that doesn't run inside the
 * coroutine engine) can be added here without touching call sites.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <stdexcept>

#include <servicelib/runtime/environment/log/log.hpp>
#include <servicelib/runtime/telemetry/telemetry.hpp>

namespace servicelib::logging {

enum class LogsEngineType {
  kUserver = 1,
};

inline log::Logger& createLogsEngine(LogsEngineType type) {
  switch (type) {
    case LogsEngineType::kUserver:
      return telemetry::createUserverLogger();
  }
  throw std::invalid_argument("unsupported logs engine");
}

}  // namespace servicelib::logging

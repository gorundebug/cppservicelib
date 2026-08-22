/*
 * log.hpp
 * userver-backed implementation of servicelib::log::Logger
 *
 * Mirrors servicelib's Go implementation
 * (runtime/telemetry/opentelemetry/opentelemetrylogging.go), but there is
 * no OTLP-specific code here at all: userver's LOG_*() macros already sit
 * behind a swappable sink (see otlp::LoggerComponent — registering it in
 * the app's component list transparently redirects LOG_*() output to an
 * OTLP collector). This adapter just bridges our engine-agnostic
 * log::Logger interface onto those macros.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <type_traits>
#include <variant>

#include <userver/logging/log.hpp>
#include <userver/logging/log_extra.hpp>

#include <servicelib/runtime/environment/log/log.hpp>

namespace servicelib::telemetry::userver_adapter {

// Keep userver types explicitly rooted outside the servicelib namespace.
namespace logging = ::userver::logging;

class UserverLogger final : public log::Logger {
 public:
  void debug(std::string_view msg,
             std::initializer_list<log::Field> fields) override {
    log(logging::Level::kDebug, msg, fields);
  }
  void info(std::string_view msg,
            std::initializer_list<log::Field> fields) override {
    log(logging::Level::kInfo, msg, fields);
  }
  void warn(std::string_view msg,
            std::initializer_list<log::Field> fields) override {
    log(logging::Level::kWarning, msg, fields);
  }
  void error(std::string_view msg,
             std::initializer_list<log::Field> fields) override {
    log(logging::Level::kError, msg, fields);
  }

  // Stateless — safe to share a single instance across the whole process.
  static log::Logger& instance() {
    static UserverLogger logger;
    return logger;
  }

 private:
  static void log(logging::Level level, std::string_view msg,
                  std::initializer_list<log::Field> fields) {
    if (!logging::ShouldLog(level)) {
      return;  // skip building LogExtra if this level is disabled
    }

    logging::LogExtra extra;
    for (const auto& f : fields) {
      std::visit(
          [&](const auto& v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, int64_t>) {
              extra.Extend(f.key(), static_cast<long long>(v));
            } else {
              extra.Extend(f.key(), v);
            }
          },
          f.value());
    }

    switch (level) {
      case logging::Level::kDebug:
        LOG_DEBUG() << msg << extra;
        break;
      case logging::Level::kInfo:
        LOG_INFO() << msg << extra;
        break;
      case logging::Level::kWarning:
        LOG_WARNING() << msg << extra;
        break;
      case logging::Level::kError:
        LOG_ERROR() << msg << extra;
        break;
      default:
        LOG_INFO() << msg << extra;
        break;
    }
  }
};

}  // namespace servicelib::telemetry::userver_adapter

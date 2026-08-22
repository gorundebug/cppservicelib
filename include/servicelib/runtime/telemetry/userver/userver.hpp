/*
 * userver.hpp
 * Single include point for the userver-backed telemetry adapter.
 *
 * Go analog: runtime/telemetry/opentelemetry (the concrete engine). See
 * ../telemetry.hpp for the backend-agnostic facade above this one — the
 * analog of runtime/telemetry/telemetry.go.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <servicelib/runtime/telemetry/userver/log.hpp>
#include <servicelib/runtime/telemetry/userver/metrics.hpp>
#include <servicelib/runtime/telemetry/userver/tracing.hpp>

#pragma once

#include <cstdlib>
#include <stdexcept>
#include <string_view>

#include <userver/engine/task/cancel.hpp>

inline void ThrowUnhandledCallbackFailure() {
  const char* mode = std::getenv("SERVICELIB_FATAL_CALLBACK_CHILD");
  if (mode && std::string_view(mode) == "cancelled-error") {
    userver::engine::current_task::RequestCancel();
  }
  const char* failureKind = std::getenv("SERVICELIB_FATAL_CALLBACK_CHILD");
  if (failureKind && failureKind[0] == 'n') {
    throw 42;
  }
  throw std::logic_error("callback-failure-process-probe");
}

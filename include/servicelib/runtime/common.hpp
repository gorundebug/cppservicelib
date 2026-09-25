#pragma once

#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <utility>

#include <userver/engine/exception.hpp>

#include <servicelib/runtime/context.hpp>
#include <servicelib/runtime/payload.hpp>

namespace servicelib {

class OperationCancelledError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

namespace detail {

// An exception escaping detached business work has no caller to receive it.
// Ordinary business failures must use their typed result/error channel.
// Do not catch (...): userver's private coroutine-unwind exception MUST reach
// its task boundary. It cannot be swallowed or mistaken for a business panic.
template <typename Function>
void invokeAsyncCallback(Function&& function) {
  try {
    std::invoke(std::forward<Function>(function));
  } catch (const OperationCancelledError&) {
    // Expected cancellation is not a process failure.
  } catch (const userver::engine::WaitInterruptedException&) {
  } catch (const userver::engine::TaskCancelledException&) {
  } catch (const std::exception& error) {
    std::fprintf(stderr, "servicelib: unhandled callback exception: %s\n", error.what());
    std::_Exit(2);
  }
}

}  // namespace detail

template <typename R>
class SubStreamCollector {
 public:
  virtual ~SubStreamCollector() = default;
  virtual bool out(MessageContext context, const R& value) = 0;
};

template <typename R>
class SubStreamCollectorFunc final : public SubStreamCollector<R> {
 public:
  using Function = std::function<bool(MessageContext, const R&)>;
  explicit SubStreamCollectorFunc(Function function) : function_(std::move(function)) {}
  bool out(MessageContext context, const R& value) override {
    return function_(std::move(context), value);
  }
 private:
  Function function_;
};

template <typename T, typename R>
class ISubStream {
 public:
  virtual ~ISubStream() = default;
  virtual void consume(MessageContext context, Payload<T> value,
                       std::shared_ptr<SubStreamCollector<R>> collector) = 0;
};

inline constexpr std::string_view kStreamIdHeader = "x-stream-id";

template <typename State>
struct BeginResult final {
  MessageContext context;
  State state;
};

template <typename F>
void bestEffortTelemetry(F&& function) noexcept {
  try {
    std::forward<F>(function)();
  } catch (...) {
    // Telemetry must never change request processing semantics.
  }
}

}  // namespace servicelib

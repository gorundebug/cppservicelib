#pragma once

#include <chrono>
#include <cstddef>
#include <concepts>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <userver/engine/async.hpp>
#include <userver/engine/task/cancel.hpp>
#include <userver/engine/task/task_with_result.hpp>

#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/environment/log/log.hpp>
#include <servicelib/runtime/environment/metrics/metrics.hpp>
#include <servicelib/runtime/payload.hpp>

namespace servicelib {
class IServiceEnvironment;

template <typename T, typename R, typename E = std::exception_ptr>
class QueryEndpointStream {
 public:
  using ValueType = T;
  using ResultType = R;
  using ErrorType = E;
  using ResultOutput = std::function<void(MessageContext, Payload<R>)>;
  using ErrorOutput = std::function<void(MessageContext, Payload<E>)>;

  virtual ~QueryEndpointStream() = default;

  [[nodiscard]] virtual IServiceEnvironment& environment() const = 0;
  [[nodiscard]] virtual int endpointId() const noexcept = 0;
  [[nodiscard]] virtual std::size_t streamConfigId() const noexcept = 0;
  virtual void collectResult(MessageContext, Payload<R>) = 0;
  virtual void collectError(MessageContext, Payload<E>) = 0;

  [[nodiscard]] ResultOutput resultOutput() {
    return [this](MessageContext context, Payload<R> result) {
      collectResult(std::move(context), std::move(result));
    };
  }

  [[nodiscard]] ErrorOutput errorOutput() {
    return [this](MessageContext context, Payload<E> error) {
      collectError(std::move(context), std::move(error));
    };
  }
};


// Non-owning construction-time reference, independent of Sink.
// Graph lifetime owns the stream; endpoint binding happens after graph creation.
template <typename T, typename R, typename E = std::exception_ptr>
class QueryEndpointStreamRef {
 public:
  QueryEndpointStreamRef& operator=(QueryEndpointStream<T, R, E>& stream) noexcept {
    stream_ = std::ref(stream);
    return *this;
  }
  [[nodiscard]] QueryEndpointStream<T, R, E>& get() const {
    if (!stream_) throw std::logic_error("query endpoint stream is not initialized");
    return stream_->get();
  }
 private:
  std::optional<std::reference_wrapper<QueryEndpointStream<T, R, E>>> stream_;
};

}  // namespace servicelib

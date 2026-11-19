#pragma once

#include <exception>
#include <servicelib/dataquery/metrics.hpp>
#include <functional>
#include <optional>
#include <utility>
#include <type_traits>
#include <servicelib/runtime/query.hpp>
#include <servicelib/dataquery/request_task.hpp>
#include <userver/engine/task/cancel.hpp>

namespace servicelib::dataquery {

// Invocation-scoped result collector; independent of Sink and its lifecycle.
template <typename Value>
class Collector final {
 public:
  using Output = std::function<void(MessageContext, Payload<Value>)>;
  explicit Collector(Output output) : output_(std::move(output)) {}
  Collector(const Collector&) = delete;
  Collector& operator=(const Collector&) = delete;
  void out(MessageContext context, const Value& value) {
    output_(std::move(context), Payload<Value>::make(value));
  }
  void out(MessageContext context, Value&& value) {
    output_(std::move(context), Payload<Value>::make(std::move(value)));
  }
 private:
  Output output_;
};

template <typename HandlerState>
struct BeginRequest final {
  MessageContext context;
  HandlerState state;
  std::exception_ptr error;
};

// Handler supplies State, beginRequest and endRequest. Operation executes native
// I/O and closes its cursor/body before returning, including when it throws.
// End receives nullptr only if beginRequest throws before returning state.
// Both collectors remain valid only through endRequest; no completion is emitted.
template <typename T, typename R, typename E, typename Handler, typename Operation>
class Endpoint final {
 public:
  using State = typename std::remove_reference_t<Handler>::State;
  Endpoint(QueryEndpointStream<T, R, E>& stream, Handler handler, Operation operation,
           std::chrono::milliseconds operationTimeout = std::chrono::milliseconds::zero())
      : stream_(stream), handler_(std::forward<Handler>(handler)), operation_(std::move(operation)), metrics_(stream.environment(), stream.endpointId()),
        operationTimeout_(operationTimeout) {
    if (operationTimeout_ < std::chrono::milliseconds::zero())
      throw std::invalid_argument("Query timeout cannot be negative");
  }

  void consume(MessageContext context, Payload<T> payload) {
    EndpointMetrics::Request observation(metrics_);
    auto output = stream_.resultOutput();
    if (metrics_.enabled()) output = [this, emit = std::move(output)](MessageContext context, Payload<R> result) {
      metrics_.result();
      emit(std::move(context), std::move(result));
    };
    Collector<R> out(std::move(output));
    Collector<E> errors(stream_.errorOutput());
    std::optional<BeginRequest<State>> begun;
    std::exception_ptr error;
    try {
      begun.emplace(handler_.beginRequest(context, stream_));
      context = begun->context;
      error = begun->error;
      if (!error) {
        if (context.cancelled()) throw OperationCancelledError("Query cancelled");
        detail::runRequestTask(context, [&] {
          operation_.execute(context, begun->state, payload.get(), handler_, out);
        }, operationTimeout_);
      }
    } catch (const std::exception&) {
      error = std::current_exception();
    } catch (...) {
      // userver cancellation uses a non-std exception that must be rethrown
      // from this catch scope. Finalize first without swallowing cancellation.
      observation.outcome(std::current_exception(), true);
      userver::engine::TaskCancellationBlocker cancellationBlocker;
      handler_.endRequest(context, stream_, begun ? &begun->state : nullptr,
                          std::current_exception(), out, errors);
      throw;
    }
    observation.outcome(error, context.cancelled());
    userver::engine::TaskCancellationBlocker cancellationBlocker;
    handler_.endRequest(context, stream_, begun ? &begun->state : nullptr,
                             error, out, errors);
  }
 private:
  QueryEndpointStream<T, R, E>& stream_;
  Handler handler_;
  Operation operation_;
  EndpointMetrics metrics_;
  std::chrono::milliseconds operationTimeout_;
};

}  // namespace servicelib::dataquery

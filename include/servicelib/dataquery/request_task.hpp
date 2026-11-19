#pragma once

#include <functional>
#include <chrono>
#include <memory>
#include <stop_token>
#include <utility>
#include <vector>

#include <servicelib/runtime/common.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/deadline.hpp>
#include <userver/engine/task/cancel.hpp>

namespace servicelib::dataquery::detail {

// Native userver clients honor their current task's cancellation. Isolate that
// cancellation from the caller (which may be a reusable graph executor). This
// task is joined in this scope, never detached, and owns no queue or retry loop.
template <typename Function>
void runRequestTask(const MessageContext& context, Function&& operation,
                    std::chrono::milliseconds timeout = std::chrono::milliseconds::zero()) {
  if (timeout < std::chrono::milliseconds::zero())
    throw std::invalid_argument("Query timeout cannot be negative");
  if (context.cancelled()) throw OperationCancelledError("Query cancelled");
  auto deadline = context.deadline();
  if (timeout > std::chrono::milliseconds::zero()) {
    const auto limit = std::chrono::steady_clock::now() + timeout;
    if (!deadline || limit < *deadline) deadline = limit;
  }
  auto task = userver::engine::CriticalAsyncNoTracing(
      [&context, &operation, deadline] {
        if (deadline) {
          userver::engine::current_task::SetDeadline(
              userver::engine::Deadline::FromTimePoint(*deadline));
        }
        if (context.cancelled()) throw OperationCancelledError("Query cancelled");
        userver::engine::current_task::CancellationPoint();
        std::invoke(operation);
        // Application mapping may finish without a native cancellation point.
        // Resources have unwound, but late cancellation is still a failed
        // request, never permission to replay an already committed write.
        userver::engine::current_task::CancellationPoint();
        if (context.cancelled() ||
            (deadline && std::chrono::steady_clock::now() >= *deadline)) {
          throw OperationCancelledError("Query cancelled");
        }
      });

  struct Cancel final {
    userver::engine::TaskCancellationToken token;
    void operator()() { token.RequestCancel(); }
  };
  using Callback = std::stop_callback<Cancel>;
  const userver::engine::TaskCancellationToken token(task);
  Callback stop(context.stopToken(), Cancel{token});
  std::vector<std::unique_ptr<Callback>> externalStops;
  externalStops.reserve(context.externalStopTokens().size());
  for (const auto& external : context.externalStopTokens()) {
    externalStops.push_back(std::make_unique<Callback>(external, Cancel{token}));
  }
  // If waiting is interrupted, Task destruction cancels and joins the child
  // before operation captures are destroyed or the caller runs EndRequest.
  task.Get();
}

}  // namespace servicelib::dataquery::detail

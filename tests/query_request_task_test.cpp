#include <array>
#include <atomic>
#include <chrono>
#include <stop_token>

#include <servicelib/dataquery/request_task.hpp>
#include <userver/engine/single_consumer_event.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/utest/utest.hpp>

namespace {
using servicelib::MessageContext;
using servicelib::dataquery::detail::runRequestTask;
using namespace std::chrono_literals;

struct Released final {
  std::atomic<bool>& value;
  ~Released() { value.store(true); }
};

UTEST(QueryRequestTask, CancellationTokensInterruptNativeWaitAndJoin) {
  for (int selected = 0; selected < 3; ++selected) {
    std::array<std::stop_source, 3> stops;
    auto context = MessageContext{}.withStopToken(stops[0].get_token())
        .withExternalCancellation(stops[1].get_token())
        .withExternalCancellation(stops[2].get_token());
    std::atomic<bool> released{false};
    userver::engine::SingleConsumerEvent entered;
    auto caller = userver::engine::AsyncNoTracing([&] {
      EXPECT_THROW(runRequestTask(context, [&] {
        Released cleanup{released};
        entered.Send();
        userver::engine::InterruptibleSleepFor(30s);
        userver::engine::current_task::CancellationPoint();
        FAIL() << "Native wait was not cancelled";
      }), userver::engine::TaskCancelledException);
      EXPECT_TRUE(released.load());
      EXPECT_FALSE(userver::engine::current_task::ShouldCancel());
      // Request cancellation must not poison a reusable caller task.
      int calls = 0;
      runRequestTask(MessageContext{}, [&] { ++calls; });
      EXPECT_EQ(calls, 1);
    });
    ASSERT_TRUE(entered.WaitForEventFor(userver::utest::kMaxTestWaitTime));
    const auto cancel_started = std::chrono::steady_clock::now();
    stops[selected].request_stop();
    caller.Get();
    EXPECT_LT(std::chrono::steady_clock::now() - cancel_started, 5s);
  }
}

UTEST(QueryRequestTask, DeadlineInterruptsNativeWait) {
  auto context = MessageContext{}.withDeadline(std::chrono::steady_clock::now() + 50ms);
  std::atomic<bool> released{false};
  EXPECT_THROW(runRequestTask(context, [&] {
    Released cleanup{released};
    userver::engine::InterruptibleSleepFor(30s);
    userver::engine::current_task::CancellationPoint();
    FAIL() << "Native wait exceeded Query deadline";
  }), userver::engine::TaskCancelledException);
  EXPECT_TRUE(released.load());
  EXPECT_FALSE(userver::engine::current_task::ShouldCancel());
}

UTEST(QueryRequestTask, PreCancelledDoesNotInvokeOperation) {
  std::stop_source stop;
  stop.request_stop();
  bool invoked = false;
  EXPECT_THROW(runRequestTask(MessageContext{}.withStopToken(stop.get_token()),
                             [&] { invoked = true; }),
               servicelib::OperationCancelledError);
  EXPECT_FALSE(invoked);
}

UTEST(QueryRequestTask, OperationTimeoutBoundsRequestWithoutCallerDeadline) {
  std::atomic<bool> released{false};
  EXPECT_THROW(runRequestTask(MessageContext{}, [&] {
    Released cleanup{released};
    userver::engine::InterruptibleSleepFor(30s);
    userver::engine::current_task::CancellationPoint();
    FAIL() << "Operation timeout did not cancel the native task";
  }, 50ms), userver::engine::TaskCancelledException);
  EXPECT_TRUE(released.load());
  EXPECT_FALSE(userver::engine::current_task::ShouldCancel());
}

UTEST(QueryRequestTask, OperationTimeoutCannotExtendRequestDeadline) {
  const auto begin = std::chrono::steady_clock::now();
  auto context = MessageContext{}.withDeadline(begin + 50ms);
  EXPECT_THROW(runRequestTask(context, [] {
    userver::engine::InterruptibleSleepFor(30s);
    userver::engine::current_task::CancellationPoint();
  }, 20s), userver::engine::TaskCancelledException);
  EXPECT_LT(std::chrono::steady_clock::now() - begin, 5s);
}

UTEST(QueryRequestTask, OrdinaryFailureReleasesResourcesAndPropagates) {
  std::atomic<bool> released{false};
  EXPECT_THROW(runRequestTask(MessageContext{}, [&] {
    Released cleanup{released};
    throw std::logic_error("mapping failed");
  }), std::logic_error);
  EXPECT_TRUE(released.load());
  EXPECT_FALSE(userver::engine::current_task::ShouldCancel());
}

UTEST(QueryRequestTask, CancellationBeforeOperationReturnsCannotReportSuccess) {
  std::stop_source stop;
  std::atomic<bool> released{false};
  int writes = 0;
  EXPECT_ANY_THROW(runRequestTask(MessageContext{}.withStopToken(stop.get_token()), [&] {
    Released cleanup{released};
    ++writes;  // A completed write must not be replayed after mapper cancellation.
    stop.request_stop();
  }));
  EXPECT_EQ(writes, 1);
  EXPECT_TRUE(released.load());
  EXPECT_FALSE(userver::engine::current_task::ShouldCancel());
}

UTEST(QueryRequestTask, LateMapperCannotReportSuccessAfterOperationTimeout) {
  std::atomic<bool> released{false};
  int writes = 0;
  EXPECT_ANY_THROW(runRequestTask(MessageContext{}, [&] {
    Released cleanup{released};
    ++writes;
    // Some application mappers do not observe cancellation themselves.
    userver::engine::SleepFor(30ms);
  }, 5ms));
  EXPECT_EQ(writes, 1);
  EXPECT_TRUE(released.load());
  EXPECT_FALSE(userver::engine::current_task::ShouldCancel());
}
}  // namespace

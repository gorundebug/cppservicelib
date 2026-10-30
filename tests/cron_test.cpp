/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

#include <stdexcept>
#include <atomic>
#include <thread>

#include <userver/utest/utest.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/sleep.hpp>

#include <servicelib/datasource/cron/libcron.hpp>
#include <servicelib/datasource/cron/local_interval.hpp>
#include "cron_interval_source_test.hpp"

UTEST(CronDataSource, LocalIntervalGrammarAndBounds) {
  using servicelib::datasource::cron::detail::ParseLocalInterval;
  EXPECT_EQ(ParseLocalInterval("@every 10ms"), std::chrono::milliseconds{10});
  EXPECT_EQ(ParseLocalInterval("@every 86400000ms"),
            std::chrono::milliseconds{86400000});
  EXPECT_FALSE(ParseLocalInterval("*/5 * * * *"));
  for (const auto expression : {"@every", "@every 9ms", "@every 86400001ms",
                               "@every 010ms", "@every +10ms", "@every 1s",
                               "@every 10.5ms", "@every  10ms",
                               "@every 10ms\n", "@every 10ms ",
                               "@every 999999999999999999ms"}) {
    EXPECT_THROW(ParseLocalInterval(expression), std::invalid_argument)
        << expression;
  }
}

UTEST(CronDataSource, LocalIntervalHasFixedSlotsAndBoundedCatchUp) {
  using servicelib::datasource::cron::detail::LocalInterval;
  using namespace std::chrono;
  const auto anchor = LocalInterval::Clock::time_point{milliseconds{1234}} +
                      microseconds{567};
  LocalInterval schedule{milliseconds{100}, anchor};
  EXPECT_EQ(schedule.delay(anchor), milliseconds{100});
  EXPECT_FALSE(schedule.takeDue(anchor + milliseconds{99}));
  const auto first = schedule.takeDue(anchor + milliseconds{100});
  ASSERT_TRUE(first);
  EXPECT_EQ(first->scheduledAt.time_since_epoch(), milliseconds{1334});
  EXPECT_FALSE(first->missed);
  const auto late = schedule.takeDue(anchor + milliseconds{1000000000});
  ASSERT_TRUE(late);
  EXPECT_TRUE(late->missed);
  EXPECT_EQ(late->scheduledAt.time_since_epoch(), milliseconds{1000001234});
  EXPECT_FALSE(schedule.takeDue(anchor + milliseconds{1000000000}));
  EXPECT_EQ(schedule.delay(anchor + milliseconds{1000000000}),
            milliseconds{100});
  EXPECT_FALSE(schedule.takeDue(anchor));  // A backward clock does not replay.
}

UTEST(CronDataSource, AdaptsPortableExpressionsToLibcron) {
  using servicelib::datasource::cron::ToLibcronExpression;
  EXPECT_EQ(ToLibcronExpression("*/5 * * * *"), "0 */5 * * * ?");
  EXPECT_EQ(ToLibcronExpression("30 8 1 * *"), "0 30 8 1 * ?");
  EXPECT_EQ(ToLibcronExpression("15 9 * * MON-FRI"),
            "0 15 9 ? * MON-FRI");
}

UTEST(CronDataSource, RejectsNonPortableExpressions) {
  using servicelib::datasource::cron::ToLibcronExpression;
  EXPECT_THROW(ToLibcronExpression("0 0 1 * MON"), std::invalid_argument);
  EXPECT_THROW(ToLibcronExpression("0 0 * *"), std::invalid_argument);
  EXPECT_THROW(ToLibcronExpression("0 0 0 * * ?"), std::invalid_argument);
}

UTEST(CronDataSource, WaitsForCorrelatedPipelineResult) {
  servicelib::datasource::cron::detail::ResultWaiter waiter;
  auto pending = waiter.begin("request-1");
  std::atomic<bool> completed{false};
  auto waiting = userver::engine::AsyncNoTracing([&] {
    waiter.wait("request-1", pending);
    completed.store(true, std::memory_order_release);
  });
  EXPECT_FALSE(completed.load(std::memory_order_acquire));
  EXPECT_EQ(waiter.complete("request-1"),
            servicelib::datasource::cron::detail::ResultWaiter::Completion::
                kCompleted);
  waiting.Get();
  EXPECT_TRUE(completed.load(std::memory_order_acquire));
}

UTEST(CronDataSource, OccurrencePreservesOwnerDeadlineSamplingAndCancellation) {
  std::stop_source stop;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
  const auto owner = servicelib::Context{}.withStopToken(stop.get_token())
                         .withDeadline(deadline).withSampling(true);
  auto context = servicelib::datasource::cron::detail::OccurrenceContext(owner)
                     .withStreamId("occurrence");
  EXPECT_EQ(context.deadline(), owner.deadline());
  EXPECT_TRUE(context.samplingEnabled());
  EXPECT_EQ(context.streamId(), "occurrence");
  EXPECT_FALSE(context.cancelled());
  bool notified = false;
  std::stop_callback callback{context.stopToken(), [&] { notified = true; }};
  stop.request_stop();
  EXPECT_TRUE(context.cancelled());
  EXPECT_TRUE(notified);
}

UTEST(CronDataSource, OccurrencePreservesEveryExternalCancellation) {
  for (int cancelled = 0; cancelled < 2; ++cancelled) {
    std::stop_source first, second;
    const auto owner = servicelib::Context{}
                           .withExternalCancellation(first.get_token())
                           .withExternalCancellation(second.get_token());
    const auto context = servicelib::datasource::cron::detail::OccurrenceContext(owner);
    EXPECT_EQ(context.externalStopTokens().size(), 2U);
    EXPECT_FALSE(context.cancelled());
    (cancelled == 0 ? first : second).request_stop();
    EXPECT_TRUE(context.cancelled());
  }
}

UTEST(CronDataSource, OccurrenceDoesNotExtendExpiredOwnerDeadline) {
  const auto owner = servicelib::Context{}.withDeadline(std::chrono::steady_clock::now());
  const auto context = servicelib::datasource::cron::detail::OccurrenceContext(owner);
  EXPECT_TRUE(context.cancelled());
  EXPECT_EQ(context.deadline(), owner.deadline());
}

UTEST(CronDataSource, DrainDeadlineLeavesActiveRunsForRetry) {
  userver::engine::Mutex mutex;
  userver::engine::ConditionVariable drained;
  std::size_t active = 1;
  EXPECT_FALSE(servicelib::datasource::cron::detail::WaitForDrain(
      servicelib::Context{}.bounded(std::chrono::milliseconds{10}), mutex, drained, active));
  EXPECT_EQ(active, 1U);
  auto retry = userver::engine::AsyncNoTracing([&] {
    return servicelib::datasource::cron::detail::WaitForDrain(
        servicelib::Context{}.bounded(std::chrono::seconds{1}), mutex, drained, active);
  });
  userver::engine::SleepFor(std::chrono::milliseconds{10});
  {
    std::lock_guard lock(mutex);
    active = 0;
  }
  drained.NotifyAll();
  EXPECT_TRUE(retry.Get());
}

UTEST(CronDataSource, CancelledDrainDoesNotClaimCompletionOfActiveRuns) {
  userver::engine::Mutex mutex;
  userver::engine::ConditionVariable drained;
  std::size_t active = 1;
  std::stop_source stop;
  stop.request_stop();
  const auto context = servicelib::Context{}.withStopToken(stop.get_token());
  EXPECT_FALSE(servicelib::datasource::cron::detail::WaitForDrain(
      context, mutex, drained, active));
  EXPECT_EQ(active, 1U);
}

UTEST(CronDataSource, CompletedDrainWinsOverExpiredWaitContext) {
  userver::engine::Mutex mutex;
  userver::engine::ConditionVariable drained;
  std::size_t active = 0;
  const auto context = servicelib::Context{}.withDeadline(std::chrono::steady_clock::now());
  EXPECT_TRUE(servicelib::datasource::cron::detail::WaitForDrain(
      context, mutex, drained, active));
}

UTEST(CronDataSource, StopTokensWakeAnAlreadyWaitingDrainFromAnOrdinaryThread) {
  for (const bool withDeadline : {false, true}) {
    for (int cancelled = 0; cancelled < 3; ++cancelled) {
      userver::engine::Mutex mutex;
      userver::engine::ConditionVariable drained;
      userver::engine::SingleConsumerEvent entered;
      std::size_t active = 1;
      std::stop_source primary, first, second;
      auto context = servicelib::Context{}.withStopToken(primary.get_token())
                         .withExternalCancellation(first.get_token())
                         .withExternalCancellation(second.get_token());
      if (withDeadline) context = context.bounded(std::chrono::hours{1});
      std::atomic<bool> finished{false};
      auto waiting = userver::engine::AsyncNoTracing([&] {
        entered.Send();
        const bool result = servicelib::datasource::cron::detail::WaitForDrain(
            context, mutex, drained, active);
        finished.store(true, std::memory_order_release);
        return result;
      });
      EXPECT_TRUE(entered.WaitForEvent());
      userver::engine::SleepFor(std::chrono::milliseconds{5});
      std::thread cancel([&] {
        if (cancelled == 0) primary.request_stop();
        else if (cancelled == 1) first.request_stop();
        else second.request_stop();
      });
      cancel.join();
      for (int attempt = 0; attempt < 20 && !finished.load(std::memory_order_acquire); ++attempt) {
        userver::engine::SleepFor(std::chrono::milliseconds{5});
      }
      EXPECT_TRUE(finished.load(std::memory_order_acquire));
      {
        std::lock_guard lock(mutex);
        EXPECT_EQ(active, 1U);
        // Also release a broken implementation, so the regression cannot hang.
        active = 0;
      }
      drained.NotifyAll();
      EXPECT_FALSE(waiting.Get());
      EXPECT_TRUE(servicelib::datasource::cron::detail::WaitForDrain(
          servicelib::Context{}, mutex, drained, active));
    }
  }
}

UTEST(CronDataSource, CancellingOneDrainWaitDoesNotCompleteAnotherWait) {
  userver::engine::Mutex mutex;
  userver::engine::ConditionVariable drained;
  std::size_t active = 1;
  std::stop_source stop;
  std::atomic<bool> cancelledFinished{false}, liveFinished{false};
  auto cancelled = userver::engine::AsyncNoTracing([&] {
    const bool result = servicelib::datasource::cron::detail::WaitForDrain(
        servicelib::Context{}.withStopToken(stop.get_token()), mutex, drained, active);
    cancelledFinished.store(true, std::memory_order_release);
    return result;
  });
  auto live = userver::engine::AsyncNoTracing([&] {
    const bool result = servicelib::datasource::cron::detail::WaitForDrain(
        servicelib::Context{}, mutex, drained, active);
    liveFinished.store(true, std::memory_order_release);
    return result;
  });
  userver::engine::SleepFor(std::chrono::milliseconds{5});
  stop.request_stop();
  for (int attempt = 0; attempt < 20 && !cancelledFinished.load(std::memory_order_acquire); ++attempt) {
    userver::engine::SleepFor(std::chrono::milliseconds{5});
  }
  EXPECT_TRUE(cancelledFinished.load(std::memory_order_acquire));
  EXPECT_FALSE(liveFinished.load(std::memory_order_acquire));
  {
    std::lock_guard lock(mutex);
    active = 0;
  }
  drained.NotifyAll();
  EXPECT_FALSE(cancelled.Get());
  EXPECT_TRUE(live.Get());
}

UTEST(CronDataSource, CompletedWaitUnsubscribesBeforeLaterCancellation) {
  userver::engine::Mutex mutex;
  userver::engine::ConditionVariable drained;
  std::size_t active = 1;
  std::stop_source stop;
  auto waiting = userver::engine::AsyncNoTracing([&] {
    return servicelib::datasource::cron::detail::WaitForDrain(
        servicelib::Context{}.withStopToken(stop.get_token()), mutex, drained, active);
  });
  userver::engine::SleepFor(std::chrono::milliseconds{5});
  {
    std::lock_guard lock(mutex);
    active = 0;
  }
  drained.NotifyAll();
  EXPECT_TRUE(waiting.Get());
  std::thread cancel([&] { stop.request_stop(); });
  cancel.join();
}

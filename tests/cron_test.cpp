/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

#include <stdexcept>

#include <userver/utest/utest.hpp>

#include <atomic>
#include <userver/engine/async.hpp>

#include <servicelib/datasource/cron/libcron.hpp>

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

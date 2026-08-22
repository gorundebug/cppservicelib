#include <atomic>
#include <chrono>
#include <mutex>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>

#include <userver/engine/mutex.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/single_consumer_event.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/utest/utest.hpp>

#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/pool/delaypool.hpp>
#include <servicelib/runtime/pool/prioritytaskpool.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

namespace {

using namespace std::chrono_literals;

constexpr char kPoolName[] = "test-priority-pool";
constexpr char kServiceName[] = "other-pools-test-service";

class TestConfig final : public servicelib::config::IConfig {
 public:
  explicit TestConfig(int executors_count)
      : pool_{.name = kPoolName,
              .executorsCount = executors_count,
              .queueCapacity = 256,
              .properties = {}} {}

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {};
  }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override {
    return {&pool_};
  }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override {
    return {};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override {
    return {};
  }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override {
    return {};
  }

 private:
  servicelib::config::PoolConfig pool_;
};

class TestEnvironment final : public servicelib::IServiceEnvironment {
 public:
  explicit TestEnvironment(int executors_count = 1)
      : config_(executors_count), runtime_config_(config_) {
    service_config_.name = kServiceName;
  }

  std::shared_ptr<const servicelib::config::RuntimeConfig>
  getRuntimeConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::RuntimeConfig>(
        runtime_config_);
  }
  std::shared_ptr<const servicelib::config::ServiceConfig>
  getServiceConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::ServiceConfig>(
        service_config_);
  }
  servicelib::log::Logger& getLogger() override { return log_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }

  servicelib::testmetrics::TestMetrics& metrics() { return metrics_; }

 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtime_config_;
  servicelib::config::ServiceConfig service_config_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};

template <typename Pool>
class StopPoolOnExit final {
 public:
  explicit StopPoolOnExit(Pool& pool) : pool_(pool) {}
  ~StopPoolOnExit() { pool_.stop(servicelib::Context{}); }

  StopPoolOnExit(const StopPoolOnExit&) = delete;
  StopPoolOnExit& operator=(const StopPoolOnExit&) = delete;

 private:
  Pool& pool_;
};

servicelib::metrics::Labels PriorityLabels() {
  return {{"name", kPoolName}, {"service", kServiceName}};
}

servicelib::metrics::Labels PriorityEventLabels(std::string event) {
  auto labels = PriorityLabels();
  labels.emplace("event", std::move(event));
  return labels;
}

servicelib::metrics::Labels DelayLabels() {
  return {{"service", kServiceName}};
}

servicelib::metrics::Labels DelayEventLabels(std::string event) {
  auto labels = DelayLabels();
  labels.emplace("event", std::move(event));
  return labels;
}

UTEST(PriorityTaskPool, PriorityFifoAndDeadlinePromotion) {
  TestEnvironment environment;
  servicelib::pool::PriorityTaskPoolImpl pool{kPoolName, environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  userver::engine::SingleConsumerEvent blocker_started;
  userver::engine::SingleConsumerEvent release_blocker;
  userver::engine::Mutex order_mutex;
  std::vector<int> execution_order;

  pool.addTask(servicelib::Context{}, 0, [&] {
    blocker_started.Send();
    static_cast<void>(release_blocker.WaitForEvent());
  });
  ASSERT_TRUE(
      blocker_started.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  EXPECT_EQ(environment.metrics()
                .gauge("priority_task_pool.executors_target", PriorityLabels())
                .value(),
            1);
  EXPECT_EQ(
      environment.metrics()
          .gauge("priority_task_pool.executors_allocated", PriorityLabels())
          .value(),
      1);
  EXPECT_EQ(environment.metrics()
                .gauge("priority_task_pool.executors_busy", PriorityLabels())
                .value(),
            1);

  const auto append = [&](int value) {
    return [&, value] {
      std::lock_guard lock{order_mutex};
      execution_order.push_back(value);
    };
  };

  pool.addTask(servicelib::Context{}, 100, append(1));
  pool.addTask(servicelib::Context{}.withDeadline(
                   std::chrono::steady_clock::now() + 40ms),
               -100, append(2));
  pool.addTask(servicelib::Context{}, 50, append(3));
  pool.addTask(servicelib::Context{}, 50, append(4));

  userver::engine::SleepFor(80ms);
  release_blocker.Send();
  pool.stop(servicelib::Context{});

  EXPECT_EQ(execution_order, (std::vector<int>{2, 3, 4, 1}));
  EXPECT_EQ(environment.metrics()
                .counter("priority_task_pool.events_total",
                         PriorityEventLabels("task_expedited"))
                .count(),
            1);
  EXPECT_EQ(environment.metrics()
                .gauge("priority_task_pool.queue_length", PriorityLabels())
                .value(),
            0);
  EXPECT_EQ(
      environment.metrics()
          .gauge("priority_task_pool.executors_allocated", PriorityLabels())
          .value(),
      0);
  EXPECT_EQ(environment.metrics()
                .gauge("priority_task_pool.executors_busy", PriorityLabels())
                .value(),
            0);
}

UTEST(PriorityTaskPool, ExplicitCancellationPromotesOnlyOnce) {
  TestEnvironment environment;
  servicelib::pool::PriorityTaskPoolImpl pool{kPoolName, environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  userver::engine::SingleConsumerEvent blocker_started;
  userver::engine::SingleConsumerEvent release_blocker;
  userver::engine::SingleConsumerEvent completed;

  pool.addTask(servicelib::Context{}, 0, [&] {
    blocker_started.Send();
    static_cast<void>(release_blocker.WaitForEvent());
  });
  ASSERT_TRUE(
      blocker_started.WaitForEventFor(userver::utest::kMaxTestWaitTime));

  std::stop_source source;
  auto context = servicelib::Context{}
                     .withDeadline(std::chrono::steady_clock::now() + 1h)
                     .withStopToken(source.get_token());
  pool.addTask(context, 100, [&] { completed.Send(); });
  source.request_stop();
  userver::engine::SleepFor(40ms);
  release_blocker.Send();

  ASSERT_TRUE(completed.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  pool.stop(servicelib::Context{});
  EXPECT_EQ(environment.metrics()
                .counter("priority_task_pool.events_total",
                         PriorityEventLabels("task_expedited"))
                .count(),
            1);
}

UTEST(PriorityTaskPool, ExternalCancellationPromotesQueuedTask) {
  TestEnvironment environment;
  servicelib::pool::PriorityTaskPoolImpl pool{kPoolName, environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  userver::engine::SingleConsumerEvent blocker_started;
  userver::engine::SingleConsumerEvent release_blocker;
  userver::engine::Mutex order_mutex;
  std::vector<int> execution_order;

  pool.addTask(servicelib::Context{}, 0, [&] {
    blocker_started.Send();
    static_cast<void>(release_blocker.WaitForEvent());
  });
  ASSERT_TRUE(
      blocker_started.WaitForEventFor(userver::utest::kMaxTestWaitTime));

  const auto append = [&](int value) {
    return [&, value] {
      std::lock_guard lock{order_mutex};
      execution_order.push_back(value);
    };
  };

  std::stop_source transport_cancellation;
  pool.addTask(servicelib::Context{}.withExternalCancellation(
                   transport_cancellation.get_token()),
               100, append(1));
  pool.addTask(servicelib::Context{}, 1, append(2));

  transport_cancellation.request_stop();
  userver::engine::SleepFor(40ms);
  release_blocker.Send();
  pool.stop(servicelib::Context{});

  EXPECT_EQ(execution_order, (std::vector<int>{1, 2}));
  EXPECT_EQ(environment.metrics()
                .counter("priority_task_pool.events_total",
                         PriorityEventLabels("task_expedited"))
                .count(),
            1);
}

UTEST(PriorityTaskPool, RejectsExpiredDeadline) {
  TestEnvironment environment;
  servicelib::pool::PriorityTaskPoolImpl pool{kPoolName, environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  EXPECT_THROW(pool.addTask(servicelib::Context{}.withDeadline(
                                std::chrono::steady_clock::now() - 1ms),
                            0, [] {}),
               servicelib::pool::PoolCancelledError);
}

UTEST(DelayPool, DeadlineAndCancellationExecuteExactlyOnce) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  std::atomic<int> remaining{2};
  std::atomic<int> executions{0};
  userver::engine::SingleConsumerEvent completed;
  const auto task = [&] {
    executions.fetch_add(1, std::memory_order_relaxed);
    if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      completed.Send();
    }
  };

  pool.delay(servicelib::Context{}.withDeadline(
                 std::chrono::steady_clock::now() + 40ms),
             1h, task);

  std::stop_source source;
  pool.delay(servicelib::Context{}.withStopToken(source.get_token()), 1h, task);
  source.request_stop();

  ASSERT_TRUE(completed.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  userver::engine::SleepFor(40ms);
  pool.stop(servicelib::Context{});

  EXPECT_EQ(executions.load(std::memory_order_relaxed), 2);
  EXPECT_EQ(environment.metrics()
                .counter("delay_pool.tasks_total", DelayLabels())
                .count(),
            2);
  EXPECT_EQ(environment.metrics()
                .histogram("delay_pool.task_execution_duration_seconds",
                           DelayLabels())
                .count(),
            2);
  EXPECT_EQ(environment.metrics()
                .counter("delay_pool.events_total",
                         DelayEventLabels("task_cancelled"))
                .count(),
            2);
  EXPECT_EQ(environment.metrics()
                .gauge("delay_pool.wait_queue_length", DelayLabels())
                .value(),
            0);
}

UTEST(DelayPool, PositiveDelayUsesNormalTimerPath) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  userver::engine::SingleConsumerEvent completed;
  pool.delay(servicelib::Context{}, 30ms, [&] { completed.Send(); });

  ASSERT_TRUE(completed.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  pool.stop(servicelib::Context{});

  EXPECT_EQ(environment.metrics()
                .counter("delay_pool.tasks_total", DelayLabels())
                .count(),
            1);
  EXPECT_EQ(environment.metrics()
                .counter("delay_pool.events_total",
                         DelayEventLabels("task_cancelled"))
                .count(),
            0);
  EXPECT_EQ(environment.metrics()
                .gauge("delay_pool.wait_queue_length", DelayLabels())
                .value(),
            0);
}

UTEST(DelayPool, DelayBeforeStartIsAccepted) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  StopPoolOnExit stop_guard{pool};

  userver::engine::SingleConsumerEvent completed;
  pool.delay(servicelib::Context{}, 0ms, [&] { completed.Send(); });

  ASSERT_TRUE(completed.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  EXPECT_NO_THROW(pool.start(servicelib::Context{}));
}

UTEST(DelayPool, StopDeadlineReportsButStillDrainsAcceptedTask) {
  TestEnvironment environment;
  auto pool = std::make_unique<servicelib::pool::DelayPoolImpl>(environment);
  pool->start(servicelib::Context{});

  userver::engine::SingleConsumerEvent started;
  userver::engine::SingleConsumerEvent release;
  userver::engine::SingleConsumerEvent completed;
  pool->delay(servicelib::Context{}, 0ms, [&] {
    started.Send();
    static_cast<void>(release.WaitForEvent());
    completed.Send();
  });
  ASSERT_TRUE(started.WaitForEventFor(userver::utest::kMaxTestWaitTime));

  const auto stopStarted = std::chrono::steady_clock::now();
  userver::engine::SingleConsumerEvent stopped;
  auto stopTask = userver::engine::AsyncNoTracing([&] {
    pool->stop(servicelib::Context{}.withDeadline(stopStarted + 20ms));
    stopped.Send();
  });
  EXPECT_FALSE(stopped.WaitForEventFor(50ms));
  EXPECT_EQ(
      environment.metrics()
          .counter("delay_pool.events_total", DelayEventLabels("stop_timeout"))
          .count(),
      1);
  EXPECT_EQ(environment.metrics()
                .gauge("delay_pool.wait_queue_length", DelayLabels())
                .value(),
            1);

  release.Send();
  ASSERT_TRUE(completed.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  ASSERT_TRUE(stopped.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  stopTask.Get();
  pool.reset();

  const auto gaugeDeadline =
      std::chrono::steady_clock::now() + userver::utest::kMaxTestWaitTime;
  while (environment.metrics()
                 .gauge("delay_pool.wait_queue_length", DelayLabels())
                 .value() != 0 &&
         std::chrono::steady_clock::now() < gaugeDeadline) {
    userver::engine::SleepFor(1ms);
  }
  EXPECT_EQ(environment.metrics()
                .gauge("delay_pool.wait_queue_length", DelayLabels())
                .value(),
            0);
}

UTEST(DelayPool, CancelledTimerCoroutineRetiresPromptly) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  std::stop_source source;
  userver::engine::SingleConsumerEvent completed;
  pool.delay(servicelib::Context{}.withStopToken(source.get_token()), 1h,
             [&] { completed.Send(); });
  source.request_stop();

  ASSERT_TRUE(completed.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  const auto deadline =
      std::chrono::steady_clock::now() + userver::utest::kMaxTestWaitTime;
  while (pool.activeTasksApprox() != 0 &&
         std::chrono::steady_clock::now() < deadline) {
    userver::engine::SleepFor(1ms);
  }
  EXPECT_EQ(pool.activeTasksApprox(), 0);
}

UTEST(DelayPool, RejectsCancelledContextAndDetectsSelfStop) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  std::stop_source cancelled;
  cancelled.request_stop();
  EXPECT_THROW(
      pool.delay(servicelib::Context{}.withStopToken(cancelled.get_token()), 1s,
                 [] {}),
      servicelib::pool::PoolCancelledError);

  std::atomic<bool> self_stop_rejected{false};
  userver::engine::SingleConsumerEvent completed;
  pool.delay(servicelib::Context{}, 0ms, [&] {
    try {
      pool.stop(servicelib::Context{});
    } catch (const servicelib::pool::PoolSelfStopError&) {
      self_stop_rejected.store(true, std::memory_order_relaxed);
    }
    completed.Send();
  });

  ASSERT_TRUE(completed.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  pool.stop(servicelib::Context{});

  EXPECT_TRUE(self_stop_rejected.load(std::memory_order_relaxed));
  EXPECT_EQ(
      environment.metrics()
          .counter("delay_pool.events_total", DelayEventLabels("task_rejected"))
          .count(),
      1);
  EXPECT_EQ(environment.metrics()
                .counter("delay_pool.tasks_total", DelayLabels())
                .count(),
            1);
}

UTEST(DelayPool, ExternalCancellationExpeditesAndIsVisibleToCallback) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  std::stop_source transport_cancellation;
  const auto context = servicelib::Context{}.withExternalCancellation(
      transport_cancellation.get_token());
  std::atomic<int> executions{0};
  std::atomic<bool> observed_cancelled{false};
  userver::engine::SingleConsumerEvent completed;

  pool.delay(context, 1h, [&, context] {
    executions.fetch_add(1, std::memory_order_relaxed);
    observed_cancelled.store(context.cancelled(), std::memory_order_relaxed);
    completed.Send();
  });
  transport_cancellation.request_stop();

  ASSERT_TRUE(completed.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  userver::engine::SleepFor(40ms);
  pool.stop(servicelib::Context{});

  EXPECT_EQ(executions.load(std::memory_order_relaxed), 1);
  EXPECT_TRUE(observed_cancelled.load(std::memory_order_relaxed));
  EXPECT_EQ(environment.metrics()
                .counter("delay_pool.events_total",
                         DelayEventLabels("task_cancelled"))
                .count(),
            1);
}

UTEST(DelayPool, RejectsExpiredDeadline) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  EXPECT_THROW(pool.delay(servicelib::Context{}.withDeadline(
                              std::chrono::steady_clock::now() - 1ms),
                          1h, [] {}),
               servicelib::pool::PoolCancelledError);
}

}  // namespace

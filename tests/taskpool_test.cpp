#include <atomic>
#include <chrono>
#include <mutex>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>

#include <userver/engine/mutex.hpp>
#include <userver/engine/single_consumer_event.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/utest/utest.hpp>

#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/pool/taskpool.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

namespace {

using namespace std::chrono_literals;

constexpr char kPoolName[] = "test-task-pool";
constexpr char kServiceName[] = "taskpool-test-service";

class TestConfig final : public servicelib::config::IConfig {
 public:
  explicit TestConfig(int executors_count)
      : pool_{.name = kPoolName,
              .executorsCount = executors_count,
              .queueCapacity = 0,
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
  servicelib::testlog::TestLog& log() { return log_; }

 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtime_config_;
  servicelib::config::ServiceConfig service_config_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};

class StopPoolOnExit final {
 public:
  explicit StopPoolOnExit(servicelib::pool::ITaskPool& pool) : pool_(pool) {}
  ~StopPoolOnExit() { pool_.stop(servicelib::Context{}); }

  StopPoolOnExit(const StopPoolOnExit&) = delete;
  StopPoolOnExit& operator=(const StopPoolOnExit&) = delete;

 private:
  servicelib::pool::ITaskPool& pool_;
};

servicelib::metrics::Labels BaseMetricLabels() {
  return {{"name", kPoolName}, {"service", kServiceName}};
}

servicelib::metrics::Labels EventMetricLabels(std::string event) {
  auto labels = BaseMetricLabels();
  labels.emplace("event", std::move(event));
  return labels;
}

UTEST(TaskPool, LifecycleFifoAndMetrics) {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};

  EXPECT_THROW(pool.addTask(servicelib::Context{}, [] {}),
               servicelib::pool::PoolNotStartedError);

  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};
  EXPECT_EQ(pool.getExecutorsCount(), 1);

  userver::engine::SingleConsumerEvent first_started;
  userver::engine::SingleConsumerEvent release_first;
  userver::engine::Mutex order_mutex;
  std::vector<int> execution_order;

  pool.addTask(servicelib::Context{}, [&] {
    {
      std::lock_guard lock{order_mutex};
      execution_order.push_back(0);
    }
    first_started.Send();
    static_cast<void>(release_first.WaitForEvent());
  });

  ASSERT_TRUE(first_started.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  const auto labels = BaseMetricLabels();
  EXPECT_EQ(
      environment.metrics().gauge("task_pool.executors_target", labels).value(),
      1);
  EXPECT_EQ(environment.metrics()
                .gauge("task_pool.executors_allocated", labels)
                .value(),
            1);
  EXPECT_EQ(
      environment.metrics().gauge("task_pool.executors_busy", labels).value(),
      1);
  pool.addTask(servicelib::Context{}, [&] {
    std::lock_guard lock{order_mutex};
    execution_order.push_back(1);
  });
  pool.addTask(servicelib::Context{}, [&] {
    std::lock_guard lock{order_mutex};
    execution_order.push_back(2);
  });
  release_first.Send();

  pool.stop(servicelib::Context{});

  EXPECT_EQ(execution_order, (std::vector<int>{0, 1, 2}));
  EXPECT_THROW(pool.addTask(servicelib::Context{}, [] {}),
               servicelib::pool::PoolStoppedError);

  EXPECT_EQ(
      environment.metrics().counter("task_pool.tasks_total", labels).count(),
      3);
  EXPECT_EQ(environment.metrics()
                .histogram("task_pool.task_execution_duration_seconds", labels)
                .count(),
            3);
  EXPECT_EQ(
      environment.metrics().gauge("task_pool.queue_length", labels).value(), 0);
  EXPECT_EQ(environment.metrics()
                .gauge("task_pool.executors_allocated", labels)
                .value(),
            0);
  EXPECT_EQ(
      environment.metrics().gauge("task_pool.executors_busy", labels).value(),
      0);
  EXPECT_EQ(
      environment.metrics()
          .counter("task_pool.events_total", EventMetricLabels("task_rejected"))
          .count(),
      2);
}

UTEST(TaskPool, CancelledContextIsRejectedAndTaskFailureIsIsolated) {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  std::stop_source cancelled_source;
  cancelled_source.request_stop();
  const auto cancelled_context =
      servicelib::Context{}.withStopToken(cancelled_source.get_token());

  EXPECT_THROW(pool.addTask(cancelled_context, [] {}),
               servicelib::pool::PoolCancelledError);

  std::atomic<int> completed{0};
  pool.addTask(servicelib::Context{},
               [] { throw std::runtime_error("expected task failure"); });
  pool.addTask(servicelib::Context{},
               [&] { completed.fetch_add(1, std::memory_order_relaxed); });
  pool.stop(servicelib::Context{});

  EXPECT_EQ(completed.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(environment.metrics()
                .counter("task_pool.tasks_total", BaseMetricLabels())
                .count(),
            2);
  EXPECT_EQ(
      environment.metrics()
          .counter("task_pool.events_total", EventMetricLabels("task_rejected"))
          .count(),
      1);
  EXPECT_FALSE(
      environment.log().entriesAtLevel(servicelib::log::Level::kWarn).empty());
}

UTEST(TaskPool, DeadlineMovesQueuedTaskToFront) {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  userver::engine::SingleConsumerEvent blocker_started;
  userver::engine::SingleConsumerEvent release_blocker;
  userver::engine::Mutex order_mutex;
  std::vector<int> execution_order;

  pool.addTask(servicelib::Context{}, [&] {
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

  pool.addTask(servicelib::Context{}, append(1));
  const auto deadline = std::chrono::steady_clock::now() + 40ms;
  pool.addTask(servicelib::Context{}.withDeadline(deadline), append(2));
  pool.addTask(servicelib::Context{}, append(3));

  userver::engine::SleepFor(80ms);
  release_blocker.Send();
  pool.stop(servicelib::Context{});

  EXPECT_EQ(execution_order, (std::vector<int>{2, 1, 3}));
  EXPECT_EQ(environment.metrics()
                .counter("task_pool.events_total",
                         EventMetricLabels("task_cancelled"))
                .count(),
            1);
}

UTEST(TaskPool, EarlierDeadlinePrecedesLaterExplicitCancellation) {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  userver::engine::SingleConsumerEvent blocker_started;
  userver::engine::SingleConsumerEvent release_blocker;
  userver::engine::Mutex order_mutex;
  std::vector<int> execution_order;

  pool.addTask(servicelib::Context{}, [&] {
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

  pool.addTask(servicelib::Context{}, append(1));
  pool.addTask(servicelib::Context{}.withDeadline(
                   std::chrono::steady_clock::now() + 40ms),
               append(2));

  std::stop_source cancelled_source;
  pool.addTask(
      servicelib::Context{}.withStopToken(cancelled_source.get_token()),
      append(3));
  pool.addTask(servicelib::Context{}, append(4));

  userver::engine::SleepFor(80ms);
  cancelled_source.request_stop();
  userver::engine::SleepFor(40ms);
  release_blocker.Send();
  pool.stop(servicelib::Context{});

  // Deadline(2) happened first and moved 2 to the head. Cancellation(3)
  // happened later and therefore moved 3 in front of 2.
  EXPECT_EQ(execution_order, (std::vector<int>{3, 2, 1, 4}));
  EXPECT_EQ(environment.metrics()
                .counter("task_pool.events_total",
                         EventMetricLabels("task_cancelled"))
                .count(),
            2);
}

UTEST(TaskPool, ExternalCancellationMovesQueuedTaskToFront) {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  userver::engine::SingleConsumerEvent blocker_started;
  userver::engine::SingleConsumerEvent release_blocker;
  userver::engine::Mutex order_mutex;
  std::vector<int> execution_order;

  pool.addTask(servicelib::Context{}, [&] {
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

  pool.addTask(servicelib::Context{}, append(1));
  std::stop_source transport_cancellation;
  pool.addTask(servicelib::Context{}.withExternalCancellation(
                   transport_cancellation.get_token()),
               append(2));

  transport_cancellation.request_stop();
  userver::engine::SleepFor(40ms);
  release_blocker.Send();
  pool.stop(servicelib::Context{});

  EXPECT_EQ(execution_order, (std::vector<int>{2, 1}));
  EXPECT_EQ(environment.metrics()
                .counter("task_pool.events_total",
                         EventMetricLabels("task_cancelled"))
                .count(),
            1);
}

UTEST(TaskPool, RejectsExpiredDeadline) {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  EXPECT_THROW(
      pool.addTask(servicelib::Context{}.withDeadline(
                       std::chrono::steady_clock::now() - 1ms),
                   [] {}),
      servicelib::pool::PoolCancelledError);
}

}  // namespace

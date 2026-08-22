#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <benchmark/benchmark.h>
#include <userver/engine/run_standalone.hpp>
#include <userver/engine/single_consumer_event.hpp>

namespace {

struct LockProfileStats final {
  void reset() noexcept {
    samples.store(0, std::memory_order_relaxed);
    waitNs.store(0, std::memory_order_relaxed);
    holdNs.store(0, std::memory_order_relaxed);
    maxWaitNs.store(0, std::memory_order_relaxed);
    maxHoldNs.store(0, std::memory_order_relaxed);
  }

  static void updateMax(std::atomic<std::uint64_t>& target,
                        std::uint64_t value) noexcept {
    auto observed = target.load(std::memory_order_relaxed);
    while (observed < value &&
           !target.compare_exchange_weak(observed, value,
                                         std::memory_order_relaxed)) {
    }
  }

  void add(std::uint64_t wait, std::uint64_t hold) noexcept {
    samples.fetch_add(1, std::memory_order_relaxed);
    waitNs.fetch_add(wait, std::memory_order_relaxed);
    holdNs.fetch_add(hold, std::memory_order_relaxed);
    updateMax(maxWaitNs, wait);
    updateMax(maxHoldNs, hold);
  }

  std::atomic<std::uint64_t> samples{0};
  std::atomic<std::uint64_t> waitNs{0};
  std::atomic<std::uint64_t> holdNs{0};
  std::atomic<std::uint64_t> maxWaitNs{0};
  std::atomic<std::uint64_t> maxHoldNs{0};
};

std::array<LockProfileStats, 2> gLockProfile;

class TaskPoolLockProbe final {
 public:
  explicit TaskPoolLockProbe(std::size_t operation) noexcept
      : operation_(operation), sampled_((++sampleSequence_ & 255U) == 0) {
    if (sampled_) {
      requested_ = Clock::now();
    }
  }

  void acquired() noexcept {
    if (sampled_) {
      acquired_ = Clock::now();
      waitNs_ = std::chrono::duration_cast<std::chrono::nanoseconds>(acquired_ -
                                                                     requested_)
                    .count();
    }
  }

  void resumed() noexcept {
    if (sampled_) {
      acquired_ = Clock::now();
    }
  }

  void releasing() noexcept {
    if (sampled_) {
      releasing_ = Clock::now();
    }
  }

  void commit() noexcept {
    if (!sampled_) {
      return;
    }
    const auto hold = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          releasing_ - acquired_)
                          .count();
    gLockProfile[operation_].add(static_cast<std::uint64_t>(waitNs_),
                                 static_cast<std::uint64_t>(hold));
  }

 private:
  using Clock = std::chrono::steady_clock;
  inline static thread_local std::uint64_t sampleSequence_ = 0;

  std::size_t operation_;
  bool sampled_;
  Clock::time_point requested_{};
  Clock::time_point acquired_{};
  Clock::time_point releasing_{};
  std::int64_t waitNs_{0};
};

}  // namespace

#define STREAM_TASKPOOL_LOCK_PROFILE_BEGIN(operation) \
  ::TaskPoolLockProbe stream_taskpool_lock_probe { operation }
#define STREAM_TASKPOOL_LOCK_PROFILE_ACQUIRED() \
  stream_taskpool_lock_probe.acquired()
#define STREAM_TASKPOOL_LOCK_PROFILE_RESUMED() \
  stream_taskpool_lock_probe.resumed()
#define STREAM_TASKPOOL_LOCK_PROFILE_RELEASING() \
  stream_taskpool_lock_probe.releasing()
#define STREAM_TASKPOOL_LOCK_PROFILE_COMMIT() \
  stream_taskpool_lock_probe.commit()

#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/pool/prioritytaskpool.hpp>
#include <servicelib/runtime/pool/taskpool.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

namespace {

constexpr char kPoolName[] = "benchmark-task-pool";
constexpr char kServiceName[] = "taskpool-benchmark-service";

class BenchmarkConfig final : public servicelib::config::IConfig {
 public:
  explicit BenchmarkConfig(int executors_count)
      : pool_{.name = kPoolName,
              .executorsCount = executors_count,
              .queueCapacity = 0} {}

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

class BenchmarkEnvironment final : public servicelib::IServiceEnvironment {
 public:
  explicit BenchmarkEnvironment(int executors_count)
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

 private:
  BenchmarkConfig config_;
  servicelib::config::RuntimeConfig runtime_config_;
  servicelib::config::ServiceConfig service_config_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};

struct BatchCompletion final {
  explicit BatchCompletion(std::size_t count) : remaining(count) {}

  void taskCompleted() {
    if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      completed.Send();
    }
  }

  std::atomic<std::size_t> remaining;
  userver::engine::SingleConsumerEvent completed;
};

void BusyWaitFor(std::chrono::nanoseconds duration) {
  if (duration <= std::chrono::nanoseconds::zero()) {
    return;
  }

  const auto deadline = std::chrono::steady_clock::now() + duration;
  while (std::chrono::steady_clock::now() < deadline) {
    // Prevent the compiler from replacing the loop with a single sleep or
    // removing it altogether. The clock check deliberately models CPU-bound
    // work; it does not yield the userver task processor.
    benchmark::DoNotOptimize(deadline);
  }
}

void RunEndToEndBenchmark(
    benchmark::State& state, bool attach_deadline,
    std::chrono::nanoseconds task_work = std::chrono::nanoseconds::zero()) {
  const auto executors_count = static_cast<int>(state.range(0));
  const auto batch_size = static_cast<std::size_t>(state.range(1));

  userver::engine::TaskProcessorPoolsConfig engine_config;
  // Docker Desktop may not expose userfaultfd even to native containers.
  engine_config.is_stack_usage_monitor_enabled = false;

  userver::engine::RunStandalone(
      static_cast<std::size_t>(executors_count), engine_config, [&] {
        BenchmarkEnvironment environment{executors_count};
        servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
        pool.start(servicelib::Context{});

        for (auto unused : state) {
          static_cast<void>(unused);
          auto batch = std::make_shared<BatchCompletion>(batch_size);

          auto context = servicelib::Context{};
          if (attach_deadline) {
            context = context.withDeadline(std::chrono::steady_clock::now() +
                                           std::chrono::hours{1});
          }

          for (std::size_t i = 0; i < batch_size; ++i) {
            pool.addTask(context, [batch, task_work] {
              BusyWaitFor(task_work);
              batch->taskCompleted();
            });
          }

          if (!batch->completed.WaitForEvent()) {
            state.SkipWithError("batch wait was cancelled");
            break;
          }
        }

        pool.stop(servicelib::Context{});
      });

  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(batch_size));
}

void RunPriorityEndToEndBenchmark(benchmark::State& state,
                                  bool attach_deadline) {
  const auto executors_count = static_cast<int>(state.range(0));
  const auto batch_size = static_cast<std::size_t>(state.range(1));

  userver::engine::TaskProcessorPoolsConfig engine_config;
  engine_config.is_stack_usage_monitor_enabled = false;

  userver::engine::RunStandalone(
      static_cast<std::size_t>(executors_count), engine_config, [&] {
        BenchmarkEnvironment environment{executors_count};
        servicelib::pool::PriorityTaskPoolImpl pool{kPoolName, environment};
        pool.start(servicelib::Context{});

        for (auto unused : state) {
          static_cast<void>(unused);
          auto batch = std::make_shared<BatchCompletion>(batch_size);

          auto context = servicelib::Context{};
          if (attach_deadline) {
            context = context.withDeadline(std::chrono::steady_clock::now() +
                                           std::chrono::hours{1});
          }

          for (std::size_t i = 0; i < batch_size; ++i) {
            pool.addTask(context, static_cast<int>(i % 8),
                         [batch] { batch->taskCompleted(); });
          }

          if (!batch->completed.WaitForEvent()) {
            state.SkipWithError("batch wait was cancelled");
            break;
          }
        }

        pool.stop(servicelib::Context{});
      });

  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(batch_size));
}

void TaskPoolEnqueueAndExecute(benchmark::State& state) {
  RunEndToEndBenchmark(state, false);
}

void TaskPoolEnqueueAndExecuteWithDeadline(benchmark::State& state) {
  RunEndToEndBenchmark(state, true);
}

void TaskPoolWorkloadSweep(benchmark::State& state) {
  RunEndToEndBenchmark(
      state, false,
      std::chrono::nanoseconds{static_cast<std::int64_t>(state.range(2))});
}

void TaskPoolMutexProfile(benchmark::State& state) {
  for (auto& stats : gLockProfile) {
    stats.reset();
  }

  RunEndToEndBenchmark(state, false);

  constexpr std::array<const char*, 2> kNames{"enqueue", "dequeue"};
  for (std::size_t i = 0; i < gLockProfile.size(); ++i) {
    const auto samples =
        gLockProfile[i].samples.load(std::memory_order_relaxed);
    const double divisor = samples == 0 ? 1.0 : static_cast<double>(samples);
    state.counters[std::string{kNames[i]} + "_wait_avg_ns"] =
        static_cast<double>(
            gLockProfile[i].waitNs.load(std::memory_order_relaxed)) /
        divisor;
    state.counters[std::string{kNames[i]} + "_hold_avg_ns"] =
        static_cast<double>(
            gLockProfile[i].holdNs.load(std::memory_order_relaxed)) /
        divisor;
    state.counters[std::string{kNames[i]} + "_wait_max_ns"] =
        static_cast<double>(
            gLockProfile[i].maxWaitNs.load(std::memory_order_relaxed));
    state.counters[std::string{kNames[i]} + "_hold_max_ns"] =
        static_cast<double>(
            gLockProfile[i].maxHoldNs.load(std::memory_order_relaxed));
    state.counters[std::string{kNames[i]} + "_samples"] =
        static_cast<double>(samples);
  }
}

void PriorityTaskPoolEnqueueAndExecute(benchmark::State& state) {
  RunPriorityEndToEndBenchmark(state, false);
}

void PriorityTaskPoolEnqueueAndExecuteWithDeadline(benchmark::State& state) {
  RunPriorityEndToEndBenchmark(state, true);
}

void TaskPoolArguments(benchmark::internal::Benchmark* benchmark) {
  for (const auto executors : {1, 2, 4, 8}) {
    for (const auto batch_size : {1, 256, 1024}) {
      benchmark->Args({executors, batch_size});
    }
  }
}

void TaskPoolWorkloadArguments(benchmark::internal::Benchmark* benchmark) {
  constexpr std::int64_t kBatchSize = 1024;
  for (const auto executors : {1, 2, 4, 8}) {
    for (const std::int64_t work_ns :
         {0, 250, 500, 1000, 2000, 5000, 10000, 50000, 100000}) {
      benchmark->Args({executors, kBatchSize, work_ns});
    }
  }
}

void TaskPoolMutexProfileArguments(benchmark::internal::Benchmark* benchmark) {
  for (const auto executors : {1, 2, 4, 8}) {
    benchmark->Args({executors, 4096});
  }
}

BENCHMARK(TaskPoolEnqueueAndExecute)
    ->Apply(TaskPoolArguments)
    ->ArgNames({"executors", "batch"})
    ->UseRealTime();

BENCHMARK(TaskPoolEnqueueAndExecuteWithDeadline)
    ->Apply(TaskPoolArguments)
    ->ArgNames({"executors", "batch"})
    ->UseRealTime();

BENCHMARK(TaskPoolWorkloadSweep)
    ->Apply(TaskPoolWorkloadArguments)
    ->ArgNames({"executors", "batch", "work_ns"})
    ->UseRealTime();

BENCHMARK(TaskPoolMutexProfile)
    ->Apply(TaskPoolMutexProfileArguments)
    ->ArgNames({"executors", "batch"})
    ->UseRealTime();

BENCHMARK(PriorityTaskPoolEnqueueAndExecute)
    ->Apply(TaskPoolArguments)
    ->ArgNames({"executors", "batch"})
    ->UseRealTime();

BENCHMARK(PriorityTaskPoolEnqueueAndExecuteWithDeadline)
    ->Apply(TaskPoolArguments)
    ->ArgNames({"executors", "batch"})
    ->UseRealTime();

}  // namespace

/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

#include <servicelib/datasource/cron/libcron.hpp>

#include <libcron/Cron.h>
#include <libcron/CronSchedule.h>

#include <atomic>
#include <chrono>
#include <exception>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

#include <userver/concurrent/background_task_storage.hpp>
#include <userver/utils/periodic_task.hpp>
#include <userver/utils/uuid7.hpp>

#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/datasource.hpp>

namespace servicelib::datasource::cron {
namespace {

using Scheduler = libcron::Cron<libcron::UTCClock, libcron::Locker>;
using Clock = std::chrono::system_clock;

config::CronEndpointConfig EndpointConfig(
    const IServiceEnvironment& environment, int endpointId) {
  const auto runtime = environment.getRuntimeConfigSnapshot();
  const auto value = runtime ? runtime->GetEndpointConfigByID(endpointId)
                             : std::nullopt;
  const auto* result = value ? value->As<config::CronEndpointConfig>() : nullptr;
  if (!result) {
    throw std::invalid_argument("cron endpoint config not found");
  }
  return *result;
}

config::CronDataConnectorConfig ConnectorConfig(
    const IServiceEnvironment& environment, int connectorId) {
  const auto runtime = environment.getRuntimeConfigSnapshot();
  const auto value = runtime ? runtime->GetDataConnectorByID(connectorId)
                             : std::nullopt;
  const auto* result =
      value ? value->As<config::CronDataConnectorConfig>() : nullptr;
  if (!result) {
    throw std::invalid_argument("cron data connector config not found");
  }
  if (result->implementation !=
      servicelib::api::DataConnectorImplementation::kCppLibcron) {
    throw std::invalid_argument("cron data connector implementation must be cpp/libcron");
  }
  return *result;
}

}  // namespace

std::string ToLibcronExpression(const std::string& expression) {
  std::istringstream input(expression);
  std::vector<std::string> fields;
  std::string field;
  while (input >> field) fields.push_back(std::move(field));
  if (fields.size() != 5) {
    throw std::invalid_argument(
        "portable cron expression must contain exactly five fields");
  }
  const bool dayOfMonthSpecified = fields[2] != "*";
  const bool dayOfWeekSpecified = fields[4] != "*";
  if (dayOfMonthSpecified && dayOfWeekSpecified) {
    throw std::invalid_argument(
        "portable cron expression cannot constrain both day-of-month and day-of-week");
  }
  if (dayOfMonthSpecified) {
    fields[4] = "?";
  } else if (dayOfWeekSpecified) {
    fields[2] = "?";
  } else {
    fields[4] = "?";
  }
  return "0 " + fields[0] + " " + fields[1] + " " + fields[2] + " " +
         fields[3] + " " + fields[4];
}

struct Endpoint::Impl final {
  Impl(IServiceEnvironment& environmentValue, int endpointIdValue,
       Output outputValue)
      : environment(environmentValue),
        endpointId(endpointIdValue),
        endpointName(EndpointConfig(environment, endpointId).name),
        output(std::move(outputValue)),
        metrics(environment.getMetrics(), environment.getLogger(),
                ConnectorConfig(environment,
                                EndpointConfig(environment, endpointId)
                                    .idDataConnector)
                    .name,
                endpointName, "cron") {}

  std::optional<std::string> configure() {
    const auto cfg = EndpointConfig(environment, endpointId);
    if (!cfg.enabled) return std::nullopt;
    if (cfg.timezone != "UTC") {
      throw std::invalid_argument("scheduled endpoint timezone must be UTC");
    }
    overlapPolicy = cfg.overlapPolicy;
    missedRunPolicy = cfg.missedRunPolicy;
    const auto expression = ToLibcronExpression(cfg.schedule);
    auto cronData = libcron::CronData::create(expression);
    if (!cronData.is_valid()) {
      throw std::invalid_argument("invalid cron schedule for endpoint " +
                                  endpointName);
    }
    evaluator.emplace(cronData);
    lastScheduled.reset();
    stopping.store(false, std::memory_order_release);
    return expression;
  }

  void fire(const libcron::TaskInformation& information) {
    const auto firedAt = Clock::now();
    const auto scheduledAt = firedAt - information.get_delay();
    std::size_t due = 1;
    if (lastScheduled && evaluator) {
      auto cursor = *lastScheduled + std::chrono::seconds{1};
      while (cursor <= scheduledAt) {
        const auto [valid, next] = evaluator->calculate_from(cursor);
        if (!valid || next >= scheduledAt) break;
        ++due;
        cursor = next + std::chrono::seconds{1};
      }
    }
    lastScheduled = scheduledAt;
    if (due > 1 &&
        missedRunPolicy == api::ScheduleMissedRunPolicy::kSkip) {
      return;
    }
    if (stopping.load(std::memory_order_acquire)) return;

    bool ownsRunning = false;
    if (overlapPolicy == api::ScheduleOverlapPolicy::kSkip) {
      bool expected = false;
      if (!running.compare_exchange_strong(expected, true,
                                           std::memory_order_acq_rel)) {
        return;
      }
      ownsRunning = true;
    }
    try {
      tasks.CriticalAsyncDetach(
          "servicelib-cron-endpoint",
          [this, scheduledAt, ownsRunning] {
            struct RunningGuard final {
              std::atomic<bool>* running{};
              ~RunningGuard() {
                if (running) running->store(false, std::memory_order_release);
              }
            } guard{ownsRunning ? &running : nullptr};
            auto context = ApplyDataSourceEndpointTracing(
                MessageContext{}.withStreamId(
                    userver::utils::generators::GenerateUuidV7()),
                environment, endpointId);
            const auto started = metrics.requestStart();
            std::exception_ptr error;
            try {
              output(std::move(context), Payload<ScheduleTrigger>::make(
                  MakeScheduleTrigger(endpointId, endpointName, scheduledAt,
                                      Clock::now(), ScheduleBackend::kLocal)));
            } catch (...) {
              error = std::current_exception();
            }
            metrics.requestEnd(started, error);
          });
    } catch (...) {
      if (ownsRunning) running.store(false, std::memory_order_release);
      throw;
    }
  }

  void stop() noexcept {
    stopping.store(true, std::memory_order_release);
    tasks.CancelAndWait();
    running.store(false, std::memory_order_release);
  }

  IServiceEnvironment& environment;
  int endpointId;
  std::string endpointName;
  Output output;
  DataSourceEndpointMetrics metrics;
  api::ScheduleOverlapPolicy overlapPolicy{api::ScheduleOverlapPolicy::kSkip};
  api::ScheduleMissedRunPolicy missedRunPolicy{
      api::ScheduleMissedRunPolicy::kSkip};
  std::optional<libcron::CronSchedule> evaluator;
  std::optional<Clock::time_point> lastScheduled;
  std::atomic<bool> running{false};
  std::atomic<bool> stopping{true};
  userver::concurrent::BackgroundTaskStorage tasks;
};

Endpoint::Endpoint(IServiceEnvironment& environment, int endpointId,
                   Output output)
    : impl_(std::make_unique<Impl>(environment, endpointId,
                                  std::move(output))) {}

Endpoint::~Endpoint() = default;
int Endpoint::id() const noexcept { return impl_->endpointId; }
const std::string& Endpoint::name() const noexcept { return impl_->endpointName; }

struct LibcronDataSource::Impl final {
  Impl(IServiceEnvironment& environmentValue, int connectorIdValue)
      : environment(environmentValue),
        connectorId(connectorIdValue),
        connectorName(ConnectorConfig(environment, connectorId).name) {}

  IServiceEnvironment& environment;
  int connectorId;
  std::string connectorName;
  std::vector<std::shared_ptr<Endpoint>> endpoints;
  Scheduler scheduler;
  userver::utils::PeriodicTask ticker;
  bool started{false};
};

std::shared_ptr<LibcronDataSource> LibcronDataSource::make(
    IServiceEnvironment& environment, int connectorId) {
  return std::shared_ptr<LibcronDataSource>(
      new LibcronDataSource(environment, connectorId));
}

LibcronDataSource::LibcronDataSource(IServiceEnvironment& environment,
                                     int connectorId)
    : impl_(std::make_unique<Impl>(environment, connectorId)) {}

LibcronDataSource::~LibcronDataSource() = default;

void LibcronDataSource::addEndpoint(std::shared_ptr<Endpoint> endpoint) {
  if (!endpoint) throw std::invalid_argument("cron endpoint is null");
  if (impl_->started) {
    throw std::logic_error("cron endpoints must be added before start");
  }
  for (const auto& existing : impl_->endpoints) {
    if (existing->id() == endpoint->id()) {
      throw std::logic_error("duplicate cron endpoint: " + endpoint->name());
    }
  }
  const auto cfg = EndpointConfig(impl_->environment, endpoint->id());
  if (cfg.idDataConnector != impl_->connectorId) {
    throw std::invalid_argument("cron endpoint references another connector");
  }
  impl_->endpoints.push_back(std::move(endpoint));
}

void LibcronDataSource::start(Context context) {
  static_cast<void>(context);
  if (impl_->started) {
    throw std::logic_error("cron data source is already started");
  }
  try {
    for (const auto& endpoint : impl_->endpoints) {
      const auto expression = endpoint->impl_->configure();
      if (!expression) continue;
      if (!impl_->scheduler.add_schedule(
              endpoint->name(), *expression,
              [endpoint](const libcron::TaskInformation& information) {
                endpoint->impl_->fire(information);
              })) {
        throw std::invalid_argument("invalid cron schedule for endpoint " +
                                    endpoint->name());
      }
    }
    userver::utils::PeriodicTask::Settings settings{
        std::chrono::milliseconds{500}};
    impl_->ticker.Start(impl_->connectorName + "-cron", settings,
                        [this] { impl_->scheduler.tick(); });
    impl_->started = true;
  } catch (...) {
    impl_->ticker.Stop();
    impl_->scheduler.clear_schedules();
    for (const auto& endpoint : impl_->endpoints) endpoint->impl_->stop();
    throw;
  }
}

void LibcronDataSource::stop(Context context) {
  static_cast<void>(context);
  if (!impl_->started) return;
  impl_->ticker.Stop();
  impl_->scheduler.clear_schedules();
  for (const auto& endpoint : impl_->endpoints) endpoint->impl_->stop();
  impl_->started = false;
}

int LibcronDataSource::id() const noexcept { return impl_->connectorId; }
const std::string& LibcronDataSource::getName() const noexcept {
  return impl_->connectorName;
}

}  // namespace servicelib::datasource::cron

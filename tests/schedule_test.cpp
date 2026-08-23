#include <array>
#include <chrono>

#include <userver/utest/utest.hpp>

#include <servicelib/runtime/schedule.hpp>

UTEST(Schedule, IdentityIsStableAcrossRetries) {
  using namespace std::chrono;
  const auto scheduled = sys_days{year{2026} / August / 24} + 12h + 30min +
                         123456us;
  const auto first = servicelib::MakeScheduleTrigger(
      17, "hourly", scheduled, scheduled + 1ms,
      servicelib::ScheduleBackend::kTemporal);
  const auto retry = servicelib::MakeScheduleTrigger(
      17, "hourly", scheduled, scheduled + 1s,
      servicelib::ScheduleBackend::kTemporal);
  EXPECT_EQ(first.triggerId, retry.triggerId);
  EXPECT_EQ(first.triggerId,
            "29b272e3eeee0c67fe5b5a121f8f39d4b5d9625d656e8a0ec7f2b0f1615e2914");
}

UTEST(Schedule, TemporalPriorityIsBounded) {
  constexpr std::array input{-100, -2, -1, 0, 1, 2, 100};
  constexpr std::array expected{1, 1, 2, 3, 4, 5, 5};
  std::array<int, input.size()> actual{};
  for (std::size_t index = 0; index < input.size(); ++index) {
    actual[index] = servicelib::NormalizeTemporalPriority(input[index]);
  }
  EXPECT_EQ(actual, expected);
}

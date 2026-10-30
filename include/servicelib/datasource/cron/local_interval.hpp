#pragma once

#include <chrono>
#include <optional>
#include <stdexcept>
#include <string_view>

namespace servicelib::datasource::cron::detail {

// Local schedules only. Calendar parsing remains the provider's responsibility.
inline std::optional<std::chrono::milliseconds> ParseLocalInterval(
    std::string_view expression) {
  if (!expression.starts_with("@every")) return std::nullopt;
  const auto invalid = [] {
    throw std::invalid_argument(
        "local interval must be @every Nms with N between 10 and 86400000");
  };
  if (!expression.starts_with("@every ") || !expression.ends_with("ms") ||
      expression.size() < 10 || expression.size() > 17) {
    invalid();
  }
  const auto digits = expression.substr(7, expression.size() - 9);
  if (digits.empty() || digits.front() == '0') invalid();
  int milliseconds = 0;
  for (const char digit : digits) {
    if (digit < '0' || digit > '9') invalid();
    milliseconds = milliseconds * 10 + (digit - '0');
  }
  if (milliseconds < 10 || milliseconds > 86400000) invalid();
  return std::chrono::milliseconds{milliseconds};
}

class LocalInterval final {
 public:
  using Clock = std::chrono::system_clock;
  struct Due final {
    Clock::time_point scheduledAt;
    bool missed;
  };

  LocalInterval(std::chrono::milliseconds period, Clock::time_point anchor)
      : period_(period),
        next_(std::chrono::floor<std::chrono::milliseconds>(anchor) + period) {
    if (period.count() < 10 || period.count() > 86400000) {
      throw std::invalid_argument("local interval is outside supported bounds");
    }
  }

  std::optional<Due> takeDue(Clock::time_point now) {
    if (now < next_) return std::nullopt;
    const auto skipped = (now - next_) / period_;
    const auto latest = next_ + period_ * skipped;
    next_ = latest + period_;
    return Due{latest, skipped != 0};
  }

  std::chrono::milliseconds delay(Clock::time_point now) const {
    if (now >= next_) return std::chrono::milliseconds::zero();
    return std::chrono::ceil<std::chrono::milliseconds>(next_ - now);
  }

  std::chrono::milliseconds period() const { return period_; }

 private:
  std::chrono::milliseconds period_;
  Clock::time_point next_;
};

}  // namespace servicelib::datasource::cron::detail

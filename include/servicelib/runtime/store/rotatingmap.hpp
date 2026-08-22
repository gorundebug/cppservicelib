/*
 * rotatingmap.hpp
 * C++ streams API — two-generation rotating map.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <array>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <utility>

#include <userver/concurrent/background_task_storage_fwd.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/condition_variable.hpp>
#include <userver/engine/mutex.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/engine/task/cancel.hpp>
#include <userver/engine/task/current_task.hpp>
#include <userver/engine/task/task_with_result.hpp>
#include <userver/utils/assert.hpp>
#include <userver/utils/async.hpp>

#include <servicelib/runtime/pool/userver_aliases.hpp>
#include <servicelib/runtime/store/storage.hpp>

namespace servicelib::store {

inline constexpr std::size_t kRotatingMapShrinkFactor = 4;
inline constexpr std::size_t kRotatingMapShardCount = 64;
inline constexpr std::size_t kRotatingMapMinCapacity = 1'000;

// Hash map with periodic bucket-capacity reclamation after a large transient
// growth. Each shard must reach its own minimum capacity before it becomes
// eligible. No entries are expired by rotation.
template <typename K, typename V, typename Hash = std::hash<K>,
          typename Equal = std::equal_to<K>>
class RotatingMap final : public IStorage {
 public:
  using Duration = std::chrono::steady_clock::duration;

  explicit RotatingMap(Duration interval,
                       std::size_t minCapacity = kRotatingMapMinCapacity)
      : interval_(interval), minCapacity_(minCapacity) {
    if (interval_ <= Duration::zero()) {
      throw std::invalid_argument("rotating map interval must be positive");
    }
  }

  ~RotatingMap() override {
    // Lifecycle assertion only; concurrent destruction is an object-lifetime
    // violation and is not made safe by this check.
    if (state_ != State::kCreated && state_ != State::kStopped) {
      pool::utils::AbortWithStacktrace(
          "RotatingMap must be stopped before destruction");
    }
  }

  void start([[maybe_unused]] Context ctx) override {
    std::lock_guard<pool::engine::Mutex> lock(stateMutex_);
    switch (state_) {
      case State::kCreated:
        break;
      case State::kRunning:
      case State::kFailed:
        throw StoreAlreadyStartedError();
      case State::kStopping:
      case State::kStopped:
        throw StoreStoppedError();
    }

    state_ = State::kRunning;
    try {
      timerTask_ =
          pool::engine::CriticalAsyncNoTracing([this] { timerLoop(); });
    } catch (...) {
      state_ = State::kFailed;
      throw;
    }
  }

  void stop([[maybe_unused]] Context ctx) override {
    pool::engine::TaskCancellationBlocker cancellationBlocker;
    {
      std::unique_lock<pool::engine::Mutex> lock(stateMutex_);
      if (state_ == State::kStopped) {
        return;
      }
      if (state_ == State::kStopping) {
        static_cast<void>(stateChanged_.Wait(
            lock, [this] { return state_ == State::kStopped; }));
        return;
      }
      state_ = State::kStopping;
    }

    if (timerTask_.IsValid()) {
      timerTask_.SyncCancel();
    }

    std::lock_guard<pool::engine::Mutex> lock(stateMutex_);
    state_ = State::kStopped;
    stateChanged_.NotifyAll();
  }

  void set(K key, V value) {
    auto& shard = shardFor(key);
    std::lock_guard<pool::engine::Mutex> lock(shard.mutex);
    if (shard.current.contains(key) || shard.previous.contains(key)) {
      throw DuplicateKeyError();
    }
    shard.current.emplace(std::move(key), std::move(value));
  }

  // Atomically returns the existing value for key (checking current then
  // previous, without moving it), or inserts and returns one newly
  // constructed via factory. loaded reports whether an existing value was
  // found. factory must be cheap and non-blocking: it runs while the map's
  // internal lock is held, so callers must not perform I/O or other
  // blocking/coroutine-suspending work inside it.
  template <typename Factory>
  [[nodiscard]] std::pair<V, bool> getOrCreate(const K& key,
                                               Factory&& factory) {
    auto& shard = shardFor(key);
    std::lock_guard<pool::engine::Mutex> lock(shard.mutex);
    if (const auto it = shard.current.find(key); it != shard.current.end()) {
      return {it->second, true};
    }
    if (const auto it = shard.previous.find(key); it != shard.previous.end()) {
      return {it->second, true};
    }
    V value = std::forward<Factory>(factory)();
    shard.current.emplace(key, value);
    return {std::move(value), false};
  }

  [[nodiscard]] std::optional<V> get(const K& key) const
    requires std::copy_constructible<V>
  {
    const auto& shard = shardFor(key);
    std::lock_guard<pool::engine::Mutex> lock(shard.mutex);
    if (const auto it = shard.current.find(key); it != shard.current.end()) {
      return it->second;
    }
    if (const auto it = shard.previous.find(key); it != shard.previous.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  [[nodiscard]] std::optional<V> pop(const K& key) {
    auto& shard = shardFor(key);
    std::lock_guard<pool::engine::Mutex> lock(shard.mutex);
    if (auto it = shard.current.find(key); it != shard.current.end()) {
      V value = std::move(it->second);
      shard.current.erase(it);
      return value;
    }
    if (auto it = shard.previous.find(key); it != shard.previous.end()) {
      V value = std::move(it->second);
      shard.previous.erase(it);
      return value;
    }
    return std::nullopt;
  }

  [[nodiscard]] std::size_t size() const {
    std::size_t result = 0;
    for (const auto& shard : shards_) {
      std::lock_guard<pool::engine::Mutex> lock(shard.mutex);
      result += shard.current.size() + shard.previous.size();
    }
    return result;
  }

 private:
  enum class State { kCreated, kRunning, kStopping, kStopped, kFailed };

  struct Shard {
    mutable pool::engine::Mutex mutex;
    std::unordered_map<K, V, Hash, Equal> current;
    std::unordered_map<K, V, Hash, Equal> previous;
    std::size_t highWaterMark = 0;
  };

  [[nodiscard]] Shard& shardFor(const K& key) {
    return shards_[hash_(key) % shards_.size()];
  }

  [[nodiscard]] const Shard& shardFor(const K& key) const {
    return shards_[hash_(key) % shards_.size()];
  }

  void timerLoop() {
    while (!pool::engine::current_task::ShouldCancel()) {
      pool::engine::InterruptibleSleepFor(interval_);
      if (pool::engine::current_task::ShouldCancel()) {
        return;
      }
      rotate();
    }
  }

  void rotate() {
    {
      std::lock_guard<pool::engine::Mutex> lock(stateMutex_);
      if (state_ != State::kRunning) {
        return;
      }
    }
    for (auto& shard : shards_) rotateShard(shard);
  }

  void rotateShard(Shard& shard) {
    std::lock_guard<pool::engine::Mutex> lock(shard.mutex);
    const std::size_t total = shard.current.size() + shard.previous.size();
    const bool shouldRotate =
        shard.highWaterMark == 0 ||
        total < (shard.highWaterMark + kRotatingMapShrinkFactor - 1) /
                    kRotatingMapShrinkFactor;
    if (total > shard.highWaterMark) {
      shard.highWaterMark = total;
    }
    if (shard.highWaterMark < minCapacity_) {
      return;
    }
    if (!shouldRotate) {
      return;
    }

    shard.highWaterMark = total;
    decltype(shard.current) fresh;
    fresh.reserve(total);
    for (auto& [key, value] : shard.current) {
      fresh.emplace(key, std::move(value));
    }
    for (auto& [key, value] : shard.previous) {
      fresh.try_emplace(key, std::move(value));
    }
    shard.previous = std::move(fresh);
    shard.current = decltype(shard.current){};
  }

  Duration interval_;
  std::size_t minCapacity_;
  mutable pool::engine::Mutex stateMutex_;
  pool::engine::ConditionVariable stateChanged_;
  std::array<Shard, kRotatingMapShardCount> shards_;
  Hash hash_;
  State state_ = State::kCreated;
  pool::engine::TaskWithResult<void> timerTask_;
};

template <typename K, typename V, typename Hash = std::hash<K>,
          typename Equal = std::equal_to<K>>
std::unique_ptr<RotatingMap<K, V, Hash, Equal>> makeRotatingMap(
    typename RotatingMap<K, V, Hash, Equal>::Duration interval) {
  return std::make_unique<RotatingMap<K, V, Hash, Equal>>(interval);
}

}  // namespace servicelib::store

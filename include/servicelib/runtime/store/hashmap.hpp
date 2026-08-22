/*
 * hashmap.hpp
 * C++ streams API — in-memory hash-map join storage with optional TTL.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <userver/concurrent/background_task_storage.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/condition_variable.hpp>
#include <userver/engine/mutex.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/engine/task/cancel.hpp>
#include <userver/engine/task/current_task.hpp>
#include <userver/engine/task/task_with_result.hpp>
#include <userver/utils/assert.hpp>
#include <userver/utils/async.hpp>

#include <servicelib/api/serviceapi.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/pool/userver_aliases.hpp>
#include <servicelib/runtime/store/joinstore.hpp>
#include <servicelib/runtime/store/rotatingmap.hpp>

namespace servicelib::store {

template <typename K, typename Hash = std::hash<K>,
          typename Equal = std::equal_to<K>>
class HashMapJoinStorage final : public IJoinStorage<K> {
 public:
  using Duration = JoinStorageConfig::Duration;

  HashMapJoinStorage(IServiceEnvironment& env, JoinStorageConfig config)
      : env_(env),
        config_(std::move(config)),
        metricsEnabled_(env.getMetrics().enabled()) {
    const auto serviceSnapshot = env_.getServiceConfigSnapshot();
    const auto* service = serviceSnapshot.get();
    auto scope = env_.getMetrics().scope(
        "hashmap_join_storage",
        metrics::Labels{{"service", service ? service->name : std::string()},
                        {"name", config_.name}});
    gaugeCount_ =
        scope->gauge("count", "Elements count stored in a join storage");
    evictionsTotal_ = scope->counter(
        "evictions_total",
        "Total number of items evicted from join storage by TTL");
  }

  ~HashMapJoinStorage() override {
    // Lifecycle assertion only; concurrent destruction is an object-lifetime
    // violation and is not made safe by this check.
    if (state_ != State::kCreated && state_ != State::kStopped) {
      pool::utils::AbortWithStacktrace(
          "HashMapJoinStorage must be stopped before destruction");
    }
  }

  void start([[maybe_unused]] Context ctx) override {
    std::lock_guard<pool::engine::Mutex> lock(mu_);
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

    try {
      expiryTasks_.emplace(pool::engine::current_task::GetTaskProcessor());
      state_ = State::kRunning;
      if (config_.ttl > Duration::zero()) {
        rotationTask_ =
            pool::engine::CriticalAsyncNoTracing([this] { rotationLoop(); });
      }
    } catch (...) {
      state_ = State::kFailed;
      throw;
    }
  }

  void stop([[maybe_unused]] Context ctx) override {
    pool::engine::TaskCancellationBlocker cancellationBlocker;
    std::vector<std::shared_ptr<Item>> items;
    {
      std::unique_lock<pool::engine::Mutex> lock(mu_);
      if (state_ == State::kStopped) {
        return;
      }
      if (state_ == State::kStopping) {
        static_cast<void>(stateChanged_.Wait(
            lock, [this] { return state_ == State::kStopped; }));
        return;
      }
      state_ = State::kStopping;
      static_cast<void>(
          stateChanged_.Wait(lock, [this] { return activeOperations_ == 0; }));

      items.reserve(current_.size() + previous_.size());
      for (const auto& [unused, item] : current_) {
        items.push_back(item);
      }
      for (const auto& [unused, item] : previous_) {
        if (std::find(items.begin(), items.end(), item) == items.end()) {
          items.push_back(item);
        }
      }
    }

    // Do not hold mu_ while unregistering callbacks: a callback already in
    // flight may be waiting for mu_ in scheduleExpiryNow().
    for (const auto& item : items) {
      std::lock_guard<pool::engine::Mutex> itemLock(item->mu);
      item->cancelCallback.reset();
    }

    if (rotationTask_.IsValid()) {
      rotationTask_.SyncCancel();
    }
    if (expiryTasks_) {
      expiryTasks_->CancelAndWait();
    }

    {
      std::lock_guard<pool::engine::Mutex> lock(mu_);
      current_.clear();
      previous_.clear();
      count_ = 0;
      publishCountLocked();
      state_ = State::kStopped;
      stateChanged_.NotifyAll();
    }
  }

  void joinValue(Context ctx, K key, std::size_t index, std::any value,
                 JoinValueFunction callback) override {
    OperationGuard operation(*this);

    const auto now = std::chrono::steady_clock::now();
    Duration ttl = config_.ttl;
    const auto contextDeadline = ctx.deadline();
    if (contextDeadline.has_value()) {
      ttl = *contextDeadline - now;
    }

    while (true) {
      bool inPrevious = false;
      auto item = findItem(key, inPrevious);
      bool newlyInserted = false;
      std::unique_lock<pool::engine::Mutex> itemLock;
      if (!item) {
        auto candidate = std::make_shared<Item>();
        candidate->values.resize(index + 1);
        candidate->onExpire = callback;
        if (ttl > Duration::zero()) {
          // Config TTL is relative to each creation attempt, as in Go's
          // time.Now().Add(ttl). A Context deadline remains absolute: unlike a
          // Go context, this C++ Context does not automatically request its
          // stop_token when the time point elapses.
          candidate->deadline =
              contextDeadline.has_value()
                  ? *contextDeadline
                  : saturatedAdd(std::chrono::steady_clock::now(), ttl);
        }

        std::unique_lock<pool::engine::Mutex> candidateLock(candidate->mu);
        {
          std::lock_guard<pool::engine::Mutex> lock(mu_);
          if (const auto existing = findItemLocked(key, inPrevious)) {
            item = existing;
          } else {
            item = candidate;
            current_.emplace(key, item);
            ++count_;
            publishCountLocked();
            newlyInserted = true;
          }
        }

        if (newlyInserted) {
          itemLock = std::move(candidateLock);
          if (item->deadline.has_value()) {
            const std::uint64_t generation = ++item->generation;
            try {
              // Go arms time.AfterFunc/context.AfterFunc before publishing the
              // value to f(values). Preserve that commit boundary: if initial
              // scheduling fails, no value or callback result was accepted.
              armExpiryLocked(key, item, ctx, generation, *item->deadline);
            } catch (...) {
              item->processed = true;
              item->cancelCallback.reset();
              itemLock.unlock();
              removeIfSame(key, item);
              throw;
            }
          }
        }
      }

      if (!itemLock.owns_lock()) {
        itemLock = std::unique_lock<pool::engine::Mutex>(item->mu);
      }
      const auto itemNow = std::chrono::steady_clock::now();
      if (item->processed) {
        itemLock.unlock();
        removeIfSame(key, item);
        continue;
      }
      if (!newlyInserted && item->deadline.has_value() &&
          *item->deadline <= itemNow) {
        item->processed = true;
        ++item->generation;
        auto expiryCallback = item->onExpire;
        item->cancelCallback.reset();
        itemLock.unlock();
        bestEffort([&expiryCallback, &item] { expiryCallback(item->values); });
        removeIfSame(key, item, true);
        continue;
      }

      if (ttl > Duration::zero() && config_.renewTtl && !newlyInserted) {
        const auto previousDeadline = item->deadline;
        const std::uint64_t previousGeneration = item->generation;
        const std::uint64_t previousArmedGeneration =
            item->armedGeneration.load(std::memory_order_relaxed);
        const std::uint64_t previousCancelledGeneration =
            item->cancelledGeneration.load(std::memory_order_relaxed);
        item->deadline = contextDeadline.has_value()
                             ? *contextDeadline
                             : saturatedAdd(itemNow, ttl);
        const std::uint64_t generation = ++item->generation;
        item->cancelCallback.reset();
        try {
          // Renewal is part of the operation's commit boundary. Schedule it
          // before accepting value so a scheduling failure cannot be reported
          // after the callback has observed a partially committed join.
          armExpiryLocked(key, item, ctx, generation, *item->deadline);
        } catch (...) {
          item->cancelCallback.reset();
          item->deadline = previousDeadline;
          item->generation = previousGeneration;
          item->armedGeneration.store(previousArmedGeneration,
                                      std::memory_order_relaxed);
          item->cancelledGeneration.store(previousCancelledGeneration,
                                          std::memory_order_relaxed);
          throw;
        }

        std::lock_guard<pool::engine::Mutex> lock(mu_);
        if (const auto it = previous_.find(key);
            it != previous_.end() && it->second == item) {
          previous_.erase(it);
          current_[key] = item;
        }
      }

      if (item->values.size() <= index) {
        item->values.resize(index + 1);
      }
      item->values[index].push_back(std::move(value));
      item->processed = callback(item->values);

      if (item->processed) {
        ++item->generation;
        item->cancelCallback.reset();
        itemLock.unlock();
        removeIfSame(key, item);
        return;
      }
      return;
    }
  }

  [[nodiscard]] std::size_t size() const {
    std::lock_guard<pool::engine::Mutex> lock(mu_);
    return static_cast<std::size_t>(count_);
  }

 private:
  enum class State { kCreated, kRunning, kStopping, kStopped, kFailed };

  struct Item {
    pool::engine::Mutex mu;
    JoinValues values;
    std::optional<std::chrono::steady_clock::time_point> deadline;
    bool processed = false;
    std::uint64_t generation = 0;
    std::atomic<std::uint64_t> armedGeneration{0};
    std::atomic<std::uint64_t> cancelledGeneration{0};
    JoinValueFunction onExpire;
    std::optional<std::stop_callback<std::function<void()>>> cancelCallback;
  };

  class OperationGuard final {
   public:
    explicit OperationGuard(HashMapJoinStorage& storage) : storage_(storage) {
      std::lock_guard<pool::engine::Mutex> lock(storage_.mu_);
      if (storage_.state_ != State::kRunning) {
        if (storage_.state_ == State::kStopping ||
            storage_.state_ == State::kStopped) {
          throw StoreStoppedError();
        }
        throw StoreNotStartedError();
      }
      ++storage_.activeOperations_;
    }

    ~OperationGuard() {
      std::lock_guard<pool::engine::Mutex> lock(storage_.mu_);
      --storage_.activeOperations_;
      storage_.stateChanged_.NotifyAll();
    }

    OperationGuard(const OperationGuard&) = delete;
    OperationGuard& operator=(const OperationGuard&) = delete;

   private:
    HashMapJoinStorage& storage_;
  };

  template <typename Callback>
  static void bestEffort(Callback&& callback) noexcept {
    try {
      std::forward<Callback>(callback)();
    } catch (...) {
    }
  }

  static std::chrono::steady_clock::time_point saturatedAdd(
      std::chrono::steady_clock::time_point now, Duration ttl) {
    if (ttl <= Duration::zero()) {
      return now;
    }
    const auto maxTtl = std::chrono::steady_clock::time_point::max() - now;
    return ttl >= maxTtl ? std::chrono::steady_clock::time_point::max()
                         : now + ttl;
  }

  static void publishMax(std::atomic<std::uint64_t>& target,
                         std::uint64_t value) noexcept {
    std::uint64_t observed = target.load(std::memory_order_relaxed);
    while (observed < value && !target.compare_exchange_weak(
                                   observed, value, std::memory_order_release,
                                   std::memory_order_relaxed)) {
    }
  }

  std::shared_ptr<Item> findItem(const K& key, bool& inPrevious) const {
    std::lock_guard<pool::engine::Mutex> lock(mu_);
    return findItemLocked(key, inPrevious);
  }

  std::shared_ptr<Item> findItemLocked(const K& key, bool& inPrevious) const {
    if (const auto it = current_.find(key); it != current_.end()) {
      inPrevious = false;
      return it->second;
    }
    if (const auto it = previous_.find(key); it != previous_.end()) {
      inPrevious = true;
      return it->second;
    }
    inPrevious = false;
    return {};
  }

  void publishCountLocked() noexcept {
    if (!metricsEnabled_) return;
    bestEffort([this] { gaugeCount_->set(count_); });
  }

  void removeIfSame(const K& key, const std::shared_ptr<Item>& item,
                    bool eviction = false) {
    bool removed = false;
    {
      std::lock_guard<pool::engine::Mutex> lock(mu_);
      if (const auto it = current_.find(key);
          it != current_.end() && it->second == item) {
        current_.erase(it);
        removed = true;
      } else if (const auto it = previous_.find(key);
                 it != previous_.end() && it->second == item) {
        previous_.erase(it);
        removed = true;
      }
      if (removed) {
        --count_;
        publishCountLocked();
      }
    }
    if (removed && eviction && metricsEnabled_) {
      bestEffort([this] { evictionsTotal_->inc(); });
    }
  }

  void armExpiryLocked(const K& key, const std::shared_ptr<Item>& item,
                       const Context& ctx, std::uint64_t generation,
                       std::chrono::steady_clock::time_point deadline) {
    std::weak_ptr<Item> weakItem(item);
    item->cancelCallback.emplace(
        ctx.stopToken(), [this, key, weakItem, generation]() noexcept {
          if (const auto locked = weakItem.lock()) {
            publishMax(locked->cancelledGeneration, generation);
            if (locked->armedGeneration.load(std::memory_order_acquire) >=
                generation) {
              scheduleExpiryNow(key, weakItem, generation);
            }
          }
        });

    {
      std::lock_guard<pool::engine::Mutex> lock(mu_);
      // OperationGuard admits work only in kRunning, and stop() keeps
      // expiryTasks_ alive until all admitted operations finish. Therefore an
      // operation that observes kStopping here must still be allowed to
      // complete its expiry commit, matching Go's rotateLock serialization.
      if (state_ != State::kRunning && state_ != State::kStopping) {
        throw StoreStoppedError();
      }
      expiryTasks_->CriticalAsyncDetach(
          config_.name + "-join-item-ttl",
          [this, key, weakItem, generation, deadline] {
            pool::engine::InterruptibleSleepUntil(deadline);
            if (pool::engine::current_task::ShouldCancel()) {
              return;
            }
            expire(key, weakItem, generation);
          });
    }
    item->armedGeneration.store(generation, std::memory_order_release);
    if (item->cancelledGeneration.load(std::memory_order_acquire) >=
        generation) {
      scheduleExpiryNow(key, weakItem, generation);
    }
  }

  void scheduleExpiryNow(const K& key, const std::weak_ptr<Item>& item,
                         std::uint64_t generation) noexcept {
    try {
      std::lock_guard<pool::engine::Mutex> lock(mu_);
      if (state_ != State::kRunning) {
        return;
      }
      expiryTasks_->CriticalAsyncDetach(
          config_.name + "-join-item-context-done",
          [this, key, item, generation] {
            if (pool::engine::current_task::ShouldCancel()) {
              return;
            }
            expire(key, item, generation);
          });
    } catch (...) {
      // Exceptions must not escape std::stop_callback. The TTL coroutine
      // remains armed and will eventually process the item.
    }
  }

  void expire(const K& key, const std::weak_ptr<Item>& weakItem,
              std::uint64_t generation) {
    const auto item = weakItem.lock();
    if (!item) {
      return;
    }

    JoinValueFunction callback;
    {
      std::lock_guard<pool::engine::Mutex> itemLock(item->mu);
      if (item->processed || item->generation != generation) {
        return;
      }
      item->processed = true;
      callback = item->onExpire;
      item->cancelCallback.reset();
    }

    bestEffort([&callback, &item] { callback(item->values); });
    removeIfSame(key, item, true);
  }

  void rotationLoop() {
    while (!pool::engine::current_task::ShouldCancel()) {
      pool::engine::InterruptibleSleepFor(config_.ttl);
      if (pool::engine::current_task::ShouldCancel()) {
        return;
      }
      rotate();
    }
  }

  void rotate() {
    std::int64_t evicted = 0;
    {
      std::lock_guard<pool::engine::Mutex> lock(mu_);
      if (state_ != State::kRunning) {
        return;
      }
      const std::size_t total = current_.size() + previous_.size();
      const bool shouldRotate =
          highWaterMark_ == 0 ||
          total < (highWaterMark_ + kRotatingMapShrinkFactor - 1) /
                      kRotatingMapShrinkFactor;
      highWaterMark_ = std::max(highWaterMark_, total);
      if (!shouldRotate) {
        return;
      }

      highWaterMark_ = total;
      std::size_t rescued = 0;
      for (auto& [key, item] : previous_) {
        if (current_.try_emplace(key, item).second) {
          ++rescued;
        }
      }
      evicted = static_cast<std::int64_t>(previous_.size() - rescued);
      // Rotation is a capacity-reclamation mechanism; exact TTL eviction is
      // handled by per-item expiry tasks. Move the live generation into
      // previous_ and replace current_ with a genuinely fresh map so the old
      // bucket allocation is released instead of being swapped back.
      previous_ = std::move(current_);
      current_ = decltype(current_){};
      count_ -= evicted;
      publishCountLocked();
    }
    if (evicted > 0 && metricsEnabled_) {
      bestEffort([this, evicted] { evictionsTotal_->add(evicted); });
    }
  }

  IServiceEnvironment& env_;
  JoinStorageConfig config_;
  bool metricsEnabled_{};
  mutable pool::engine::Mutex mu_;
  pool::engine::ConditionVariable stateChanged_;
  std::unordered_map<K, std::shared_ptr<Item>, Hash, Equal> current_;
  std::unordered_map<K, std::shared_ptr<Item>, Hash, Equal> previous_;
  std::int64_t count_ = 0;
  std::size_t highWaterMark_ = 0;
  std::size_t activeOperations_ = 0;
  State state_ = State::kCreated;
  pool::engine::TaskWithResult<void> rotationTask_;

  std::unique_ptr<metrics::Int64Gauge> gaugeCount_;
  std::unique_ptr<metrics::Int64Counter> evictionsTotal_;

  // Must be destroyed before every field captured by expiry coroutines.
  std::optional<pool::concurrent::BackgroundTaskStorage> expiryTasks_;
};

template <typename K, typename Hash = std::hash<K>,
          typename Equal = std::equal_to<K>>
std::unique_ptr<IJoinStorage<K>> makeHashMapJoinStorage(
    IServiceEnvironment& env, JoinStorageConfig config) {
  return std::make_unique<HashMapJoinStorage<K, Hash, Equal>>(
      env, std::move(config));
}

template <typename K, typename Hash = std::hash<K>,
          typename Equal = std::equal_to<K>>
std::unique_ptr<IJoinStorage<K>> makeJoinStorage(api::JoinStorageType type,
                                                 IServiceEnvironment& env,
                                                 JoinStorageConfig config) {
  switch (type) {
    case api::JoinStorageType::kHashMap:
      return makeHashMapJoinStorage<K, Hash, Equal>(env, std::move(config));
    case api::JoinStorageType::kUndefined:
    case api::JoinStorageType::kRocksDB:
    case api::JoinStorageType::kAerospike:
      throw UnsupportedStoreError();
  }
  throw UnsupportedStoreError();
}

}  // namespace servicelib::store

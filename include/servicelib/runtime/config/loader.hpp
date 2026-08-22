/*
 * loader.hpp
 * C++ streams API — YAML config loader with hot-reload, built on userver's
 * native yaml_config::YamlConfig substitution instead of a hand-rolled one.
 *
 * Mirrors servicelib's Go implementation (runtime/runtime.go's
 * serviceLoader.init, viper-based) — deliberately narrower than an earlier
 * version of this file, which also ported pyservicelib's separate
 * values-file placeholder layer. That layer doesn't exist in Go, and the
 * user directed this port to follow Go's architecture, not Python's:
 *   1. Read a mandatory base config file.
 *   2. Optionally deep-merge an override file on top (Go: viper.MergeConfig).
 *   3. Parse the result into the generated ConcreteConfig type and build a
 *      RuntimeConfig from it.
 *   4. If an override path was given, periodically re-check its mtime and
 *      contents (for filesystems with coarse timestamp resolution), then
 *      repeat 1-3 (re-reading *both* files, exactly like Go's fsnotify
 *      handler does) on change, publishing the new RuntimeConfig without
 *      disrupting whatever's already running.
 *
 * $var/#env/#fallback/#file substitution is userver's own
 * yaml_config::YamlConfig — not reimplemented here. Both the base and
 * override files are wrapped in a YamlConfig sharing the *same*
 * config_vars as the rest of the service (see ConfigVars(), sourced from
 * the owning component's own ComponentConfig::GetRawConfigVars()), then
 * flattened via `.As<formats::yaml::Value>()` before DeepMerge and
 * Parse<ConcreteConfig> — see yaml_config::YamlConfig's file comment for
 * why that flattening step is what "applies" the substitutions.
 *
 * Departure from Go: hot-reload here is poll-based
 * (utils::PeriodicTask checking the override file), not push-based
 * (fsnotify). userver's OSS core has no cross-platform file-watch
 * primitive (engine::io::sys_linux::inotify exists but is Linux-only);
 * polling is also what TaskPoolImpl/PriorityTaskPoolImpl already do for
 * PoolConfig.executorsCount (managerLoop re-reads the runtime config snapshot
 * tick), so this keeps the whole port consistent on one freshness model.
 * As a side benefit, mtime polling on the watched path handles Kubernetes
 * ConfigMap's atomic symlink-swap for free (last_write_time follows the
 * symlink to whatever it currently resolves to), whereas Go's fsnotify
 * needs explicit realpath-comparison logic to catch that case because it
 * watches directory entries, not resolved content.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

#include <userver/engine/async.hpp>
#include <userver/engine/task/task_processor_fwd.hpp>
#include <userver/formats/yaml/file.hpp>
#include <userver/rcu/rcu.hpp>
#include <userver/utils/periodic_task.hpp>
#include <userver/yaml_config/yaml_config.hpp>

#include <servicelib/runtime/config/config.hpp>
#include <servicelib/runtime/config/config_substitution.hpp>
#include <servicelib/runtime/environment/log/log.hpp>
#include <servicelib/runtime/environment/metrics/metrics.hpp>

namespace servicelib::config {

// Generated config code provides these two ADL customization points:
//
//   ConcreteConfig MakeConfig(formats::parse::To<ConcreteConfig>);
//   void ApplyConfig(const formats::yaml::Value&, ConcreteConfig&);
//   void ApplyEnvironment(ConcreteConfig&);
//
// This mirrors Go's configMaker() + viper.Unmarshal(existingConfig): every
// load starts from the generated topology and applies the YAML as a patch.
template <typename ConcreteConfig>
struct GeneratedConfigAdapter {
  static ConcreteConfig Make() {
    return MakeConfig(userver::formats::parse::To<ConcreteConfig>{});
  }

  static void Apply(const userver::formats::yaml::Value& value,
                    ConcreteConfig& config) {
    ApplyConfig(value, config);
  }

  static void Finalize(ConcreteConfig& config) { ApplyEnvironment(config); }
};

// Precondition for GetRuntimeConfig()/GetConfig(): Load() must have been
// called (and returned normally) at least once first. Matches Go's
// serviceLoader, which builds the running service around the *result* of
// the first load rather than exposing a config accessor before one exists.
template <typename ConcreteConfig,
          typename ConfigAdapter = GeneratedConfigAdapter<ConcreteConfig>>
class ConfigLoader final {
  static_assert(std::is_base_of_v<IConfig, ConcreteConfig>,
                "ConcreteConfig must derive from servicelib::config::IConfig");

 public:
  struct Paths {
    std::string configPath;
    std::optional<std::string> overridePath;
  };

  // Invoked with the newly built RuntimeConfig after every successful
  // reload (not for the initial Load()). Go analog:
  // service.GetRuntime().updateConfig().
  using ReloadCallback =
      std::function<void(std::shared_ptr<const RuntimeConfig>)>;

  // `configVars` should be the same config_vars document the rest of the
  // service resolves $var/#env/#fallback against — typically
  // `componentConfig.GetRawConfigVars()` from the owning component, so our
  // topology file's $var references share one namespace with
  // static_config.yaml's.
  ConfigLoader(Paths paths, userver::formats::yaml::Value configVars,
               log::Logger& logger, metrics::Metrics& metrics,
               std::string serviceName,
               userver::engine::TaskProcessor* fileTaskProcessor = nullptr)
      : paths_(std::move(paths)),
        configVars_(std::move(configVars)),
        logger_(logger),
        metrics_(metrics),
        serviceName_(std::move(serviceName)),
        fileTaskProcessor_(fileTaskProcessor) {}

  ~ConfigLoader() {
    // Must happen before this object's other members (in particular
    // logger_/reload*Counter_, which the poller's callback captures via
    // `this`) become invalid — see utils::PeriodicTask::Stop()'s
    // documented requirement.
    poller_.Stop();
  }

  ConfigLoader(const ConfigLoader&) = delete;
  ConfigLoader& operator=(const ConfigLoader&) = delete;

  // Synchronous first load. Throws on any failure (missing/malformed file,
  // ConcreteConfig parse failure, duplicate name/id while building
  // RuntimeConfig). Mirrors Go's serviceLoader.init: the very first load
  // is fatal-on-error, not something a reload-style log-and-keep-serving
  // path should cover.
  std::shared_ptr<const RuntimeConfig> Load() {
    auto load = [this] {
      auto loaded = std::make_shared<const LoadedConfig>(LoadOnce());
      RecordWatchedMtime();
      return loaded;
    };
    auto loaded = fileTaskProcessor_ ? userver::engine::AsyncNoTracing(
                                           *fileTaskProcessor_, std::move(load))
                                           .Get()
                                     : load();
    initializeServiceMetrics(loaded->runtimeConfig);
    state_.Assign(loaded);
    return RuntimeConfigView(loaded);
  }

  // Starts periodic mtime polling of the override file (if one was given
  // in Paths — a no-op otherwise, matching Go only starting its watch
  // goroutine when an override file is actually configured).
  void Start(std::chrono::milliseconds pollInterval, ReloadCallback onReload) {
    if (paths_.configPath.empty() || !paths_.overridePath) {
      return;
    }
    // Stop before replacing onReload_: PeriodicTask::Start() also restarts
    // internally, but assigning the callback first would race a PollOnce()
    // already executing on the file task processor.
    poller_.Stop();
    onReload_ = std::move(onReload);
    userver::utils::PeriodicTask::Settings settings{pollInterval};
    settings.task_processor = fileTaskProcessor_;
    poller_.Start(serviceName_ + "-config-reload", settings,
                  [this] { PollOnce(); });
  }

  void Stop() { poller_.Stop(); }

  // Current RuntimeConfig snapshot. Safe to call from any coroutine at any
  // time; the returned shared_ptr keeps the whole underlying LoadedConfig
  // (including the ConcreteConfig it points into — see LoadedConfig's
  // comment) alive independently of subsequent reloads.
  std::shared_ptr<const RuntimeConfig> GetRuntimeConfig() const {
    const auto state = state_.Read();
    return RuntimeConfigView(*state);
  }

  // Current typed config snapshot. Go analog: RuntimeConfig.GetConfig().
  std::shared_ptr<const ConcreteConfig> GetConfig() const {
    const auto state = state_.Read();
    auto loaded = *state;
    return std::shared_ptr<const ConcreteConfig>(loaded, &loaded->config);
  }

 private:
  // Bundles the parsed ConcreteConfig with the RuntimeConfig built over
  // it: RuntimeConfig stores a *reference* to its IConfig (see
  // config.hpp), so the two must share exactly one lifetime. Declaration
  // order matters — config is fully constructed before runtimeConfig's
  // member-initializer runs.
  struct LoadedConfig {
    explicit LoadedConfig(ConcreteConfig cfg)
        : config(std::move(cfg)), runtimeConfig(config) {}

    ConcreteConfig config;
    RuntimeConfig runtimeConfig;
  };

  static std::shared_ptr<const RuntimeConfig> RuntimeConfigView(
      const std::shared_ptr<const LoadedConfig>& loaded) {
    // Aliasing constructor: shares LoadedConfig's ownership/refcount, but
    // the returned pointer targets its runtimeConfig subobject.
    return std::shared_ptr<const RuntimeConfig>(loaded, &loaded->runtimeConfig);
  }

  // Wraps a freshly-read file in a YamlConfig sharing configVars_, then
  // flattens it — the flattening is what actually resolves every
  // $var/#env/#fallback/#file reference into a plain formats::yaml::Value,
  // per yaml_config::YamlConfig's documented Parse(..., To<Value>)
  // overload.
  userver::formats::yaml::Value ReadAndFlatten(const std::string& path) const {
    auto raw = userver::formats::yaml::FromFile(path);
    const userver::yaml_config::YamlConfig wrapped(
        std::move(raw), configVars_,
        userver::yaml_config::YamlConfig::Mode::kEnvAllowed);
    return wrapped.As<userver::formats::yaml::Value>();
  }

  ConcreteConfig LoadOnce() const {
    auto config = ConfigAdapter::Make();
    if (paths_.configPath.empty()) {
      ConfigAdapter::Finalize(config);
      return config;
    }
    auto flattened = ReadAndFlatten(paths_.configPath);
    if (paths_.overridePath) {
      flattened = DeepMerge(flattened, ReadAndFlatten(*paths_.overridePath));
    }
    ConfigAdapter::Apply(flattened, config);
    ConfigAdapter::Finalize(config);
    return config;
  }

  void PollOnce() {
    if (!HasOverrideChanged()) {
      return;
    }
    std::shared_ptr<const LoadedConfig> loaded;
    try {
      loaded = std::make_shared<const LoadedConfig>(LoadOnce());
      state_.Assign(loaded);
      RecordWatchedMtime();
      IncBestEffort(reloadSuccessCounter_);
    } catch (const std::exception& e) {
      // Deliberately does not update the recorded mtime here: the next
      // tick will see the same "changed" state and retry, rather than
      // silently adopting a broken file's timestamp and going quiet.
      // Mirrors Go: logs and continues serving the previously-loaded
      // config without touching it.
      try {
        logger_.warn(
            "config reload failed",
            {log::Field::Str("service", serviceName_), log::Field::Err(e)});
      } catch (...) {
      }
      IncBestEffort(reloadErrorCounter_);
      return;
    }

    if (onReload_) {
      try {
        onReload_(RuntimeConfigView(loaded));
      } catch (const std::exception& e) {
        // Publication has already succeeded. A consumer callback failure is
        // operationally separate from a config reload failure and must not
        // produce both success and error for the same reload.
        try {
          logger_.warn(
              "config reload callback failed",
              {log::Field::Str("service", serviceName_), log::Field::Err(e)});
        } catch (...) {
        }
      } catch (...) {
        try {
          logger_.warn("config reload callback failed",
                       {log::Field::Str("service", serviceName_)});
        } catch (...) {
        }
      }
    }
  }

  static void IncBestEffort(
      const std::unique_ptr<metrics::Int64Counter>& counter) noexcept {
    try {
      if (counter) {
        counter->inc();
      }
    } catch (...) {
    }
  }

  static std::string EnvironmentName(api::Environment environment) {
    switch (environment) {
      case api::Environment::kLocal:
        return "local";
      case api::Environment::kDebug:
        return "debug";
      case api::Environment::kStaging:
        return "staging";
      case api::Environment::kProduction:
        return "production";
      case api::Environment::kUndefined:
        return {};
    }
    return {};
  }

  void initializeServiceMetrics(const RuntimeConfig& runtimeConfig) {
    const auto* service = runtimeConfig.GetOnlyServiceConfig();
    if (service && !service->name.empty()) {
      serviceName_ = service->name;
    }

    auto scope =
        metrics_.scope("service", metrics::Labels{{"service", serviceName_}});
    reloadSuccessCounter_ = scope->counter(
        "config_reloads_total", "Total number of config reload attempts",
        {{"event", "success"}});
    reloadErrorCounter_ = scope->counter(
        "config_reloads_total", "Total number of config reload attempts",
        {{"event", "error"}});

    if (service) {
      auto infoScope = metrics_.scope(
          "service", metrics::Labels{{"service", serviceName_},
                                     {"environment",
                                      EnvironmentName(service->environment)}});
      serviceInfoGauge_ =
          infoScope->gauge("info", "Service information (value is always 1)");
      try {
        serviceInfoGauge_->set(1);
      } catch (...) {
        // Telemetry must not turn an otherwise valid initial config into a
        // service startup failure.
      }
    }
  }

  bool HasOverrideChanged() const {
    if (!paths_.overridePath) {
      return false;
    }
    std::error_code ec;
    const auto mtime =
        std::filesystem::last_write_time(*paths_.overridePath, ec);
    if (ec) {
      // Missing/unreadable mid-swap — not itself a "changed" signal; the
      // next successful stat will compare against the last *known-good*
      // mtime, so a transient miss here can't mask a real change.
      return false;
    }
    if (!lastOverrideMtime_.has_value() || *lastOverrideMtime_ != mtime) {
      return true;
    }
    const auto contents = ReadOverrideContents();
    return contents.has_value() && contents != lastOverrideContents_;
  }

  void RecordWatchedMtime() {
    if (!paths_.overridePath) {
      return;
    }
    std::error_code ec;
    const auto mtime =
        std::filesystem::last_write_time(*paths_.overridePath, ec);
    if (!ec) {
      lastOverrideMtime_ = mtime;
      lastOverrideContents_ = ReadOverrideContents();
    }
  }

  std::optional<std::string> ReadOverrideContents() const {
    if (!paths_.overridePath) {
      return std::nullopt;
    }
    std::ifstream input(*paths_.overridePath, std::ios::binary);
    if (!input) {
      return std::nullopt;
    }
    return std::string(std::istreambuf_iterator<char>(input),
                       std::istreambuf_iterator<char>());
  }

  Paths paths_;
  userver::formats::yaml::Value configVars_;
  log::Logger& logger_;
  metrics::Metrics& metrics_;
  std::string serviceName_;
  userver::engine::TaskProcessor* fileTaskProcessor_;

  userver::rcu::Variable<std::shared_ptr<const LoadedConfig>> state_;
  std::optional<std::filesystem::file_time_type> lastOverrideMtime_;
  std::optional<std::string> lastOverrideContents_;

  userver::utils::PeriodicTask poller_;
  ReloadCallback onReload_;

  std::unique_ptr<metrics::Int64Counter> reloadSuccessCounter_;
  std::unique_ptr<metrics::Int64Counter> reloadErrorCounter_;
  std::unique_ptr<metrics::Int64Gauge> serviceInfoGauge_;
};

}  // namespace servicelib::config

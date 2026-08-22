/*
 * component.hpp
 * C++ streams API — thin userver component wrapping ConfigLoader<T>.
 *
 * This is the registration point that makes servicelib's stream topology
 * config participate in userver's normal component lifecycle (startup
 * ordering, static-config schema validation, static_config.yaml's own
 * $var/config_vars.yaml namespace) while the topology itself keeps living
 * in a *separate* file with its own hot-reload — see loader.hpp's file
 * comment for why: static_config.yaml component sections are constructed
 * exactly once and never re-read at runtime, so embedding the topology
 * directly there would give up hot-reloading PoolConfig.executorsCount
 * and friends, which is the one thing actually relied on in practice.
 *
 * Expected static_config.yaml usage:
 *   components:
 *       servicelib-runtime:
 *           config-path: $servicelib-config-path
 *           override-path: $servicelib-override-path   # optional
 *           poll-interval: 5s                           # optional, default 5s
 *
 * Only one instantiation of this template should be registered per
 * service — kName is fixed regardless of ConcreteConfig, so two
 * instantiations in the same ComponentList would collide.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <userver/components/component_base.hpp>
#include <userver/components/component_config.hpp>
#include <userver/components/component_context.hpp>
#include <userver/components/statistics_storage.hpp>
#include <userver/yaml_config/merge_schemas.hpp>

#include <servicelib/runtime/config/loader.hpp>
#include <servicelib/runtime/telemetry/userver/log.hpp>
#include <servicelib/runtime/telemetry/userver/metrics.hpp>

namespace servicelib::config {

template <typename ConcreteConfig>
class ServicelibRuntimeComponent final
    : public userver::components::ComponentBase {
 public:
  static constexpr std::string_view kName = "servicelib-runtime";

  ServicelibRuntimeComponent(
      const userver::components::ComponentConfig& config,
      const userver::components::ComponentContext& context)
      : ComponentBase(config, context),
        metrics_(context.FindComponent<userver::components::StatisticsStorage>()
                     .GetStorage()),
        pollInterval_(
            config["poll-interval"].template As<std::chrono::milliseconds>(
                std::chrono::seconds{5})),
        loader_(
            typename ConfigLoader<ConcreteConfig>::Paths{
                config["config-path"].template As<std::string>(""),
                config["override-path"].template As<std::optional<std::string>>(
                    std::nullopt),
            },
            config.GetRawConfigVars(),
            telemetry::userver_adapter::UserverLogger::instance(), metrics_,
            config.Name(),
            &context.GetTaskProcessor(
                config["fs-task-processor"].template As<std::string>(
                    "fs-task-processor"))) {
    RuntimeConfigRegistry::Publish(loader_.Load());
    loader_.Start(pollInterval_,
                  [](std::shared_ptr<const RuntimeConfig> config) {
                    RuntimeConfigRegistry::Publish(std::move(config));
                  });
  }

  ~ServicelibRuntimeComponent() override {
    loader_.Stop();
    RuntimeConfigRegistry::Publish(nullptr);
  }

  // Replaces the reload callback registered in the constructor. Must be
  // called before Start() would otherwise fire a reload — in practice,
  // from a component that depends on this one and does its setup in its
  // own constructor or OnAllComponentsLoaded().
  void SetReloadCallback(
      typename ConfigLoader<ConcreteConfig>::ReloadCallback callback) {
    loader_.Start(pollInterval_,
                  [callback = std::move(callback)](
                      std::shared_ptr<const RuntimeConfig> config) mutable {
                    RuntimeConfigRegistry::Publish(config);
                    if (callback) {
                      callback(std::move(config));
                    }
                  });
  }

  std::shared_ptr<const RuntimeConfig> GetRuntimeConfig() const {
    return loader_.GetRuntimeConfig();
  }

  std::shared_ptr<const ConcreteConfig> GetConfig() const {
    return loader_.GetConfig();
  }

  static userver::yaml_config::Schema GetStaticConfigSchema() {
    return userver::yaml_config::MergeSchemas<
        userver::components::ComponentBase>(R"(
type: object
description: Loads and hot-reloads the servicelib stream topology config.
additionalProperties: false
properties:
    config-path:
        type: string
        description: >
            optional path to the base stream topology YAML file; when absent,
            generated topology defaults are used, matching Go configMaker()
    override-path:
        type: string
        description: >
            optional path to an override YAML file; its mtime is polled and,
            on change, both files are re-read and merged
    poll-interval:
        type: string
        description: how often to check override-path for changes
    fs-task-processor:
        type: string
        description: task processor used for blocking topology file I/O
)");
  }

 private:
  telemetry::userver_adapter::UserverMetrics metrics_;
  std::chrono::milliseconds pollInterval_;
  ConfigLoader<ConcreteConfig> loader_;
};

}  // namespace servicelib::config

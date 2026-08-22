#pragma once

#include <exception>
#include <memory>
#include <string>
#include <string_view>

#include <userver/components/component_base.hpp>
#include <userver/components/component_config.hpp>
#include <userver/components/component_context.hpp>
#include <userver/components/statistics_storage.hpp>
#include <userver/formats/json/value.hpp>
#include <userver/server/http/http_request.hpp>
#include <userver/yaml_config/merge_schemas.hpp>

#include <servicelib/datasource/http/userver.hpp>
#include <servicelib/runtime/config/component.hpp>
#include <servicelib/runtime/pool/prioritytaskpool.hpp>
#include <servicelib/runtime/serviceapp.hpp>
#include <servicelib/runtime/telemetry/userver/log.hpp>
#include <servicelib/runtime/telemetry/userver/metrics.hpp>
#include <servicelib/transformation/streams.hpp>

#include "config/config.hpp"

namespace mockservice {

struct RequestData final {
  std::string text;
};

struct DataTypes final {
  template <typename>
  struct DataType {};
};

class Service final : public servicelib::ServiceApp<Service, DataTypes> {
 public:
  explicit Service(userver::utils::statistics::Storage& statisticsStorage);
  ~Service() override;

  void start();
  void stop() noexcept;

  servicelib::log::Logger& getLogger() override;
  servicelib::metrics::Metrics& getMetrics() override;
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }

  std::shared_ptr<servicelib::datasource::http::IUserverEndpoint>
  httpDataSourceEndpoint(int endpointId) const;

 private:
  struct RequestHandler;

  void initRuntime();
  void initPools();
  void initStreams(const config::Config& config);
  void initDataSources(const config::Config& config);
  void releaseRuntime() noexcept;
  void consumeRequest(servicelib::MessageContext context, const RequestData& value);

  servicelib::telemetry::userver_adapter::UserverMetrics metrics_;
  std::shared_ptr<servicelib::pool::IPriorityTaskPool> priorityPool_;
  std::shared_ptr<servicelib::InputStream<RequestData, std::monostate,
                                      std::exception_ptr, Service>>
      input_;
  std::shared_ptr<servicelib::datasource::http::UserverEndpointConsumer<
      RequestData, std::monostate, std::exception_ptr, Service, RequestHandler>>
      endpointConsumer_;
  std::shared_ptr<servicelib::datasource::http::UserverDataSource> dataSource_;
};

using RuntimeConfigComponent =
    servicelib::config::ServicelibRuntimeComponent<config::Config>;

class MockServiceComponent final : public userver::components::ComponentBase {
 public:
  static constexpr std::string_view kName = "mockservice";

  MockServiceComponent(const userver::components::ComponentConfig& config,
                       const userver::components::ComponentContext& context);
  ~MockServiceComponent() override;

  std::shared_ptr<servicelib::datasource::http::IUserverEndpoint>
  httpDataSourceEndpoint(int endpointId) const;

  static userver::yaml_config::Schema GetStaticConfigSchema();

 private:
  Service service_;
};

class DataHandler final
    : public servicelib::datasource::http::UserverHandlerComponentBase<
          MockServiceComponent, config::kDataEndpointId> {
 public:
  static constexpr std::string_view kName = "handler-mockservice-data";
  using UserverHandlerComponentBase::UserverHandlerComponentBase;
};

}  // namespace mockservice

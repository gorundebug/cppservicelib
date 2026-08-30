#include "mockservice.hpp"

#include <stdexcept>
#include <utility>

#include <userver/formats/json/serialize.hpp>
#include <userver/formats/json/value_builder.hpp>
#include <userver/testsuite/testpoint.hpp>

namespace mockservice {

struct Service::RequestHandler final {
  using State = std::monostate;
  using Request = RequestData;
  using Response = std::monostate;

  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&, auto&) {
    return {std::move(context), {}};
  }

  void consumeMessage(servicelib::MessageContext context, auto& streamContext,
                      State&, servicelib::datasource::http::HandlerData& data,
                      auto) {
    try {
      const auto json =
          userver::formats::json::FromString(data.request.RequestBody());
      streamContext.collect(std::move(context),
                            RequestData{json["text"].As<std::string>("")});
    } catch (const std::exception&) {
      // Matches the Go mock handler: malformed input produces no stream
      // message, while the HTTP request itself is still completed normally.
    }
  }

  std::string getMessageId(servicelib::MessageContext, auto&, State&,
                           const std::monostate&) {
    return {};
  }

  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr, State&,
                  auto&) noexcept {}
};

Service::Service(userver::utils::statistics::Storage& statisticsStorage)
    : metrics_(statisticsStorage) {}

Service::~Service() { stop(); }

void Service::start() {
  try {
    initRuntime();
    servicelib::ServiceApp<Service, DataTypes>::start();
  } catch (...) {
    stop();
    releaseRuntime();
    throw;
  }
}

void Service::initRuntime() {
  initPools();

  const auto runtimeConfig = getRuntimeConfigSnapshot();
  if (!runtimeConfig) {
    throw std::runtime_error("mockservice runtime config is not published");
  }
  const auto configSnapshot = std::dynamic_pointer_cast<const config::Config>(
      std::shared_ptr<const servicelib::config::IConfig>(
          runtimeConfig, &runtimeConfig->GetConfig()));
  if (!configSnapshot) {
    throw std::runtime_error("mockservice config has unexpected type");
  }

  initStreams(*configSnapshot);
  initDataSources(*configSnapshot);
}

void Service::initPools() {
  priorityPool_ = std::shared_ptr<servicelib::pool::IPriorityTaskPool>(
      servicelib::pool::makePriorityTaskPool(config::kDefaultPriorityPoolName,
                                             *this)
          .release());
  registerPriorityTaskPool(priorityPool_);
}

void Service::initStreams(const config::Config& cfg) {
  input_ = servicelib::makeInputStream<RequestData, std::monostate,
                                       std::exception_ptr, Service>(
      cfg.streams.inputRequest, nullptr, *this);
  auto sink = [this](servicelib::MessageContext context,
                     const RequestData& value) {
    consumeRequest(std::move(context), value);
  };
  input_->sink(cfg.streams.sink,
               servicelib::make_function(std::move(sink), "sink"));
}

void Service::initDataSources([[maybe_unused]] const config::Config& cfg) {
  endpointConsumer_ = servicelib::datasource::http::UserverEndpointConsumer<
      RequestData, std::monostate, std::exception_ptr, Service,
      RequestHandler>::make(*this, *input_, RequestHandler{});
  dataSource_ = servicelib::datasource::http::UserverDataSource::make(
      *this, *input_);
  dataSource_->addEndpoint(endpointConsumer_->endpoint());
  registerDataSource(dataSource_);
}

void Service::stop() noexcept {
  try {
    servicelib::ServiceApp<Service, DataTypes>::stop();
    releaseRuntime();
  } catch (...) {
    std::terminate();
  }
}

void Service::releaseRuntime() noexcept {
  dataSource_.reset();
  endpointConsumer_.reset();
  input_.reset();
  priorityPool_.reset();
}

servicelib::log::Logger& Service::getLogger() {
  return servicelib::telemetry::userver_adapter::UserverLogger::instance();
}

servicelib::metrics::Metrics& Service::getMetrics() { return metrics_; }

std::shared_ptr<servicelib::datasource::http::IUserverEndpoint>
Service::httpDataSourceEndpoint(int endpointId) const {
  if (!endpointConsumer_ || endpointConsumer_->endpoint()->id() != endpointId) {
    return nullptr;
  }
  return endpointConsumer_->endpoint();
}

void Service::consumeRequest(
    [[maybe_unused]] servicelib::MessageContext context,
    const RequestData& value) {
  userver::formats::json::ValueBuilder payload;
  payload["text"] = value.text;
  TESTPOINT("mockservice-request-data", payload.ExtractValue());
}

MockServiceComponent::MockServiceComponent(
    const userver::components::ComponentConfig& componentConfig,
    const userver::components::ComponentContext& context)
    : ComponentBase(componentConfig, context),
      service_(context.FindComponent<userver::components::StatisticsStorage>()
                   .GetStorage()) {
  // Registers the lifecycle dependency and guarantees publication before the
  // config-driven graph is constructed.
  static_cast<void>(context.FindComponent<RuntimeConfigComponent>());
  service_.start();
}

MockServiceComponent::~MockServiceComponent() { service_.stop(); }

void MockServiceComponent::OnAllComponentsAreStopping() { service_.stop(); }

std::shared_ptr<servicelib::datasource::http::IUserverEndpoint>
MockServiceComponent::httpDataSourceEndpoint(int endpointId) const {
  return service_.httpDataSourceEndpoint(endpointId);
}

userver::yaml_config::Schema MockServiceComponent::GetStaticConfigSchema() {
  return userver::yaml_config::MergeSchemas<userver::components::ComponentBase>(
      R"(
type: object
description: Config-driven servicelib mock service used by integration tests.
additionalProperties: false
properties: {}
)");
}

}  // namespace mockservice

#pragma once

#include <servicelib/datasource/grpc/nostreaming.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

namespace {

class SourceLifetimeReviewConfig final : public servicelib::config::IConfig {
 public:
  SourceLifetimeReviewConfig() {
    service.id = 1;
    service.name = "source-lifetime-review";
    connector.id = 1;
    connector.name = "grpc-review";
    endpoint.id = 1;
    endpoint.name = "unary-review";
    endpoint.idDataConnector = 1;
    endpoint.grpcMethodType = servicelib::api::GrpcMethodType::kNoStreaming;
  }
  std::vector<const servicelib::config::ServiceConfig*> GetServices() const override { return {&service}; }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override { return {}; }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors() const override { return {connector}; }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints() const override { return {endpoint}; }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override { return {}; }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override { return {}; }
  std::vector<const servicelib::config::ModuleConfig*> GetModules() const override { return {}; }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override { return {}; }

  servicelib::config::ServiceConfig service;
  servicelib::config::GrpcDataConnectorConfig connector;
  servicelib::config::GrpcEndpointConfig endpoint;
};

class SourceLifetimeReviewEnvironment final : public servicelib::IServiceEnvironment {
 public:
  SourceLifetimeReviewEnvironment()
      : runtime_(std::make_shared<servicelib::config::RuntimeConfig>(config_)) {}
  std::shared_ptr<const servicelib::config::RuntimeConfig> getRuntimeConfigSnapshot() const override { return runtime_; }
  std::shared_ptr<const servicelib::config::ServiceConfig> getServiceConfigSnapshot() const override {
    return std::shared_ptr<const servicelib::config::ServiceConfig>(runtime_, &config_.service);
  }
  servicelib::log::Logger& getLogger() override { return logger_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }

 private:
  SourceLifetimeReviewConfig config_;
  std::shared_ptr<const servicelib::config::RuntimeConfig> runtime_;
  servicelib::testlog::TestLog logger_;
  servicelib::testmetrics::TestMetrics metrics_;
};

struct SourceLifetimeReviewHandler final {
  using State = int;
  struct Begin final {
    servicelib::MessageContext context;
    State state;
  };
  int* ended;

  Begin beginRequest(servicelib::MessageContext context, auto&) const {
    return {std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext, auto&, State&, const int&,
                      auto result, auto&) const {
    // Real handlers retain ResultContext in callbacks in order to signal Done.
    result.setResultCallback("pending", [result](auto, auto&, State&, const int&, auto&) mutable {
      result.done();
      return true;
    });
    throw std::runtime_error("source-lifetime-review-failure");
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error, State&) const {
    EXPECT_NE(error, nullptr);
    ++*ended;
  }
};

UTEST(GrpcSourceLifetimeReview, FinishReleasesCallbacksRetainingResultContext) {
  using namespace servicelib;
  namespace grpc = datasource::grpc;
  SourceLifetimeReviewEnvironment environment;
  int ended = 0;
  using Endpoint = grpc::NoStreamingEndpoint<int, int, int, int, SourceLifetimeReviewHandler>;
  Endpoint endpoint(environment, 1, SourceLifetimeReviewHandler{&ended},
                    [](MessageContext, Payload<int>) {}, true);
  endpoint.start(Context{});
  auto sender = std::make_shared<grpc::Sender<int>>([](int) {});
  auto request = endpoint.begin(MessageContext{}.withStreamId("failed-request"), sender, {});
  std::weak_ptr<typename Endpoint::Request> weak = request;
  endpoint.activate(request);
  std::exception_ptr error;
  try {
    endpoint.consume(request, 1);
  } catch (...) {
    error = std::current_exception();
  }
  EXPECT_NE(error, nullptr);
  endpoint.finish(request, error);
  EXPECT_EQ(ended, 1);
  {
    typename Endpoint::ResultCtx savedResult{request};
    savedResult.setResultCallback("late", [savedResult](auto, auto&, int&, const int&, auto&) mutable {
      savedResult.done();
      return true;
    });
    std::lock_guard lock(request->callbacksMutex);
    EXPECT_TRUE(request->callbacks.empty());
  }
  request.reset();
  endpoint.stop(Context{});
  EXPECT_TRUE(weak.expired()) << "finished request is retained by its own result callback";

  // Clean up only AFTER recording the failure; do not leak the diagnostic
  // fixture itself when testing the currently broken runtime.
  if (auto retained = weak.lock()) {
    std::lock_guard lock(retained->callbacksMutex);
    retained->callbacks.clear();
  }
}

}  // namespace

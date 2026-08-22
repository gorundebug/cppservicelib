/*
 * userver HTTP handlers for the built-in servicelib topology page.
 */
#pragma once

#include <string>
#include <string_view>

#include <userver/components/component_config.hpp>
#include <userver/components/component_context.hpp>
#include <userver/server/handlers/http_handler_base.hpp>
#include <userver/server/http/http_request.hpp>
#include <userver/server/http/http_status.hpp>

#include <servicelib/runtime/status/status.hpp>
#include <servicelib/runtime/status/web.generated.hpp>

namespace servicelib::telemetry::userver_adapter {

class StatusHandlerBase
    : public ::userver::server::handlers::HttpHandlerBase {
 public:
  using HttpHandlerBase::HttpHandlerBase;

 protected:
  static status::Provider* ProviderOrUnavailable(
      const ::userver::server::http::HttpRequest& request) {
    auto* provider = status::Registry::Get();
    if (!provider) {
      request.GetHttpResponse().SetStatus(
          ::userver::server::http::HttpStatus::kServiceUnavailable);
    }
    return provider;
  }
};

class StatusPageHandler final : public StatusHandlerBase {
 public:
  static constexpr std::string_view kName = "servicelib-status-handler";
  using StatusHandlerBase::StatusHandlerBase;

 protected:
  std::string HandleRequestThrow(
      const ::userver::server::http::HttpRequest& request,
      ::userver::server::request::RequestContext&) const override {
    request.GetHttpResponse().SetContentType("text/html; charset=utf-8");
    return std::string(status::web::kStatusHtml);
  }
};

class StatusDataHandler final : public StatusHandlerBase {
 public:
  static constexpr std::string_view kName = "servicelib-status-data-handler";
  using StatusHandlerBase::StatusHandlerBase;

 protected:
  std::string HandleRequestThrow(
      const ::userver::server::http::HttpRequest& request,
      ::userver::server::request::RequestContext&) const override {
    request.GetHttpResponse().SetContentType("application/json; charset=utf-8");
    const auto* provider = ProviderOrUnavailable(request);
    return provider ? provider->networkDataJson()
                    : R"({"nodes":[],"edges":[]})";
  }
};

class StatusGraphHandler final : public StatusHandlerBase {
 public:
  static constexpr std::string_view kName = "servicelib-status-graph-handler";
  using StatusHandlerBase::StatusHandlerBase;

 protected:
  std::string HandleRequestThrow(
      const ::userver::server::http::HttpRequest& request,
      ::userver::server::request::RequestContext&) const override {
    request.GetHttpResponse().SetContentType("text/yaml; charset=utf-8");
    const auto* provider = ProviderOrUnavailable(request);
    return provider ? provider->graphYaml() : std::string{};
  }
};

class StatusJavaScriptHandler final : public StatusHandlerBase {
 public:
  static constexpr std::string_view kName = "servicelib-status-js-handler";
  using StatusHandlerBase::StatusHandlerBase;

 protected:
  std::string HandleRequestThrow(
      const ::userver::server::http::HttpRequest& request,
      ::userver::server::request::RequestContext&) const override {
    auto& response = request.GetHttpResponse();
    response.SetContentType("application/javascript; charset=utf-8");
    response.SetHeader(
        std::string_view{"Cache-Control"},
        std::string{"public, max-age=31536000, immutable"});
    return std::string(status::web::kVisJavaScript);
  }
};

class StatusCssHandler final : public StatusHandlerBase {
 public:
  static constexpr std::string_view kName = "servicelib-status-css-handler";
  using StatusHandlerBase::StatusHandlerBase;

 protected:
  std::string HandleRequestThrow(
      const ::userver::server::http::HttpRequest& request,
      ::userver::server::request::RequestContext&) const override {
    auto& response = request.GetHttpResponse();
    response.SetContentType("text/css; charset=utf-8");
    response.SetHeader(
        std::string_view{"Cache-Control"},
        std::string{"public, max-age=31536000, immutable"});
    return std::string(status::web::kVisCss);
  }
};

class HealthHandler final
    : public ::userver::server::handlers::HttpHandlerBase {
 public:
  static constexpr std::string_view kName = "servicelib-health-handler";
  using HttpHandlerBase::HttpHandlerBase;

 protected:
  std::string HandleRequestThrow(
      const ::userver::server::http::HttpRequest& request,
      ::userver::server::request::RequestContext&) const override {
    request.GetHttpResponse().SetContentType("text/plain; charset=utf-8");
    return "ok\n";
  }
};

}  // namespace servicelib::telemetry::userver_adapter

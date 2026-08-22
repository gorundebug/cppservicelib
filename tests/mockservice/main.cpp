#include <userver/clients/dns/component.hpp>
#include <userver/clients/http/component_list.hpp>
#include <userver/components/minimal_server_component_list.hpp>
#include <userver/server/handlers/tests_control.hpp>
#include <userver/testsuite/testsuite_support.hpp>
#include <userver/utils/daemon_run.hpp>

#include "mockservice.hpp"

int main(int argc, char* argv[]) {
  const auto components = userver::components::MinimalServerComponentList()
                              .Append<userver::components::TestsuiteSupport>()
                              .Append<userver::server::handlers::TestsControl>()
                              .AppendComponentList(
                                  userver::clients::http::ComponentList())
                              .Append<userver::clients::dns::Component>()
                              .Append<mockservice::RuntimeConfigComponent>()
                              .Append<mockservice::MockServiceComponent>()
                              .Append<mockservice::DataHandler>();
  return userver::utils::DaemonMain(argc, argv, components);
}

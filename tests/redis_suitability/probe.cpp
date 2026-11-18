// Eligibility probe only: stock userver components and coroutine requests.
#include <userver/clients/dns/component.hpp>
#include <userver/components/component.hpp>
#include <userver/components/minimal_server_component_list.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/server/handlers/http_handler_base.hpp>
#include <userver/storages/redis/client.hpp>
#include <userver/storages/redis/component.hpp>
#include <userver/storages/secdist/component.hpp>
#include <userver/storages/secdist/provider_component.hpp>
#include <userver/testsuite/testsuite_support.hpp>
#include <userver/utils/async.hpp>
#include <userver/utils/daemon_run.hpp>
#include <chrono>
#include <stdexcept>
#include <cstdlib>

namespace uv = userver;
namespace redis = uv::storages::redis;
using namespace std::chrono_literals;
void Check(bool value, const char* message) { if (!value) throw std::runtime_error(message); }
redis::CommandControl Control(std::chrono::milliseconds timeout) {
  redis::CommandControl control{timeout, timeout, 1};
  // Stock userver's named-command statistics assert on Streams commands in Debug.
  control.account_in_statistics = false;
  return control;
}
class Probe final : public uv::server::handlers::HttpHandlerBase {
 public:
  static constexpr std::string_view kName = "probe";
  Probe(const uv::components::ComponentConfig& config, const uv::components::ComponentContext& context)
      : HttpHandlerBase(config, context), component_(context.FindComponent<uv::components::Redis>()) {}
  std::string HandleRequest(uv::server::http::HttpRequest& request, uv::server::request::RequestContext&) const override {
    if (request.GetArg("tls") == "1") {
      auto tls = component_.GetClient("tls");
      auto result = tls->GenericCommand<redis::ReplyData>("ECHO", {"hostname-probe"}, 0,
          Control(1s)).Get();
      Check(result.GetString()=="hostname-probe", "TLS command failed");
      return "UNSAFE: accepted trusted certificate for wrong hostname\n";
    }
    const auto client = component_.GetClient("control");
    const auto reader = component_.GetClient("reader");
    const auto fault = component_.GetClient("fault");
    const std::string key = std::getenv("PROBE_KEY");
    // In userver max_retries counts attempts; 1 is the no-replay setting.
    const auto cc = Control(700ms);
    auto cmd = [&](std::string name, std::vector<std::string> args, std::size_t index = 0) {
      return client->GenericCommand<redis::ReplyData>(std::move(name), std::move(args), index, cc).Get();
    };
    cmd("XGROUP", {"CREATE", key, "g", "0-0", "MKSTREAM"}, 1);
    auto blocked = uv::utils::Async("blocking-read", [&] {
      return reader->GenericCommand<redis::ReplyData>("XREADGROUP",
          {"GROUP", "g", "reader", "COUNT", "1", "BLOCK", "5000", "STREAMS", key, ">"},
          8, Control(6s)).Get();
    });
    uv::engine::SleepFor(100ms);
    const auto start = std::chrono::steady_clock::now();
    auto id = cmd("XADD", {key, "*", "same", std::string("\0\xff", 2), "same", "", "", "last"}).GetString();
    Check(std::chrono::steady_clock::now()-start < 500ms, "blocking-reader shared control connection");
    auto reply = blocked.Get();
    const auto& fields = reply.GetArray().at(0).GetArray().at(1).GetArray().at(0).GetArray().at(1).GetArray();
    Check(fields.size()==6 && fields[0].GetString()=="same" && fields[1].GetString()==std::string("\0\xff",2)
          && fields[2].GetString()=="same" && fields[3].GetString().empty() && fields[4].GetString().empty(), "binary/duplicate reply loss");
    auto claim = cmd("XAUTOCLAIM", {key, "g", "other", "1", "0-0", "COUNT", "1"});
    Check(claim.GetArray().size()==3, "claim nested tuple unavailable");
    Check(cmd("XACK", {key,"g",id}).GetInt()==1, "explicit ACK failed");
    Check(cmd("XLEN", {key}).GetInt()==1, "ACK deleted entry");
    auto cancelled = uv::utils::Async("cancel-read", [&] {
      return reader->GenericCommand<redis::ReplyData>("XREADGROUP",
        {"GROUP","g","reader","COUNT","1","BLOCK","5000","STREAMS",key,">"},8,
        Control(6s)).Get();
    });
    uv::engine::SleepFor(100ms);
    const auto cancel_start = std::chrono::steady_clock::now();
    cancelled.SyncCancel();
    Check(std::chrono::steady_clock::now()-cancel_start < 500ms, "request cancellation not prompt");
    bool unknown = false;
    try { fault->GenericCommand<redis::ReplyData>("XADD", {key,"*","fault","once"},0,cc).Get(); }
    catch (const std::exception&) { unknown = true; }
    Check(unknown, "lost reply reported as success");
    uv::engine::SleepFor(900ms);
    Check(cmd("XLEN", {key}).GetInt()==2, "ambiguous XADD replayed or not delivered");
    cmd("DEL", {key});
    return "PASS binary isolation cancellation no-replay\n";
  }
 private:
  uv::components::Redis& component_;
};
int main(int argc, char** argv) {
  auto components = uv::components::MinimalServerComponentList()
      .Append<uv::components::Redis>().Append<uv::components::Secdist>()
      .Append<uv::components::DefaultSecdistProvider>().Append<uv::clients::dns::Component>()
      .Append<uv::components::TestsuiteSupport>().Append<Probe>();
  return uv::utils::DaemonMain(argc, argv, components);
}

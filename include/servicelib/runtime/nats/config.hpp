#pragma once
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>
#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>

namespace servicelib::nats {
inline bool HasSpace(std::string_view value) {
  for (unsigned char c : value) if (c <= 32 || c == 127) return true;
  for (const auto* space : {"\xc2\x85", "\xc2\xa0", "\xe1\x9a\x80", "\xe2\x80", "\xe2\x81\x9f", "\xe3\x80\x80"})
    if (value.find(space) != value.npos) return true;
  return false;
}
inline bool ValidServer(std::string_view server) {
  const auto prefix = server.starts_with("nats://") ? 7 : server.starts_with("tls://") ? 6 : 0;
  if (!prefix || HasSpace(server)) return false;
  auto host = server.substr(prefix);
  if (host.empty() || host.find_first_of("/@?#\\%") != host.npos) return false;
  std::string_view port;
  if (host.front() == '[') {
    const auto end = host.find(']');
    if (end == host.npos || end <= 1 || host.substr(1, end-1).find(':') == host.npos) return false;
    if (end + 1 == host.size()) return true;
    if (host[end+1] != ':') return false;
    port = host.substr(end+2);
  } else {
    const auto colon = host.find(':');
    if (colon == host.npos) return true;
    if (colon == 0) return false;
    port = host.substr(colon+1);
  }
  if (port.empty() || port.size() > 5) return false;
  unsigned number = 0;
  for (const unsigned char c : port) {
    if (c < '0' || c > '9') return false;
    number = number*10 + c-'0';
  }
  return number > 0 && number <= 65535;
}
inline std::vector<std::string> InvalidConnectionFields(const config::NATSDataConnectorConfig& c, bool topology = false) {
  std::vector<std::string> errors;
  bool plain = false, tls = false, invalid = c.servers.empty();
  for (const auto& server : c.servers) {
    invalid |= !ValidServer(server);
    plain |= server.starts_with("nats://");
    tls |= server.starts_with("tls://");
  }
  if (invalid || (plain && tls && !c.tlsEnabled)) errors.emplace_back("servers");
  if (!c.tlsEnabled && !tls && (!c.tlsCaFile.empty() || !c.tlsServerName.empty() || !c.tlsCertFile.empty() || !c.tlsKeyFile.empty())) errors.emplace_back("tlsEnabled");
  if (c.tlsCertFile.empty() != c.tlsKeyFile.empty()) errors.emplace_back("tlsCertFile");
  constexpr std::int64_t maximumMs = 9223372036854LL;
  if (c.connectTimeoutMs <= 0 || c.connectTimeoutMs > maximumMs) errors.emplace_back("connectTimeoutMs");
  if (c.operationTimeoutMs <= 0 || c.operationTimeoutMs > maximumMs) errors.emplace_back("operationTimeoutMs");
  if (topology) {
    if (!c.username.empty()) errors.emplace_back("username");
    if (!c.password.empty()) errors.emplace_back("password");
    if (!c.token.empty()) errors.emplace_back("token");
  } else {
    if (c.username.empty() != c.password.empty()) errors.emplace_back("username");
    if ((!c.username.empty()) + (!c.token.empty()) + (!c.credentialsFile.empty()) > 1) errors.emplace_back("token");
  }
  return errors;
}
inline void ValidateConnection(const config::NATSDataConnectorConfig& c) {
  const auto errors = InvalidConnectionFields(c);
  if (!errors.empty()) throw std::invalid_argument("invalid NATS " + errors.front());
  if (c.type != api::DataConnectorType::kNATS && c.type != api::DataConnectorType::kJetStream)
    throw std::invalid_argument("invalid NATS connector type");
}
inline void ValidateEndpoint(const config::NATSDataConnectorConfig& c, const config::NATSEndpointConfig& e, bool source) {
  ValidateConnection(c);
  if (e.subject.empty() || HasSpace(e.subject) || e.subject.front()=='.' || e.subject.back()=='.' || e.subject.find("..") != std::string::npos)
    throw std::invalid_argument("invalid NATS subject");
  for (std::size_t start = 0; start < e.subject.size();) {
    auto end = e.subject.find('.',start); if (end==std::string::npos) end=e.subject.size();
    auto part=e.subject.substr(start,end-start);
    if (part.find_first_of("*>")!=std::string::npos && (!source || (part!="*" && !(part==">" && end==e.subject.size()))))
      throw std::invalid_argument("invalid NATS subject wildcard");
    start=end+1;
  }
  const auto nameValid=[](const std::string& n) { return !n.empty() && !HasSpace(n) && n.find_first_of(".*>/\\") == n.npos; };
  if (c.type == api::DataConnectorType::kNATS) {
    if (!e.streamName.empty() || !e.durableName.empty() || e.createStream || e.createConsumer)
      throw std::invalid_argument("Core NATS has no JetStream resources");
  } else {
    if (!nameValid(e.streamName) || (source && !nameValid(e.durableName)))
      throw std::invalid_argument("JetStream stream and source durable names required");
    if (!e.queueGroup.empty()) throw std::invalid_argument("JetStream uses durable consumers, not Core queue groups");
  }
  if (!source && (!e.queueGroup.empty() || !e.durableName.empty() || e.createConsumer))
    throw std::invalid_argument("source settings on NATS sink");
}
}  // namespace servicelib::nats

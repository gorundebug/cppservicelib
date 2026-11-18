#pragma once
#include <servicelib/runtime/detail/source_metrics.hpp>
namespace servicelib::datasource::nats { template<class, class, class, class> class Endpoint; }
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <nats/nats.h>
#include <userver/engine/task/task_processor_fwd.hpp>
#include <servicelib/runtime/context.hpp>
#include <servicelib/runtime/nats/config.hpp>

namespace servicelib::nats {
// Preserve repeated values in insertion order for each native header name.
using Headers = std::multimap<std::string, std::string>;
struct PublishAck { std::string stream; std::uint64_t sequence{}; bool duplicate{}; };
struct DeliveryResult {
  bool accepted{}; // Core: local admission only. JetStream: server PubAck.
  bool flushed{};
  std::optional<PublishAck> ack;
  std::exception_ptr error;
};
class ConsumerMessage {
 public:
  ConsumerMessage(std::shared_ptr<natsMsg> message, bool jetstream,
                  userver::engine::TaskProcessor& blocking, std::int64_t timeout);
  [[nodiscard]] std::string_view data() const;
  [[nodiscard]] std::string_view subject() const;
  [[nodiscard]] std::string_view reply() const;
  [[nodiscard]] Headers headers() const;
  [[nodiscard]] bool jetStream() const { return jetstream_; }
  [[nodiscard]] natsMsg* native() const { return message_.get(); }
  void ack() const;
  void doubleAck() const;
  void nak() const;
  void inProgress() const;
  void term() const;
 private:
  friend class Client;
  void requireJetStream() const;
  std::shared_ptr<natsMsg> message_;
  bool jetstream_;
  userver::engine::TaskProcessor* blocking_;
  std::int64_t timeout_;
};

// All business callbacks execute on applicationProcessor. DNS, TLS setup,
// management requests, synchronous acknowledgement and drain waits execute on
// blockingProcessor. Both processors are owned by the stock userver runtime.
class Client final {
 public:
  using MessageHandler = std::function<void(ConsumerMessage)>;
  using ErrorHandler = std::function<void(std::exception_ptr)>;
  using DeliveryHandler = std::function<void(DeliveryResult)>;
  static constexpr std::size_t kMaxPending = 256;
  static constexpr int kMaxPendingBytes = 8 * 1024 * 1024;
  Client(userver::engine::TaskProcessor& applicationProcessor,
         userver::engine::TaskProcessor& blockingProcessor,
         config::NATSDataConnectorConfig connector,
         config::NATSEndpointConfig endpoint);
  ~Client();
  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;
  void start(Context context, bool source, ErrorHandler onError);
  void subscribe(MessageHandler handler);
  void publish(MessageContext context, std::string value, Headers headers, DeliveryHandler callback);
  DeliveryResult publishSync(MessageContext context, std::string value, Headers headers);
  // Stop subscription admission first, retain the connection for explicit ACKs
  // while accepted graph callbacks finish, then close transport and join work.
  void stopAdmission(Context context);
  bool waitForCallbacks(Context context);
  void stop(Context context);
  [[nodiscard]] const std::string& subject() const;
 private:
  template<class, class, class, class> friend class servicelib::datasource::nats::Endpoint;
  void setSourceMetrics(std::shared_ptr<servicelib::detail::SourceMetrics> metrics);
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
MessageContext ContextFromHeaders(MessageContext context, const Headers& headers, bool tracingEnabled);
void InjectContext(const MessageContext& context, Headers& headers, bool tracingEnabled);
}  // namespace servicelib::nats

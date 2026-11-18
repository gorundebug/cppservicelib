// Included by nats_endpoints_test.cpp so the existing pinned native test target
// exercises the actual adapter, not a separate raw-client implementation.
namespace {
std::string SecurityEnv(const char* name) {
  const auto* value = std::getenv(name);
  return value ? value : "";
}
config::NATSDataConnectorConfig SecurityConfig() {
  config::NATSDataConnectorConfig c;
  c.servers = {SecurityEnv("NATS_SECURITY_URL")};
  c.username = SecurityEnv("NATS_SECURITY_USER");
  c.password = SecurityEnv("NATS_SECURITY_PASSWORD");
  c.token = SecurityEnv("NATS_SECURITY_TOKEN");
  c.credentialsFile = SecurityEnv("NATS_SECURITY_CREDS");
  c.tlsCaFile = SecurityEnv("NATS_SECURITY_CA");
  c.tlsServerName = SecurityEnv("NATS_SECURITY_HOSTNAME");
  c.tlsCertFile = SecurityEnv("NATS_SECURITY_CERT");
  c.tlsKeyFile = SecurityEnv("NATS_SECURITY_KEY");
  c.connectTimeoutMs = 500;
  c.operationTimeoutMs = 500;
  if (SecurityEnv("NATS_SECURITY_JS") == "1")
    c.type = api::DataConnectorType::kJetStream;
  return c;
}
void SecurityError(std::exception_ptr error) {
  try {
    if (error) std::rethrow_exception(error);
  } catch (const std::exception& e) {
    const std::string text = e.what();
    for (const auto* field : {"NATS_SECURITY_PASSWORD", "NATS_SECURITY_TOKEN",
                              "NATS_SECURITY_CREDS", "NATS_SECURITY_KEY"}) {
      const auto secret = SecurityEnv(field);
      if (!secret.empty()) {
        EXPECT_EQ(text.find(secret), std::string::npos)
            << "sensitive configuration in error";
      }
    }
  }
}
UTEST(NatsSecurity, NativeMatrix) {
  if (SecurityEnv("NATS_SECURITY_URL").empty()) GTEST_SKIP();
  auto& app = userver::engine::current_task::GetTaskProcessor();
  auto pool =
      userver::engine::SingleThreadedTaskProcessorsPool::MakeForTests(1);
  auto& blocking = pool.At(0);
  ASSERT_NE(&app, &blocking);
  const auto c = SecurityConfig();
  config::NATSEndpointConfig e;
  e.enabled = true;
  e.subject = SecurityEnv("NATS_SECURITY_DENIED").empty()
                  ? "allowed.security"
                  : "forbidden.security";
  const bool js = c.type == api::DataConnectorType::kJetStream;
  if (js) {
    e.streamName =
        "SECURITY_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    e.durableName = "reader";
    e.createStream = true;
    e.createConsumer = true;
  }
  const bool negative = SecurityEnv("NATS_SECURITY_NEGATIVE") == "1";
  std::atomic<int> errors{0}, received{0}, delivered{0};
  nats::Client source(app, blocking, c, e);
  const auto onError = [&](std::exception_ptr error) {
    EXPECT_EQ(&userver::engine::current_task::GetTaskProcessor(), &app);
    SecurityError(error);
    ++errors;
  };
  const auto began = std::chrono::steady_clock::now();
  std::atomic<bool> failed{false};
  try {
    source.start(Context{}, true, onError);
  } catch (...) {
    failed = true;
    SecurityError(std::current_exception());
  }
  if (negative) {
    EXPECT_TRUE(failed) << "invalid identity/TLS unexpectedly admitted";
    EXPECT_THROW(
        source.publish(MessageContext{}, "must-not-send", {}, [](auto) {}),
        std::runtime_error);
    source.stop(Context{}.bounded(100ms));
    EXPECT_LT(std::chrono::steady_clock::now() - began, 1500ms);
    return;
  }
  ASSERT_FALSE(failed);
  try {
    source.subscribe([&](nats::ConsumerMessage message) {
      EXPECT_EQ(&userver::engine::current_task::GetTaskProcessor(), &app);
      EXPECT_EQ(message.data(), "security-payload");
      if (message.jetStream())
        message.doubleAck();
      else
        EXPECT_THROW(message.ack(), std::logic_error);
      ++received;
    });
  } catch (...) {
    SecurityError(std::current_exception());
    ++errors;
  }
  auto sinkEndpoint = e;
  sinkEndpoint.durableName.clear();
  sinkEndpoint.createConsumer = false;
  nats::Client sink(app, blocking, c, sinkEndpoint);
  sink.start(Context{}, false, onError);
  sink.publish(MessageContext{}, "security-payload", {},
               [&](nats::DeliveryResult result) {
                 EXPECT_EQ(&userver::engine::current_task::GetTaskProcessor(),
                           &app);
                 EXPECT_EQ(result.ack.has_value(), js);
                 EXPECT_TRUE(result.accepted);
                 EXPECT_FALSE(result.error);
                 ++delivered;
               });
  const bool denied = !SecurityEnv("NATS_SECURITY_DENIED").empty();
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while ((delivered == 0 || (denied ? errors == 0 : received == 0)) &&
         std::chrono::steady_clock::now() < deadline)
    userver::engine::SleepFor(5ms);
  if (denied) {
    EXPECT_GT(errors, 0);
    EXPECT_EQ(received, 0);
  } else {
    EXPECT_EQ(errors, 0);
    EXPECT_EQ(received, 1);
  }
  EXPECT_EQ(delivered, 1);
  const auto stop = Context{}.bounded(500ms);
  try {
    source.stopAdmission(stop);
  } catch (...) {
    SecurityError(std::current_exception());
  }
  source.waitForCallbacks(stop);
  source.stop(stop);
  sink.stop(stop);
}
UTEST(NatsSecurity, StartupResponsiveness) {
  if (SecurityEnv("NATS_SECURITY_URL").empty()) GTEST_SKIP();
  auto& app = userver::engine::current_task::GetTaskProcessor();
  auto pool =
      userver::engine::SingleThreadedTaskProcessorsPool::MakeForTests(1);
  auto& blocking = pool.At(0);
  ASSERT_NE(&app, &blocking);
  auto c = SecurityConfig();
  c.connectTimeoutMs = 1200;
  config::NATSEndpointConfig e;
  e.enabled = true;
  e.subject = "allowed.startup";
  nats::Client client(app, blocking, c, e);
  std::stop_source stop;
  auto context = Context{}.withStopToken(stop.get_token());
  const auto mode = SecurityEnv("NATS_SECURITY_START");
  if (mode == "pre-cancel") stop.request_stop();
  if (mode == "deadline" || mode == "tls-deadline")
    context = context.bounded(100ms);
  std::atomic<int> ticks{0};
  std::atomic<bool> done{false};
  auto heartbeat = userver::engine::AsyncNoTracing(app, [&] {
    while (!done) {
      ++ticks;
      userver::engine::SleepFor(5ms);
    }
  });
  const auto began = std::chrono::steady_clock::now();
  std::atomic<bool> failed{false};
  auto startup = userver::engine::AsyncNoTracing(app, [&] {
    try {
      client.start(context, false, [](auto error) { SecurityError(error); });
    } catch (...) {
      failed = true;
      SecurityError(std::current_exception());
    }
  });
  if (mode == "cancel" || mode == "task-cancel") {
    userver::engine::SleepFor(40ms);
    if (mode == "cancel")
      stop.request_stop();
    else
      startup.RequestCancel();
  }
  startup.Wait();
  done = true;
  heartbeat.Get();
  EXPECT_TRUE(failed)
      << "startup cancellation must not become successful admission";
  EXPECT_THROW(
      client.publish(MessageContext{}, "must-not-send", {}, [](auto) {}),
      std::runtime_error);
  client.stop(Context{}.bounded(100ms));
  const auto elapsed = std::chrono::steady_clock::now() - began;
  EXPECT_LT(elapsed, mode == "pre-cancel" ? 300ms : 700ms);
  if (mode != "pre-cancel") {
    EXPECT_GE(ticks, 5) << "application worker blocked by native startup";
  }
}
}  // namespace

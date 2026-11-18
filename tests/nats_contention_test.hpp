#pragma once
// Included by nats_endpoints_test.cpp; deliberately uses the production Client.
UTEST(Nats, SharedBlockingWorkerContention) {
  const auto* server = std::getenv("SERVICELIB_NATS_TEST_URL");
  if (!server) GTEST_SKIP();
  using Clock = std::chrono::steady_clock;
  const auto ms = [](auto duration) { return std::chrono::duration<double, std::milli>(duration).count(); };
  auto& app = userver::engine::current_task::GetTaskProcessor();
  auto pool = userver::engine::SingleThreadedTaskProcessorsPool::MakeForTests(1);
  auto& blocking = pool.At(0);
  ASSERT_NE(&app, &blocking);
  config::NATSDataConnectorConfig cfg;
  cfg.type = api::DataConnectorType::kJetStream; cfg.servers = {server}; cfg.operationTimeoutMs = 2000;
  const auto suffix = std::to_string(Clock::now().time_since_epoch().count());
  std::vector<std::unique_ptr<nats::Client>> sources;
  std::vector<std::string> streams;
  std::stop_source cancellation;
  const auto owner = Context{}.withStopToken(cancellation.get_token());
  std::atomic<int> errors{0}, received{0}, completed{0}, ticks{0};
  std::atomic<bool> heartbeatDone{false};
  double heartbeatMaxMs = 0;
  auto heartbeat = userver::engine::AsyncNoTracing(app, [&] {
    auto previous = Clock::now();
    while (!heartbeatDone) {
      userver::engine::SleepFor(5ms);
      auto now = Clock::now(); heartbeatMaxMs = std::max(heartbeatMaxMs, ms(now-previous)); previous = now; ++ticks;
    }
  });
  config::NATSEndpointConfig active;
  std::vector<double> ackTimes;
  auto addSource = [&](int index) {
    config::NATSEndpointConfig ep; ep.enabled = true;
    ep.subject = "contention."+suffix+"."+std::to_string(index);
    ep.streamName = "CONTENTION_"+suffix+"_"+std::to_string(index);
    ep.durableName = "reader"; ep.createStream = true; ep.createConsumer = true;
    streams.push_back(ep.streamName);
    auto client = std::make_unique<nats::Client>(app, blocking, cfg, ep);
    client->start(owner, true, [&](auto){ ++errors; });
    client->subscribe([&, index](nats::ConsumerMessage message) {
      EXPECT_EQ(index, 0); EXPECT_EQ(&userver::engine::current_task::GetTaskProcessor(), &app);
      ++received; auto begin = Clock::now(); message.doubleAck(); ackTimes.push_back(ms(Clock::now()-begin)); ++completed;
    });
    sources.push_back(std::move(client));
    return ep;
  };
  active = addSource(0);
  auto output = active; output.durableName.clear(); output.createConsumer = false;
  nats::Client publisher(app, blocking, cfg, output);
  publisher.start(owner, false, [&](auto){ ++errors; });
  for (int idle : {0, 6}) {
    if (idle) for (int i=1; i<=idle; ++i) addSource(i);
    userver::engine::SleepFor(250ms);
    for (int sample=0; sample<6; ++sample) {
      const auto begin = Clock::now(); const auto tickStart = ticks.load();
      const int expected = completed+1;
      struct Completion { std::atomic<bool> delivered{false}; double publishMs = 0; };
      auto completion = std::make_shared<Completion>();
      publisher.publish(MessageContext{}.withDeadline(begin+10s), "payload", {}, [completion, begin, ms](nats::DeliveryResult result) {
        EXPECT_TRUE(result.accepted); EXPECT_TRUE(result.ack.has_value()); EXPECT_FALSE(result.error);
        completion->publishMs = ms(Clock::now()-begin); completion->delivered = true;
      });
      while ((!completion->delivered || completed<expected) && Clock::now()<begin+10s) userver::engine::SleepFor(5ms);
      EXPECT_TRUE(completion->delivered); EXPECT_EQ(completed, expected);
      std::cout << "CONTENTION idle=" << idle << " sample=" << sample << " publish_ms=" << completion->publishMs
                << " roundtrip_ack_ms=" << ms(Clock::now()-begin) << " ack_ms=" << (ackTimes.empty()? -1:ackTimes.back())
                << " heartbeat_ticks=" << ticks-tickStart << std::endl;
    }
  }
  const auto stopBegin = Clock::now(); cancellation.request_stop();
  const auto stopContext = Context{}.withDeadline(stopBegin+5s);
  for (auto& source : sources) source->stopAdmission(stopContext);
  std::cout << "CONTENTION stop_admission_ms=" << ms(Clock::now()-stopBegin) << std::endl;
  int drained = 0;
  for (auto& source : sources) { if (source->waitForCallbacks(stopContext)) ++drained; source->stop(stopContext); }
  std::cout << "CONTENTION readers_drained=" << drained << " sources_stop_ms=" << ms(Clock::now()-stopBegin) << std::endl;
  publisher.stop(stopContext);
  const auto stopMs = ms(Clock::now()-stopBegin);
  heartbeatDone = true; heartbeat.Get();
  EXPECT_LT(stopMs, 10000); // Test watchdog, not a production SLA or hard cancellation promise.
  EXPECT_EQ(received, 12); EXPECT_EQ(completed, 12); EXPECT_EQ(errors, 0); EXPECT_GT(ticks, 10);
  std::cout << "CONTENTION shutdown_ms=" << stopMs << " heartbeat_max_gap_ms=" << heartbeatMaxMs
            << " ticks=" << ticks << " errors=" << errors << std::endl;
  userver::engine::AsyncNoTracing(blocking, [&] {
    natsConnection* connection{}; ASSERT_EQ(natsConnection_ConnectTo(&connection, server), NATS_OK);
    jsCtx* js{}; ASSERT_EQ(natsConnection_JetStream(&js, connection, nullptr), NATS_OK);
    for (const auto& stream : streams) EXPECT_EQ(js_DeleteStream(js, stream.c_str(), nullptr, nullptr), NATS_OK);
    jsCtx_Destroy(js); natsConnection_Destroy(connection);
  }).Get();
}

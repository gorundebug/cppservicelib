// cppUserver deliberately does not patch ConsumerScope to emulate recovery.
UTEST(KafkaCheckpoint, RejectsExternalCheckpointBeforeConsumerStart) {
  userver::kafka::impl::ConsumerConfiguration config;
  config.group_id = "unsupported-checkpoint";
  userver::kafka::impl::Secret secrets;
  secrets.brokers = "127.0.0.1:1";
  auto& processor = userver::engine::current_task::GetTaskProcessor();
  userver::kafka::impl::Consumer native{
      "kafka-consumer-unsupported-checkpoint", {"events"}, processor, processor, processor,
      config, secrets, {}};
  auto scope = native.MakeConsumerScope();
  servicelib::datasource::kafka::UserverConsumerClient client{scope, "events", false};
  using namespace servicelib::datasource::kafka;
  for (const auto& positions : {CheckpointOffsets{}, CheckpointOffsets{{0, 12}}}) {
    try {
      client.start(servicelib::Context{},
          [&positions] { return std::optional<CheckpointOffsets>{positions}; },
          [](ConsumerMessage) { ADD_FAILURE() << "unsupported consumer admitted a message"; },
          [](std::exception_ptr) { ADD_FAILURE() << "unexpected transport callback"; });
      FAIL() << "external checkpoint was silently accepted";
    } catch (const CheckpointRestoreError& error) {
      EXPECT_NE(std::string{error.what()}.find("not supported by cppUserver"), std::string::npos);
    }
  }
}

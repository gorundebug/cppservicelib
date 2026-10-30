// Included after custom_kafka_endpoints_test.cpp's fixture namespace.
// Opt-in native broker acceptance; no fake ConsumerClient is injected.

namespace native_kafka_visibility {

class VisibilityContext final {
 public:
  struct Invocation {};
  static VisibilityContext& getExecutionEnvironment() {
    static VisibilityContext context;
    return context;
  }
  Invocation beginInputInvocation() const { return {}; }
  template <typename T, typename Source, typename Consumer>
  void consume(servicelib::MessageContext context,
      Source&, Consumer& consumer, servicelib::Payload<T> value) {
    consumer.consume(std::move(context), std::move(value));
  }
};

class VisibilityProducer final {
 public:
  VisibilityProducer(const std::string& brokers, const std::string& transaction = {}) {
    auto config = std::unique_ptr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy)>(
        rd_kafka_conf_new(), rd_kafka_conf_destroy);
    const auto set = [&](const char* name, const std::string& value) {
      char error[512]{};
      if (rd_kafka_conf_set(config.get(), name, value.c_str(), error, sizeof(error)) != RD_KAFKA_CONF_OK)
        throw std::runtime_error(error);
    };
    set("bootstrap.servers", brokers);
    set("message.timeout.ms", "10000");
    if (!transaction.empty()) {
      set("transactional.id", transaction);
      set("transaction.timeout.ms", "30000");
    }
    rd_kafka_conf_set_opaque(config.get(), this);
    rd_kafka_conf_set_dr_msg_cb(config.get(), [](rd_kafka_t*, const rd_kafka_message_t* message, void* opaque) {
      if (message->err) static_cast<VisibilityProducer*>(opaque)->deliveryError_ = message->err;
    });
    char error[512]{};
    auto* raw = rd_kafka_new(RD_KAFKA_PRODUCER, config.get(), error, sizeof(error));
    if (!raw) throw std::runtime_error(error);
    config.release(); // rd_kafka_new owns it on success.
    handle_.reset(raw);
    if (!transaction.empty()) check(rd_kafka_init_transactions(handle_.get(), 10000));
  }
  void send(const std::string& topic, const std::string& value, int partition = 0) {
    const auto error = rd_kafka_producev(handle_.get(), RD_KAFKA_V_TOPIC(topic.c_str()),
        RD_KAFKA_V_PARTITION(partition), RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE(const_cast<char*>(value.data()), value.size()), RD_KAFKA_V_END);
    if (error) throw std::runtime_error(rd_kafka_err2str(error));
    const auto flushed = rd_kafka_flush(handle_.get(), 10000);
    if (flushed) throw std::runtime_error(rd_kafka_err2str(flushed));
    if (deliveryError_) throw std::runtime_error(rd_kafka_err2str(deliveryError_));
  }
  void begin() { check(rd_kafka_begin_transaction(handle_.get())); }
  void abort() { check(rd_kafka_abort_transaction(handle_.get(), 10000)); }
  void commit() { check(rd_kafka_commit_transaction(handle_.get(), 10000)); }
 private:
  static void check(rd_kafka_error_t* error) {
    if (!error) return;
    const std::string message = rd_kafka_error_string(error);
    rd_kafka_error_destroy(error);
    throw std::runtime_error(message);
  }
  std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)> handle_{nullptr, rd_kafka_destroy};
  rd_kafka_resp_err_t deliveryError_{RD_KAFKA_RESP_ERR_NO_ERROR};
};

struct VisibilityHandler final {
  using State = int;
  std::atomic<unsigned>* errors;
  int concurrency(auto&) { return 2; }
  servicelib::BeginResult<State> beginRequest(
      servicelib::MessageContext context, auto&) {
    return servicelib::BeginResult<State>{std::move(context), 0};
  }
  void consumeMessage(servicelib::MessageContext context,
      auto& stream, State&, const servicelib::datasource::kafka::ConsumerMessage& message, auto) {
    stream.collect(std::move(context), message.value());
    message.commit();
  }
  std::string getMessageId(
      servicelib::MessageContext, auto&, State&, const std::string& value) { return value; }
  void onError(servicelib::MessageContext, auto&, std::exception_ptr) {
    ++*errors;
    return;
  }
  void endRequest(servicelib::MessageContext, auto&, std::exception_ptr error, State&) {
    if (error) ++*errors;
    return;
  }
};

UTEST(KafkaNativeTransactions, EndpointVisibility) {
  using namespace std::chrono_literals;
  const auto* brokers = std::getenv("SA_TEST_KAFKA_BROKERS");
  if (!brokers) GTEST_SKIP() << "set SA_TEST_KAFKA_BROKERS for native acceptance";
  TestEnvironment environment;
  const auto topic = "sa-userver-visibility-" + std::to_string(
      std::chrono::system_clock::now().time_since_epoch().count());
  environment.config().kafkaConnector.brokers = brokers;
  environment.config().kafkaConnector.dialTimeout = 10000;
  auto& config = environment.config().kafkaEndpoint;
  config.topic = topic;
  config.consumerGroup = topic;
  config.partitions = 2;
  config.replicationFactor = 1;
  config.createTopic = true;
  using Input = servicelib::InputStream<std::string, std::string, std::exception_ptr, VisibilityContext>;
  servicelib::config::InputStreamConfig inputConfig;
  inputConfig.id = 1;
  inputConfig.idEndpoint = config.id;
  inputConfig.name = "input";
  auto input = Input::make(inputConfig, nullptr, environment);
  std::mutex mutex;
  std::vector<std::string> values;
  std::atomic<unsigned> errors{0};
  servicelib::config::SinkStreamConfig sinkConfig;
  sinkConfig.id = 2;
  sinkConfig.name = "observed";
  input->sink(sinkConfig, servicelib::StreamType<std::string>{}, servicelib::StreamFunction((
      [&mutex, &values](servicelib::MessageContext, const std::string& value) -> void {
        { std::lock_guard lock(mutex); values.push_back(value); }
        return;
      })));
  // Same explicit read_committed policy emitted by the userver template.
  // ConsumerComponent uses this native Consumer; only its container wiring is
  // replaced by direct test construction, not the ConsumerScope or transport.
  userver::kafka::impl::ConsumerConfiguration consumerConfig;
  consumerConfig.group_id = topic;
  consumerConfig.rd_kafka_options["isolation.level"] = "read_committed";
  userver::kafka::impl::Secret secrets;
  secrets.brokers = brokers;
  auto& processor = userver::engine::current_task::GetTaskProcessor();
  userver::kafka::impl::Consumer nativeConsumer{
      "kafka-consumer", {topic}, processor, processor, processor,
      consumerConfig, secrets, {}};
  auto scope = nativeConsumer.MakeConsumerScope();
  servicelib::datasource::kafka::UserverConsumerClient consumer{scope, topic, false};
  auto endpoint = servicelib::datasource::kafka::Endpoint<std::string, std::string, VisibilityHandler>::make(
      environment, *input, consumer, VisibilityHandler{&errors});
  const servicelib::Context lifetime;
  const auto owner = lifetime.bounded(60s);
  endpoint->start(owner);
  struct Stop final {
    decltype(endpoint)& endpointRef;
    servicelib::Context owner;
    ~Stop() {
      try { endpointRef->stop(owner.bounded(10s)); }
      catch (const std::exception& error) { ADD_FAILURE() << error.what(); }
    }
  } stop{endpoint, lifetime};
  VisibilityProducer ordinary{brokers}, transactional{brokers, topic};
  std::vector<std::string> expected;
  const auto expect = [&](std::string value) {
    expected.push_back(std::move(value));
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    for (;;) {
      { std::lock_guard lock(mutex); if (values.size() >= expected.size()) break; }
      if (std::chrono::steady_clock::now() >= deadline)
        throw std::runtime_error("timed out waiting for graph value " + expected.back());
      userver::engine::SleepFor(10ms);
    }
    std::lock_guard lock(mutex);
    // The following record may already have reached the graph; compare the
    // ordered prefix and check the complete sequence at the final barrier.
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), values.begin()));
    EXPECT_EQ(errors.load(), 0U);
  };
  ordinary.send(topic, "baseline"); expect("baseline");
  transactional.begin(); transactional.send(topic, "aborted"); transactional.abort();
  ordinary.send(topic, "after-abort"); expect("after-abort");
  transactional.begin(); transactional.send(topic, "open-then-committed");
  ordinary.send(topic, "behind-open");
  ordinary.send(topic, "other-partition", 1); expect("other-partition");
  {
    userver::engine::SleepFor(500ms);
    std::lock_guard lock(mutex);
    EXPECT_EQ(values, expected);
  }
  transactional.commit(); expect("open-then-committed"); expect("behind-open");
  transactional.begin(); transactional.send(topic, "aborted-again"); transactional.abort();
  transactional.begin(); transactional.send(topic, "reused-producer-commit"); transactional.commit();
  ordinary.send(topic, "final-barrier"); expect("reused-producer-commit"); expect("final-barrier");
  std::lock_guard lock(mutex);
  EXPECT_EQ(values, expected);
  EXPECT_EQ(errors.load(), 0U);
}

}  // namespace native_kafka_visibility

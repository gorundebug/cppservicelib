#pragma once

#include "test_callback_failure.hpp"

#include <array>
#include <atomic>
#include <string>

// Review instrumentation: local graph delivery must not invoke these methods.
struct UserverReviewValue final {
  int value{};
  std::string large = std::string(65536, 'v');
};

struct UserverReviewOpaque final {
  int value{};
  std::string large = std::string(65536, 'o');
};

inline std::atomic<int> userverReviewSerdeCalls{0};

namespace servicelib::serde {
class UserverReviewSerde final : public Serde<UserverReviewValue> {
 public:
  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const UserverReviewValue&) const override {
    ++userverReviewSerdeCalls;
    throw std::logic_error("unexpected local serialization");
  }
  UserverReviewValue Deserialize(SerdeView) const override {
    ++userverReviewSerdeCalls;
    throw std::logic_error("unexpected local deserialization");
  }
};

template <>
struct DefaultSerdeFactory<UserverReviewValue> final {
  static std::shared_ptr<const Serde<UserverReviewValue>> Make(SerdeLimits) {
    return std::make_shared<UserverReviewSerde>();
  }
};
}  // namespace servicelib::serde

namespace {

class GraphReviewConfig final : public servicelib::config::IConfig {
 public:
  servicelib::config::ServiceConfig service;
  std::vector<servicelib::config::StreamConfigRef> streams;
  std::vector<servicelib::config::LinkConfig> links;

  GraphReviewConfig() {
    service.id = 1;
    service.name = "userver-graph-review";
    service.defaultCallSemantics = servicelib::config::MakeCallSemanticsGroup(
        servicelib::api::CallSemantics::kFunctionCall);
  }
  std::vector<const servicelib::config::ServiceConfig*> GetServices() const override {
    return {&service};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return streams;
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors() const override { return {}; }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints() const override { return {}; }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override { return {}; }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override {
    std::vector<const servicelib::config::LinkConfig*> result;
    for (const auto& link : links) result.push_back(&link);
    return result;
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules() const override { return {}; }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override { return {}; }
};

// A separate CRTP specialization per probe avoids the legacy singleton runtime
// being reused after its owning environment has been destroyed.
template <int Tag>
class GraphReviewEnvironment final
    : public servicelib::StreamExecutionEnvironment<GraphReviewEnvironment<Tag>, OperatorDataTypes> {
 public:
  using Base = servicelib::StreamExecutionEnvironment<GraphReviewEnvironment<Tag>, OperatorDataTypes>;
  explicit GraphReviewEnvironment(const GraphReviewConfig& config)
      : config_(std::make_shared<servicelib::config::RuntimeConfig>(config)) {}
  ~GraphReviewEnvironment() { static_cast<void>(this->stopExecutionRuntime()); }
  std::shared_ptr<const servicelib::config::RuntimeConfig> getRuntimeConfigSnapshot() const override {
    return config_;
  }
  using Base::startExecutionRuntime;
  using Base::stopExecutionRuntime;

 private:
  std::shared_ptr<const servicelib::config::RuntimeConfig> config_;
};

template <typename Config>
Config graphReviewNode(int id, const std::string& name) {
  Config result;
  result.id = id;
  result.name = name;
  return result;
}

UTEST_MT(GraphParityReview, MixedSplitMergeAndParallelDrainUseRealScheduler, 1) {
  using namespace servicelib;
  GraphReviewConfig config;
  auto inputConfig = graphReviewNode<config::InputStreamConfig>(1, "request");
  auto splitConfig = graphReviewNode<config::SplitStreamConfig>(2, "split");
  std::array<config::MapStreamConfig, 4> maps;
  for (int i = 0; i < 4; ++i) {
    maps[i] = graphReviewNode<config::MapStreamConfig>(3 + i, "map-" + std::to_string(i));
  }
  auto mergeConfig = graphReviewNode<config::MergeStreamConfig>(7, "merge");
  auto sinkConfig = graphReviewNode<config::SinkStreamConfig>(8, "output");
  config.streams = {inputConfig, splitConfig, maps[0], maps[1], maps[2], maps[3], mergeConfig, sinkConfig};
  for (const int id : {4, 6}) {
    config::LinkConfig link;
    link.from = 2;
    link.to = id;
    link.callSemantics = config::MakeCallSemanticsGroup(api::CallSemantics::kFunctionCall, {}, 0, true);
    config.links.push_back(link);
  }
  config::LinkConfig parallel;
  parallel.from = 2;
  parallel.to = 5;
  parallel.callSemantics = config::MakeCallSemanticsGroup(api::CallSemantics::kParallelCall);
  config.links.push_back(parallel);

  std::vector<int> observed;
  std::array<userver::engine::SingleConsumerEvent, 2> entered, release, delivered;
  std::atomic<bool> stopped{false};
  GraphReviewEnvironment<1> environment(config);
  using Context = GraphReviewEnvironment<1>::Base;
  auto input = makeInputStream<int, int, int, Context>(inputConfig, nullptr, environment);
  auto& split = input->template split<4>(splitConfig);
  auto mapper = [&](int branch) {
    return make_function([&, branch](MessageContext context, auto&, const int& value, auto&& out) {
      EXPECT_EQ(context.streamId(), "request-" + std::to_string(value));
      EXPECT_TRUE(context.hasPriority());
      EXPECT_EQ(context.priority(), 0);
      if (branch == 3) {
        entered[value - 1].Send();
        EXPECT_TRUE(release[value - 1].WaitForEventFor(userver::utest::kMaxTestWaitTime));
      }
      out.out(context, value * 10 + branch);
    }, "graph-review-map");
  };
  auto& first = split.template get<0>().map(maps[0], StreamType<int>{}, mapper(1));
  auto& second = split.template get<1>().map(maps[1], StreamType<int>{}, mapper(2));
  auto& third = split.template get<2>().map(maps[2], StreamType<int>{}, mapper(3));
  auto& fourth = split.template get<3>().map(maps[3], StreamType<int>{}, mapper(4));
  auto& merge = first.merge(mergeConfig, second, third, fourth);
  merge.sink(sinkConfig, StreamType<int>{}, make_function([&](MessageContext context, const int& value) {
    EXPECT_EQ(context.streamId(), "request-" + std::to_string(value / 10));
    observed.push_back(value);
    if (value % 10 == 3) delivered[value / 10 - 1].Send();
  }, "graph-review-output"));
  environment.startExecutionRuntime();

  input->consume(MessageContext{}.withStreamId("request-1").withPriority(0), Payload<int>::make(1));
  EXPECT_EQ(observed, (std::vector<int>{12, 14, 11}));
  EXPECT_TRUE(entered[0].WaitForEventFor(userver::utest::kMaxTestWaitTime));
  release[0].Send();
  EXPECT_TRUE(delivered[0].WaitForEventFor(userver::utest::kMaxTestWaitTime));
  input->consume(MessageContext{}.withStreamId("request-2").withPriority(0), Payload<int>::make(2));
  EXPECT_EQ(observed, (std::vector<int>{12, 14, 11, 13, 22, 24, 21}));
  EXPECT_TRUE(entered[1].WaitForEventFor(userver::utest::kMaxTestWaitTime));
  auto stop = userver::utils::Async("graph-review-stop", [&] {
    EXPECT_TRUE(environment.stopExecutionRuntime());
    stopped.store(true);
  });
  userver::engine::SleepFor(std::chrono::milliseconds(5));
  EXPECT_FALSE(stopped.load());
  release[1].Send();
  EXPECT_TRUE(delivered[1].WaitForEventFor(userver::utest::kMaxTestWaitTime));
  stop.Get();
  EXPECT_TRUE(stopped.load());
  EXPECT_EQ(observed, (std::vector<int>{12, 14, 11, 13, 22, 24, 21, 23}));
}

template <typename Value, int Tag>
void checkLocalGraphSerde(bool stub) {
  using namespace servicelib;
  GraphReviewConfig config;
  auto inputConfig = graphReviewNode<config::InputStreamConfig>(1, "request");
  auto firstConfig = graphReviewNode<config::FilterStreamConfig>(2, "parent-serde");
  auto mapConfig = graphReviewNode<config::MapStreamConfig>(3, "output-serde");
  auto secondConfig = graphReviewNode<config::FilterStreamConfig>(4, "output-child-serde");
  auto sinkConfig = graphReviewNode<config::SinkStreamConfig>(5, "result");
  config.streams = {inputConfig, firstConfig, mapConfig, secondConfig, sinkConfig};
  int observed = 0;
  GraphReviewEnvironment<Tag> environment(config);
  using Context = typename GraphReviewEnvironment<Tag>::Base;
  auto input = makeInputStream<Value, int, int, Context>(inputConfig, nullptr, environment);
  auto accept = [](MessageContext, auto&, const Value&) { return true; };
  auto& first = input->filter(firstConfig, make_function(std::ref(accept), "accept"));
  auto& mapped = first.map(mapConfig, StreamType<Value>{}, make_function(
      [](MessageContext context, auto&, const Value& value, auto&& out) { out.out(context, value); }, "identity"));
  auto& second = mapped.filter(secondConfig, make_function(std::ref(accept), "accept"));
  second.sink(sinkConfig, StreamType<int>{}, make_function([&](MessageContext, const Value& value) {
    observed = value.value;
    EXPECT_EQ(value.large.size(), 65536);
  }, "result"));
  ASSERT_NE(input->getSerde(), nullptr);
  ASSERT_NE(mapped.getSerde(), nullptr);
  EXPECT_EQ(first.getSerde(), input->getSerde());
  EXPECT_NE(mapped.getSerde(), input->getSerde());
  EXPECT_EQ(second.getSerde(), mapped.getSerde());
  EXPECT_EQ(input->getSerde()->ValueSerializer()->IsStub(), stub);
  EXPECT_EQ(mapped.getSerde()->ValueSerializer()->IsStub(), stub);
  environment.startExecutionRuntime();
  Value value;
  value.value = 42;
  input->consume(MessageContext{}.withStreamId("local-payload"), Payload<Value>::make(std::move(value)));
  EXPECT_EQ(observed, 42);
  EXPECT_EQ(userverReviewSerdeCalls.load(), 0);
  EXPECT_TRUE(environment.stopExecutionRuntime());
}

UTEST(GraphParityReview, LocalEdgesInheritOrResolveSerdeWithoutEncoding) {
  userverReviewSerdeCalls.store(0);
  checkLocalGraphSerde<UserverReviewValue, 2>(false);
  checkLocalGraphSerde<UserverReviewOpaque, 3>(true);
}

UTEST(GraphParityReview, ParallelCoroutineCancellationDrainsActiveWork) {
  GraphReviewConfig config;
  std::atomic<int> reached{0};
  GraphReviewEnvironment<4> environment(config);
  environment.startExecutionRuntime();
  environment.parallel([&] {
    ++reached;
    userver::engine::current_task::RequestCancel();
    userver::engine::current_task::CancellationPoint();
    ADD_FAILURE() << "CancellationPoint must unwind the callback";
  });
  environment.parallel([&] { ++reached; });
  EXPECT_TRUE(environment.stopExecutionRuntime());
  EXPECT_EQ(reached.load(), 2);
}

UTEST(GraphParityReview, UnhandledFailureTerminatesProcess) {
  if (!std::getenv("SERVICELIB_FATAL_CALLBACK_CHILD")) {
    GTEST_SKIP() << "Executed by the callback failure subprocess contract";
  }
  GraphReviewConfig config;
  GraphReviewEnvironment<5> environment(config);
  environment.startExecutionRuntime();
  environment.parallel(ThrowUnhandledCallbackFailure);
  static_cast<void>(environment.stopExecutionRuntime());
  FAIL() << "unhandled callback exception did not terminate the process";
}

}  // namespace

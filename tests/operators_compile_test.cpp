#include <gtest/gtest.h>

#include <userver/engine/single_consumer_event.hpp>
#include <userver/utest/utest.hpp>

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <servicelib/runtime/caller.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/transformation/streams.hpp>

// This translation unit intentionally includes the complete public stream API.
// Most operators are templates, so compiling the header is the first parity
// check: an operator that is not reachable from Stream fails this target.

#include "typed_graph_test.hpp"

TEST(Operators, PublicApiHeadersCompileTogether) { SUCCEED(); }

namespace {

struct OperatorDataTypes final {
  template <typename>
  struct DataType {};
};

class OperatorApp final
    : public servicelib::StreamApp<OperatorApp, OperatorDataTypes> {
 public:
  void streamsInit() {}
  int start() { return 0; }
  void delay(servicelib::Context, servicelib::pool::IDelayPool::Duration,
             std::function<void()> task) override {
    task();
  }
  void parallel(std::function<void()> task) override { task(); }
};

OperatorApp& operatorApp() {
  static OperatorApp& app = OperatorApp::createStreamApp();
  return app;
}

servicelib::config::SinkStreamConfig sinkConfig(int id, std::string name) {
  servicelib::config::SinkStreamConfig config;
  config.id = id;
  config.name = std::move(name);
  return config;
}

template <typename T, typename R = std::monostate, typename E = int>
auto inputStream(OperatorApp& app, int id, std::string name) {
  servicelib::config::InputStreamConfig config;
  config.id = id;
  config.name = std::move(name);
  return servicelib::makeInputStream<T, R, E,
                                     OperatorApp::TStreamExecutionEnvironment>(
      config, nullptr, app);
}

servicelib::CallerBase::Params callerParams() {
  auto scope = servicelib::metrics::NoopMetrics::instance().scope("", {});
  return {.sourceName = "caller-source",
          .consumerName = "caller-consumer",
          .tracer = {},
          .messagesCounter = scope->counter("", "")};
}

class ImmediateTaskPool final : public servicelib::pool::ITaskPool {
 public:
  const std::string& getName() const noexcept override {
    ++nameReads;
    return name_;
  }
  mutable std::size_t nameReads{};
  int getExecutorsCount() const override { return 1; }
  void start(servicelib::Context) override {}
  void stop(servicelib::Context) override {}
  void addTask(servicelib::Context context,
               std::function<void()> task) override {
    lastCancelled = context.cancelled();
    task();
  }

  bool lastCancelled{};

 private:
  std::string name_{"task-pool"};
};

class ImmediatePriorityPool final
    : public servicelib::pool::IPriorityTaskPool {
 public:
  const std::string& getName() const noexcept override {
    ++nameReads;
    return name_;
  }
  mutable std::size_t nameReads{};
  int getExecutorsCount() const override { return 1; }
  void start(servicelib::Context) override {}
  void stop(servicelib::Context) override {}
  void addTask(servicelib::Context context, int priority,
               std::function<void()> task) override {
    lastCancelled = context.cancelled();
    lastPriority = priority;
    task();
  }

  int lastPriority{-1};
  bool lastCancelled{};

 private:
  std::string name_{"priority-pool"};
};

class MoveOnlyFilter final {
 public:
  explicit MoveOnlyFilter(std::unique_ptr<int> minimum)
      : minimum_(std::move(minimum)) {}
  MoveOnlyFilter(const MoveOnlyFilter&) = delete;
  MoveOnlyFilter& operator=(const MoveOnlyFilter&) = delete;
  MoveOnlyFilter(MoveOnlyFilter&&) = default;
  MoveOnlyFilter& operator=(MoveOnlyFilter&&) = default;

  bool operator()(servicelib::MessageContext, servicelib::StreamBase&,
                  const int& value) const {
    return value >= *minimum_;
  }

 private:
  std::unique_ptr<int> minimum_;
};

}  // namespace

TEST(Operators, GraphCanReferenceOneMoveOnlyFunctionWithoutCopyingIt) {
  auto& app = operatorApp();
  auto input = inputStream<int>(app, 170, "move-only-input");
  auto function = std::make_unique<MoveOnlyFilter>(std::make_unique<int>(2));
  servicelib::config::FilterStreamConfig filterConfig;
  filterConfig.id = 171;
  filterConfig.name = "move-only";
  auto& filtered = input->filter(
      filterConfig, servicelib::StreamFunction(std::ref(*function)));
  int observed = 0;
  filtered.sink(
      sinkConfig(172, "move-only-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&observed](servicelib::MessageContext, const int& value) {
            observed += value;
          }));
  input->consume({}, servicelib::Payload<int>::make(1));
  input->consume({}, servicelib::Payload<int>::make(3));
  EXPECT_EQ(observed, 3);
}

TEST(Operators, SplitBranchesInheritRuntimeEnvironment) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<int>(app, 30, "split-input");

  servicelib::config::SplitStreamConfig config;
  config.id = 31;
  config.name = "split";
  auto& split = inputOwner->template split<2>(config);

  EXPECT_EQ(split.getEnv(), &app);
  EXPECT_EQ(split.template get<0>().getEnv(), &app);
  EXPECT_EQ(split.template get<1>().getEnv(), &app);
}

TEST(Operators, RegisteredInputFeedsConfiguredTerminalSink) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<int>(app, 40, "input");
  auto& input = *inputOwner;
  servicelib::config::SinkStreamConfig config;
  config.id = 41;
  config.name = "result-sink";
  config.idEndpoint = 17;

  int observed = 0;
  auto& sink =
      input.sink(config, servicelib::StreamType<int>{},
                 servicelib::StreamFunction(
                     [&observed](servicelib::MessageContext, const int& value) {
                       observed = value;
                     }));

  EXPECT_EQ(sink.getConfigId(), 41);
  EXPECT_EQ(sink.getEndpointId(), 17);
  input.consume(servicelib::MessageContext{},
                servicelib::Payload<int>::make(42));
  EXPECT_EQ(observed, 42);
}

TEST(Operators, ConfiguredInputOwnsEndpointResultAndErrorChannels) {
  auto& app = operatorApp();
  using Environment = OperatorApp::TStreamExecutionEnvironment;

  servicelib::config::InputStreamConfig config;
  config.id = 71;
  config.name = "http-input";
  config.idEndpoint = 29;
  auto inputOwner =
      servicelib::makeInputStream<int, std::string, int, Environment>(
          config, nullptr, app);
  auto& input = *inputOwner;

  int value = 0;
  input.sink(sinkConfig(72, "input-values"), servicelib::StreamType<int>{},
             servicelib::StreamFunction(
                 [&value](servicelib::MessageContext, const int& current) {
                   value = current;
                 }));

  int error = 0;
  input.getErrorStream().sink(
      sinkConfig(73, "input-errors"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&error](servicelib::MessageContext, const int& current) {
            error = current;
          }));

  auto resultSourceOwner =
      inputStream<std::string>(app, 74, "http-result-source");
  auto& resultSource = *resultSourceOwner;
  input.setSource(resultSource);

  std::string result;
  input.setResultConsumer([&result](servicelib::MessageContext,
                                    servicelib::Payload<std::string> payload) {
    result = payload.get();
  });

  input.consume(servicelib::MessageContext{},
                servicelib::Payload<int>::make(11));
  input.consumeError(servicelib::MessageContext{},
                     servicelib::Payload<int>::make(7));
  resultSource.consume(servicelib::MessageContext{},
                       servicelib::Payload<std::string>::make("ok"));

  EXPECT_EQ(input.getConfigId(), 71);
  EXPECT_EQ(input.getEndpointId(), 29);
  EXPECT_EQ(input.getResultStream(), &resultSource);
  EXPECT_EQ(value, 11);
  EXPECT_EQ(error, 7);
  EXPECT_EQ(result, "ok");
}

TEST(Operators, ProcessExposesGoStyleErrorOutput) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<long>(app, 50, "process-input");
  auto& input = *inputOwner;

  servicelib::config::ProcessStreamConfig processConfig;
  processConfig.id = 51;
  auto& process =
      input.process(processConfig, servicelib::StreamType<int>{},
                    servicelib::StreamType<int>{},
                    servicelib::StreamFunction(
                        [](servicelib::MessageContext context,
                           servicelib::StreamBase&, const long& value,
                           auto&& output, auto&& errors) {
                          if (value >= 0) {
                            output.out(context, static_cast<int>(value));
                          } else {
                            errors.out(context, static_cast<int>(-value));
                          }
                        }));

  int observed = 0;
  auto& sink = process.sink(
      sinkConfig(52, "process-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&observed](servicelib::MessageContext, const int& value) {
            observed += value;
          }));
  process.getErrorStream().setConsumer(sink);

  input.consume(servicelib::MessageContext{},
                servicelib::Payload<long>::make(7));
  input.consume(servicelib::MessageContext{},
                servicelib::Payload<long>::make(-5));
  EXPECT_EQ(observed, 12);
}

TEST(Operators, SinkResultReentersTheStreamGraph) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<short>(app, 60, "sink-input");
  auto& input = *inputOwner;

  servicelib::config::SinkStreamConfig config;
  config.id = 61;
  config.idEndpoint = 23;

  short request = 0;
  auto& sink = input.sinkWithResult(
      config, servicelib::StreamType<std::string>{},
      servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&request](servicelib::MessageContext, const short& value) {
            request = value;
          }));

  std::string result;
  sink.sink(sinkConfig(62, "sink-result"), servicelib::StreamType<int>{},
            servicelib::StreamFunction(
                [&result](servicelib::MessageContext,
                          const std::string& value) { result = value; }));

  const auto context = servicelib::MessageContext{}.withStreamId("request-1");
  input.consume(context, servicelib::Payload<short>::make(9));
  sink.consumeResult(context, std::string{"done"});

  EXPECT_EQ(request, 9);
  EXPECT_EQ(result, "done");
  EXPECT_EQ(sink.getEndpointId(), 23);
}

TEST(Operators, DelayUsesRuntimeSchedulerAndPreservesMessageContext) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<int>(app, 80, "delay-input");
  auto& input = *inputOwner;
  servicelib::config::DelayStreamConfig delayConfig;
  delayConfig.id = 81;

  bool sawDeadline = false;
  auto duration = [&sawDeadline](servicelib::MessageContext context,
                                 servicelib::StreamBase&, const int&) {
    sawDeadline = context.deadline().has_value();
    return std::chrono::milliseconds(1);
  };
  auto& delayed = input.delay(
      delayConfig,
      servicelib::make_function(std::move(duration), "context-aware-delay"));
  int observed = 0;
  delayed.sink(
      sinkConfig(82, "delayed-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&observed](servicelib::MessageContext context, const int& value) {
            EXPECT_EQ(context.streamId(), "delayed-message");
            observed = value;
          }));

  input.consume(servicelib::MessageContext{}
                    .withStreamId("delayed-message")
                    .withDeadline(std::chrono::steady_clock::now() +
                                  std::chrono::seconds(1)),
                servicelib::Payload<int>::make(42));

  EXPECT_TRUE(sawDeadline);
  EXPECT_EQ(observed, 42);
}

UTEST(Operators, TypedCallerPreservesAllModesAndSharedStatistics) {
  auto& app = operatorApp();
  auto owner = inputStream<int>(app, 29100, "typed-owner");
  std::vector<std::pair<int, std::string>> received;
  auto& sink = owner->sink(
      sinkConfig(29101, "typed-sink"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&received](servicelib::MessageContext context, const int& value) {
            if (value < 0) throw std::runtime_error("consumer failure");
            received.emplace_back(value, context.streamId());
          }));
  using Consumer = std::remove_reference_t<decltype(sink)>;
  using View = servicelib::Caller<int, Consumer>;
  static_assert(std::is_copy_constructible_v<View>);
  static_assert(!std::is_same_v<View, servicelib::Caller<int>>);

  servicelib::DirectCaller<int, Consumer> direct{sink, callerParams()};
  View typed{direct};
  const auto copied = typed;
  servicelib::Caller<int>& erased = direct;
  typed.consume(servicelib::MessageContext{}.withStreamId("typed"),
                servicelib::Payload<int>::make(1));
  copied.consume(servicelib::MessageContext{}.withStreamId("copied"),
                 servicelib::Payload<int>::make(2));
  erased.consume(servicelib::MessageContext{}.withStreamId("erased"),
                 servicelib::Payload<int>::make(3));
  EXPECT_FALSE(typed.isAsync());
  EXPECT_EQ(&typed.statistics(), &erased.statistics());
  EXPECT_EQ(&copied.statistics(), &erased.statistics());
  EXPECT_EQ(typed.statistics().count(), 3);
  EXPECT_THROW(typed.consume(servicelib::MessageContext{},
                            servicelib::Payload<int>::make(-1)),
               std::runtime_error);
  EXPECT_EQ(typed.statistics().count(), 4);

  servicelib::DirectCaller<int, Consumer> metadataAsync{
      sink, callerParams(), true};
  View asyncView{metadataAsync};
  asyncView.consume(servicelib::MessageContext{}.withStreamId("metadata"),
                    servicelib::Payload<int>::make(4));
  EXPECT_TRUE(asyncView.isAsync());
  ASSERT_EQ(received.size(), 4);

  ImmediateTaskPool pool;
  servicelib::testlog::TestLog logger;
  servicelib::TaskPoolCaller<int, Consumer> task{
      sink, pool, logger, callerParams()};
  View taskView{task};
  taskView.consume(servicelib::MessageContext{}.withStreamId("task"),
                   servicelib::Payload<int>::make(5));
  EXPECT_TRUE(taskView.isAsync());
  EXPECT_EQ(task.statistics().count(), 1);
  EXPECT_EQ(&taskView.statistics(), &task.statistics());

  ImmediatePriorityPool priorityPool;
  servicelib::PriorityTaskPoolCaller<int, Consumer> priority{
      sink, priorityPool, 17, logger, callerParams()};
  View priorityView{priority};
  priorityView.consume(servicelib::MessageContext{}.withStreamId("priority"),
                       servicelib::Payload<int>::make(6));
  EXPECT_EQ(priorityPool.lastPriority, 17);
  priorityView.consume(servicelib::MessageContext{}.withStreamId("zero").withPriority(0),
                       servicelib::Payload<int>::make(7));
  EXPECT_EQ(priorityPool.lastPriority, 0);
  EXPECT_TRUE(priorityView.isAsync());
  EXPECT_EQ(priorityView.statistics().count(), 2);

  // OperatorApp executes parallel callbacks inline: this checks dispatch,
  // while the existing pool tests cover actual worker scheduling.
  servicelib::ParallelCaller<int, Consumer> parallel{
      sink, app, callerParams()};
  View parallelView{parallel};
  parallelView.consume(servicelib::MessageContext{}.withStreamId("parallel"),
                       servicelib::Payload<int>::make(8));
  EXPECT_TRUE(parallelView.isAsync());
  EXPECT_EQ(parallelView.statistics().count(), 1);
  EXPECT_EQ(received, (std::vector<std::pair<int, std::string>>{
      {1, "typed"}, {2, "copied"}, {3, "erased"}, {4, "metadata"},
      {5, "task"}, {6, "priority"}, {7, "zero"}, {8, "parallel"}}));

  servicelib::DirectCaller<int> legacy{sink, callerParams()};
  EXPECT_THROW((View{legacy}), std::logic_error);
}

UTEST(Operators, TypedCallerFactoryIsOptInAndRegistrySharesTheEdge) {
  auto& app = operatorApp();
  auto owner = inputStream<int>(app, 29200, "typed-factory-owner");
  int total = 0;
  auto& sink = owner->sink(
      sinkConfig(29201, "typed-factory-sink"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&total](servicelib::MessageContext, const int& value) { total += value; }));
  using Consumer = std::remove_reference_t<decltype(sink)>;
  using Producer = std::remove_reference_t<decltype(*owner)>;
  using View = servicelib::Caller<int, Consumer>;

  auto legacy = servicelib::makeCallerFromEnv<int>(
      *owner, sink, nullptr, {29200, 29201});
  EXPECT_NE(dynamic_cast<servicelib::DirectCaller<int>*>(legacy.get()), nullptr);
  EXPECT_THROW((View{*legacy}), std::logic_error);
  auto concrete = servicelib::makeCallerFromEnv<int, Producer, Consumer>(
      *owner, sink, nullptr, {29200, 29201});
  View concreteView{*concrete};
  concreteView.consume(servicelib::MessageContext{}, servicelib::Payload<int>::make(1));
  EXPECT_EQ(concrete->statistics().count(), 1);

  auto source = inputStream<int>(app, 29202, "typed-registry-source");
  auto typed = app.prepareTypedCaller<int>(*source, sink);
  auto again = app.prepareTypedCaller<int>(*source, sink);
  auto* erased = app.prepareCaller<int>(*source, sink);
  EXPECT_EQ(&typed.statistics(), &erased->statistics());
  EXPECT_EQ(&again.statistics(), &erased->statistics());
  typed.consume(servicelib::MessageContext{}, servicelib::Payload<int>::make(2));
  erased->consume(servicelib::MessageContext{}, servicelib::Payload<int>::make(3));
  EXPECT_EQ(again.statistics().count(), 2);
  EXPECT_EQ(total, 6);

  auto erasedSource = inputStream<int>(app, 29203, "erased-registry-source");
  auto* old = app.prepareCaller<int>(*erasedSource, sink);
  EXPECT_THROW(app.prepareTypedCaller<int>(*erasedSource, sink), std::logic_error);
  EXPECT_EQ(app.prepareCaller<int>(*erasedSource, sink), old);
  EXPECT_EQ(old->statistics().count(), 0);
}

UTEST(Operators, CallerSemanticsDispatchPreserveContextPriorityAndStatistics) {
  auto& app = operatorApp();
  auto inputOwner = inputStream<int>(app, 150, "caller-input");
  struct DeliveryState final {
    std::mutex mutex;
    userver::engine::SingleConsumerEvent delivered;
    std::vector<std::pair<int, std::string>> observed;
  } state;
  auto& sink = inputOwner->sink(
      sinkConfig(151, "caller-output"), servicelib::StreamType<int>{},
      servicelib::StreamFunction(
          [&state](servicelib::MessageContext context, const int& value) {
            bool complete = false;
            {
              std::lock_guard lock(state.mutex);
              state.observed.emplace_back(value, context.streamId());
              complete = state.observed.size() == 6;
            }
            if (complete) state.delivered.Send();
          }));

  servicelib::DirectCaller<int> direct{sink, callerParams(), false};
  direct.consume(servicelib::MessageContext{}.withStreamId("direct"),
                 servicelib::Payload<int>::make(1));
  EXPECT_FALSE(direct.isAsync());
  EXPECT_EQ(direct.statistics().count(), 1);

  servicelib::DirectCaller<int> metadataAsync{sink, callerParams(), true};
  metadataAsync.consume(
      servicelib::MessageContext{}.withStreamId("function-async"),
      servicelib::Payload<int>::make(2));
  EXPECT_TRUE(metadataAsync.isAsync());

  ImmediateTaskPool taskPool;
  servicelib::testlog::TestLog logger;
  servicelib::TaskPoolCaller<int> task{sink, taskPool, logger, callerParams()};
  task.consume(servicelib::MessageContext{}.withStreamId("task"),
               servicelib::Payload<int>::make(3));
  EXPECT_TRUE(task.isAsync());
  EXPECT_FALSE(taskPool.lastCancelled);
  EXPECT_EQ(task.statistics().count(), 1);
  EXPECT_EQ(taskPool.nameReads, 0);

  ImmediatePriorityPool priorityPool;
  servicelib::PriorityTaskPoolCaller<int> priority{
      sink, priorityPool, 17, logger, callerParams()};
  priority.consume(
      servicelib::MessageContext{}.withStreamId("priority-default"),
      servicelib::Payload<int>::make(4));
  EXPECT_EQ(priorityPool.lastPriority, 17);
  priority.consume(servicelib::MessageContext{}
                       .withStreamId("priority-context")
                       .withPriority(0),
                   servicelib::Payload<int>::make(5));
  EXPECT_EQ(priorityPool.lastPriority, 0);
  EXPECT_EQ(priority.statistics().count(), 2);
  EXPECT_EQ(priorityPool.nameReads, 0);

  servicelib::ParallelCaller<int> parallel{sink, app, callerParams()};
  parallel.consume(servicelib::MessageContext{}.withStreamId("parallel"),
                   servicelib::Payload<int>::make(6));
  EXPECT_TRUE(parallel.isAsync());
  EXPECT_EQ(parallel.statistics().count(), 1);
  ASSERT_TRUE(
      state.delivered.WaitForEventFor(userver::utest::kMaxTestWaitTime));

  std::lock_guard lock(state.mutex);
  EXPECT_EQ(state.observed,
            (std::vector<std::pair<int, std::string>>{
                {1, "direct"}, {2, "function-async"}, {3, "task"},
                {4, "priority-default"}, {5, "priority-context"},
                {6, "parallel"}}));
}

#include "substream_test.hpp"
#include "graph_parity_review_test.hpp"

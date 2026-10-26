#include <atomic>
#include <chrono>
#include <optional>
#include <stop_token>
#include <thread>

#include <userver/engine/async.hpp>
#include <userver/engine/single_consumer_event.hpp>
#include <userver/utest/utest.hpp>

#include <servicelib/runtime/serviceapp.hpp>

namespace {
struct EmptyDataTypes {};

template <int Tag>
class Graph final
    : public servicelib::StreamExecutionEnvironment<Graph<Tag>, EmptyDataTypes> {
 public:
  using Base = servicelib::StreamExecutionEnvironment<Graph<Tag>, EmptyDataTypes>;
  using Base::startExecutionRuntime;
  using Base::drainExecutionRuntime;
  using Base::stopExecutionRuntime;
};

template <typename T>
concept ContextFreeStop = requires(T& graph) { graph.stopExecutionRuntime(); };
static_assert(!ContextFreeStop<Graph<1>>);

UTEST(GraphDrainContext, EveryStopTokenInterruptsWithoutAbandoningGraph) {
  Graph<1> graph;
  graph.startExecutionRuntime();
  std::optional<Graph<1>::InputInvocation> input{graph.beginInputInvocation()};
  for (int selected = 0; selected < 3; ++selected) {
    for (bool deadline : {false, true}) {
      std::array<std::stop_source, 3> stops;
      auto context = servicelib::Context{}.withStopToken(stops[0].get_token())
          .withExternalCancellation(stops[1].get_token())
          .withExternalCancellation(stops[2].get_token());
      if (deadline) context = context.bounded(std::chrono::seconds{5});
      userver::engine::SingleConsumerEvent entered;
      auto waiting = userver::engine::AsyncNoTracing([&] {
        entered.Send();
        return graph.stopExecutionRuntime(context);
      });
      EXPECT_TRUE(entered.WaitForEventFor(userver::utest::kMaxTestWaitTime));
      std::thread cancellation([&] { stops[selected].request_stop(); });
      cancellation.join();
      EXPECT_FALSE(waiting.Get());
      EXPECT_THROW(graph.beginInputInvocation(), servicelib::StreamException);
      EXPECT_THROW(graph.startExecutionRuntime(), std::logic_error);
    }
  }
  input.reset();
  EXPECT_TRUE(graph.stopExecutionRuntime(servicelib::Context{}));
}

UTEST(GraphDrainContext, DeadlineRetainsParallelWorkUntilSuccessfulRetry) {
  Graph<2> graph;
  graph.startExecutionRuntime();
  userver::engine::SingleConsumerEvent entered, release;
  std::atomic<bool> completed{false};
  graph.parallel([&] {
    entered.Send();
    EXPECT_TRUE(release.WaitForEventFor(userver::utest::kMaxTestWaitTime));
    completed.store(true);
  });
  EXPECT_TRUE(entered.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  EXPECT_FALSE(graph.stopExecutionRuntime(
      servicelib::Context{}.bounded(std::chrono::milliseconds{5})));
  EXPECT_FALSE(completed.load());
  EXPECT_FALSE(graph.stopExecutionRuntime(
      servicelib::Context{}.bounded(std::chrono::milliseconds{5})));
  release.Send();
  EXPECT_TRUE(graph.stopExecutionRuntime(servicelib::Context{}));
  EXPECT_TRUE(completed.load());
}

UTEST(GraphDrainContext, ConcurrentDrainsHaveIndependentCancellation) {
  Graph<3> graph;
  graph.startExecutionRuntime();
  std::optional<Graph<3>::InputInvocation> input{graph.beginInputInvocation()};
  std::stop_source cancellation;
  auto context = servicelib::Context{}.withStopToken(cancellation.get_token());
  auto first = userver::engine::AsyncNoTracing([&] {
    return graph.drainExecutionRuntime(context);
  });
  auto second = userver::engine::AsyncNoTracing([&] {
    return graph.drainExecutionRuntime(servicelib::Context{});
  });
  cancellation.request_stop();
  EXPECT_FALSE(first.Get());
  EXPECT_FALSE(second.IsFinished());
  input.reset();
  EXPECT_TRUE(second.Get());
  EXPECT_TRUE(graph.stopExecutionRuntime(context));
}
}  // namespace

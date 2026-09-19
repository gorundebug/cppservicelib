#pragma once

#include <atomic>
#include <stdexcept>
#include <stop_token>

#include <userver/engine/sleep.hpp>
#include <userver/utils/async.hpp>

namespace {

using TestSubStream = servicelib::SubStream<
    int, int, OperatorApp::TStreamExecutionEnvironment>;

template <typename Function>
std::shared_ptr<TestSubStream> subStreamForTest(int id, Function function) {
  auto& app = operatorApp();
  servicelib::config::SubStreamConfig config;
  config.id = id;
  config.idSource = id + 1;
  config.name = "substream-" + std::to_string(id);
  auto entry = servicelib::makeSubStream<
      int, int, OperatorApp::TStreamExecutionEnvironment>(config, app);
  servicelib::config::MapStreamConfig result;
  result.id = id + 1;
  result.idSource = id;
  result.name = config.name + "-result";
  auto& source = entry->map(
      result, servicelib::StreamType<int>{},
      servicelib::make_function(std::move(function), "substream-test"));
  entry->setSource(source);
  return entry;
}

template <typename Function>
auto subStreamCollector(Function function) {
  return std::make_shared<servicelib::SubStreamCollectorFunc<int>>(
      std::move(function));
}

auto subStreamForLateResultTest(
    int id, servicelib::MessageContext& savedContext,
    userver::engine::SingleConsumerEvent& entered) {
  auto& app = operatorApp();
  servicelib::config::SubStreamConfig config;
  config.id = id;
  config.idSource = id + 1;
  config.name = "substream-" + std::to_string(id);
  auto entry = servicelib::makeSubStream<
      int, int, OperatorApp::TStreamExecutionEnvironment>(config, app);
  servicelib::config::InputStreamConfig resultConfig;
  resultConfig.id = id + 1;
  resultConfig.name = config.name + "-late-result";
  auto source = servicelib::makeInputStream<
      int, int, int, OperatorApp::TStreamExecutionEnvironment>(
      resultConfig, nullptr, app);
  servicelib::config::MapStreamConfig bodyConfig;
  bodyConfig.id = id + 2;
  bodyConfig.idSource = id;
  bodyConfig.name = config.name + "-body";
  entry->map(bodyConfig, servicelib::StreamType<int>{},
             servicelib::make_function(
                 [&savedContext, &entered](auto context, auto&, int, auto&&) {
                   savedContext = std::move(context);
                   entered.Send();
                 },
                 "substream-late-result-test"));
  entry->setSource(*source);
  return std::make_pair(std::move(entry), std::move(source));
}

}  // namespace

TEST(SubStream, TypedContextKeysSurviveClonesButAreNotTransportIds) {
  servicelib::ContextKey<int> first;
  servicelib::ContextKey<int> second;
  const auto parent = servicelib::MessageContext{}.withStreamId("same");
  const auto bound = parent.withLocalValue(first, std::make_shared<int>(17));
  const auto child = bound.withPriority(5).withSampling(true).withStreamId("changed");
  ASSERT_TRUE(child.localValue(first));
  EXPECT_EQ(*child.localValue(first), 17);
  EXPECT_FALSE(child.localValue(second));
  EXPECT_FALSE(parent.localValue(first));
  EXPECT_FALSE(servicelib::MessageContext{}.withStreamId("same").localValue(first));
}

UTEST(SubStream, CollectorDecidesCompletionAndDropsLaterValues) {
  auto entry = subStreamForTest(500, [](auto context, auto&, int value, auto&& out) {
    out.out(context, value);
    out.out(context, value + 1);
    out.out(context, value + 2);
  });
  std::vector<int> values;
  entry->consume({}, servicelib::Payload<int>::make(10),
                 subStreamCollector([&](auto, int value) {
                   values.push_back(value);
                   return values.size() == 2;
                 }));
  EXPECT_EQ(values, (std::vector<int>{10, 11}));
}

UTEST(SubStream, HundredConcurrentCallsWithSameParentAreIsolated) {
  auto entry = subStreamForTest(510, [](auto context, auto&, int value, auto&& out) {
    userver::engine::SleepFor(std::chrono::milliseconds(1));
    out.out(context, value * 2);
  });
  const auto parent = servicelib::MessageContext{}.withStreamId("shared-parent");
  using Task = decltype(userver::utils::Async("substream", [] {}));
  std::vector<Task> tasks;
  for (int i = 0; i < 100; ++i) {
    tasks.push_back(userver::utils::Async("substream", [entry, parent, i] {
      int received = -1;
      entry->consume(parent, servicelib::Payload<int>::make(i),
                     subStreamCollector([&](auto context, int value) {
                       EXPECT_EQ(context.streamId(), parent.streamId());
                       received = value;
                       return true;
                     }));
      EXPECT_EQ(received, i * 2);
    }));
  }
  for (auto& task : tasks) task.Get();
}

UTEST(SubStream, RecursiveBusinessCallRestoresOuterContext) {
  std::weak_ptr<servicelib::ISubStream<int, int>> recursive;
  auto entry = subStreamForTest(520, [&recursive](auto context, auto&, int value, auto&& out) {
    if (value < 0) {
      recursive.lock()->consume(
          context, servicelib::Payload<int>::make(-value),
          subStreamCollector([&out](auto outerContext, int result) {
            out.out(outerContext, result + 1);
            return true;
          }));
    } else {
      out.out(context, value * 2);
    }
  });
  recursive = entry;
  int received = -1;
  entry->consume({}, servicelib::Payload<int>::make(-7),
                 subStreamCollector([&](auto, int value) {
                   received = value;
                   return true;
                 }));
  EXPECT_EQ(received, 15);
}

UTEST(SubStream, CollectorMayInvokeSameSubStream) {
  auto entry = subStreamForTest(530, [](auto context, auto&, int value, auto&& out) {
    out.out(context, value * 2);
  });
  int received = -1;
  entry->consume({}, servicelib::Payload<int>::make(3),
                 subStreamCollector([&](auto context, int value) {
                   entry->consume(context, servicelib::Payload<int>::make(value),
                                  subStreamCollector([&](auto, int nested) {
                                    received = nested;
                                    return true;
                                  }));
                   return true;
                 }));
  EXPECT_EQ(received, 12);
}

UTEST(SubStream, CancelledCallDropsRetainedLateResultAndReleasesCollector) {
  servicelib::MessageContext savedContext;
  userver::engine::SingleConsumerEvent entered;
  auto [entry, source] = subStreamForLateResultTest(540, savedContext, entered);
  std::stop_source stop;
  std::atomic<int> called{0};
  auto collector = subStreamCollector([&](auto, int) {
    ++called;
    return true;
  });
  std::weak_ptr<servicelib::SubStreamCollector<int>> weak = collector;
  auto task = userver::utils::Async("substream-cancel", [entry, &stop, collector] {
    entry->consume(servicelib::MessageContext{}.withStopToken(stop.get_token()),
                   servicelib::Payload<int>::make(1), collector);
  });
  collector.reset();
  ASSERT_TRUE(entered.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  stop.request_stop();
  EXPECT_THROW(task.Get(), std::runtime_error);
  task = {};
  EXPECT_TRUE(weak.expired());
  source->consume(savedContext, servicelib::Payload<int>::make(1));
  EXPECT_EQ(called.load(), 0);
}

UTEST(SubStream, DeadlineAndExternalCancellationEndWaiting) {
  auto entry = subStreamForTest(550, [](auto, auto&, int, auto&&) {});
  auto collector = subStreamCollector([](auto, int) { return false; });
  EXPECT_THROW(
      entry->consume(servicelib::MessageContext{}.withDeadline(
                         std::chrono::steady_clock::now() + std::chrono::milliseconds(5)),
                     servicelib::Payload<int>::make(1), collector),
      std::runtime_error);
  std::stop_source stop;
  stop.request_stop();
  EXPECT_THROW(
      entry->consume(servicelib::MessageContext{}.withExternalCancellation(stop.get_token()),
                     servicelib::Payload<int>::make(1), collector),
      std::runtime_error);
}

UTEST(SubStream, CollectorFailureReturnsToCaller) {
  auto entry = subStreamForTest(560, [](auto context, auto&, int value, auto&& out) {
    out.out(context, value);
  });
  EXPECT_THROW(entry->consume({}, servicelib::Payload<int>::make(1),
                             subStreamCollector([](auto, int) -> bool {
                               throw std::logic_error("collector failed");
                             })),
               std::logic_error);
}

UTEST(SubStream, CancellationDrainsAnActiveCollector) {
  userver::engine::SingleConsumerEvent entered;
  userver::engine::SingleConsumerEvent release;
  std::atomic<bool> returned{false};
  auto call = std::make_shared<servicelib::detail::SubStreamCall<int>>(
      servicelib::MessageContext{}, subStreamCollector([&](auto, int) {
        entered.Send();
        EXPECT_TRUE(release.WaitForEventFor(userver::utest::kMaxTestWaitTime));
        return true;
      }));
  auto delivery = userver::utils::Async("substream-delivery", [call] { call->deliver(1); });
  ASSERT_TRUE(entered.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  call->cancel();
  auto waiter = userver::utils::Async("substream-close", [call, &returned] {
    EXPECT_THROW(call->wait({}), std::runtime_error);
    call->close();
    returned = true;
  });
  userver::engine::SleepFor(std::chrono::milliseconds(5));
  EXPECT_FALSE(returned.load());
  release.Send();
  delivery.Get();
  waiter.Get();
  EXPECT_TRUE(returned.load());
}

UTEST(SubStream, TaskCancellationReleasesCallbackAndDropsLateValues) {
  servicelib::MessageContext savedContext;
  userver::engine::SingleConsumerEvent entered;
  auto [entry, source] = subStreamForLateResultTest(570, savedContext, entered);
  std::atomic<int> called{0};
  auto task = userver::utils::Async("substream-task-cancel", [entry, &called] {
    entry->consume({}, servicelib::Payload<int>::make(1),
                   subStreamCollector([&](auto, int) {
                     ++called;
                     return true;
                   }));
  });
  ASSERT_TRUE(entered.WaitForEventFor(userver::utest::kMaxTestWaitTime));
  task.RequestCancel();
  EXPECT_ANY_THROW(task.Get());
  source->consume(savedContext, servicelib::Payload<int>::make(1));
  EXPECT_EQ(called.load(), 0);
}

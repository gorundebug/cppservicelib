#include <array>
#include <functional>
#include <string>
#include <utility>

#include <gtest/gtest.h>
#include <servicelib/transformation/streams.hpp>

namespace {

struct NamedCaseTypes final {
  template <typename>
  struct DataType {};
};

class NamedCaseApp final
    : public servicelib::StreamApp<NamedCaseApp, NamedCaseTypes> {
 public:
  void streamsInit() {}
  int start() { return 0; }
  void delay(servicelib::Context, servicelib::pool::IDelayPool::Duration,
             std::function<void()> task) override {
    task();
  }
  void parallel(std::function<void()> task) override { task(); }
};

NamedCaseApp& app() {
  static auto& instance = NamedCaseApp::createStreamApp();
  return instance;
}

}  // namespace

TEST(NamedCase, SameTypedBranchesKeepMeaningAfterReordering) {
  const std::array<std::array<std::string, 3>, 3> orders{{
      {"accepted", "terminal", "unrelated"},
      {"terminal", "accepted", "unrelated"},
      {"unrelated", "terminal", "accepted"},
  }};
  int base = 0;
  for (const auto& names : orders) {
    base += 100;
    SCOPED_TRACE(names[0] + "/" + names[1] + "/" + names[2]);
    std::array<std::size_t, 2> indices{};
    servicelib::config::InputStreamConfig inputConfig;
    inputConfig.id = base + 1;
    inputConfig.name = "named-case-input";
    auto input = servicelib::makeInputStream<
        int, std::monostate, int, NamedCaseApp::TStreamExecutionEnvironment>(
        inputConfig, nullptr, app());
    servicelib::config::CaseStreamConfig caseConfig;
    caseConfig.id = base + 2;
    caseConfig.name = "named-case";
    auto& route = input->template case_<3>(
        caseConfig,
        servicelib::StreamFunction(
            [&indices](servicelib::MessageContext, servicelib::StreamBase&,
                       const int& value) -> std::size_t {
              return indices[value < 0 ? 1 : 0];
            }));
    std::string selected;
    int received = 0;
    int calls = 0;
    const auto configure = [&](auto& branch, std::size_t position) {
      servicelib::config::WhenStreamConfig whenConfig;
      whenConfig.id = base + 10 + position;
      whenConfig.name = names[position];
      branch.configure(whenConfig, nullptr, &app());
      servicelib::config::SinkStreamConfig sinkConfig;
      sinkConfig.id = base + 20 + position;
      // A shared consumer name must not become the branch identity.
      sinkConfig.name = "same-consumer-name";
      branch.sink(
          sinkConfig, servicelib::StreamType<int>{},
          servicelib::StreamFunction((
              [&, name = names[position]](servicelib::MessageContext,
                                          const int& value) {
                selected = name;
                received = value;
                ++calls;
              })));
    };
    configure(route.template get<0>(), 0);
    configure(route.template get<1>(), 1);
    configure(route.template get<2>(), 2);
    indices = {route.whenIndex("accepted"), route.whenIndex("terminal")};
    input->consume({}, servicelib::Payload<int>::make(7));
    EXPECT_EQ(selected, "accepted");
    EXPECT_EQ(received, 7);
    EXPECT_EQ(calls, 1);
    input->consume({}, servicelib::Payload<int>::make(-9));
    EXPECT_EQ(selected, "terminal");
    EXPECT_EQ(received, -9);
    EXPECT_EQ(calls, 2);
#ifdef NDEBUG
    EXPECT_THROW(route.whenIndex("missing"), servicelib::StreamException);
    EXPECT_THROW(route.whenIndex(""), servicelib::StreamException);
#else
    EXPECT_DEATH(route.whenIndex("missing"), "StreamException");
    EXPECT_DEATH(route.whenIndex(""), "StreamException");
#endif
    servicelib::config::WhenStreamConfig duplicate;
    duplicate.id = base + 12;
    duplicate.name = names[0];
    route.template get<2>().configure(duplicate, nullptr, &app());
#ifdef NDEBUG
    EXPECT_THROW(route.whenIndex(names[0]), servicelib::StreamException);
#else
    EXPECT_DEATH(route.whenIndex(names[0]), "StreamException");
#endif
  }
}

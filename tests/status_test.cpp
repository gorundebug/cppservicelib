#include <string>

#include <userver/formats/json/serialize.hpp>
#include <userver/utest/utest.hpp>

#include <servicelib/runtime/status/status.hpp>
#include <servicelib/runtime/telemetry/userver/status.hpp>
#include "mockservice/config/config.hpp"

UTEST(Status, BuildsLiveTopologyDataAndGraphYaml) {
  const auto config = mockservice::config::MakeConfig();
  const servicelib::config::RuntimeConfig runtime(config);

  const auto data = userver::formats::json::FromString(
      servicelib::status::MakeNetworkDataJson(
          runtime, [](servicelib::config::LinkID link) {
            return static_cast<std::int64_t>(link.from * 100 + link.to);
          }));

  const auto input =
      runtime.GetStreamConfigByID(mockservice::config::kInputRequestId);
  ASSERT_TRUE(input.has_value());
  EXPECT_EQ(servicelib::status::StreamIconPath(runtime, *input),
            servicelib::status::kApiIcon);

  ASSERT_TRUE(data.HasMember("nodes"));
  ASSERT_TRUE(data.HasMember("edges"));
  ASSERT_FALSE(data["nodes"].IsEmpty());
  const auto image =
      data["nodes"][0]["image"]["unselected"].As<std::string>();
  const auto second_image =
      data["nodes"][1]["image"]["unselected"].As<std::string>();
  EXPECT_TRUE(image.starts_with("data:image/svg+xml;charset=utf-8,"));
  EXPECT_NE(image.find("%3Csvg"), std::string::npos);
  EXPECT_NE(image.find("rx=%2230%22"), std::string::npos);
  EXPECT_NE(second_image.find("rx=%2210%22"), std::string::npos);
  EXPECT_NE(image, second_image);

  const auto yaml = servicelib::status::MakeGraphYaml(runtime);
  EXPECT_NE(yaml.find("services:"), std::string::npos);
  EXPECT_NE(yaml.find("streams:"), std::string::npos);
  EXPECT_NE(yaml.find("pools:"), std::string::npos);
  EXPECT_NE(yaml.find("links:"), std::string::npos);
  EXPECT_NE(yaml.find("callSemantics: PriorityTaskPool"), std::string::npos);
  EXPECT_NE(yaml.find("poolName: \"PriorityTaskPool\""), std::string::npos);
  EXPECT_NE(yaml.find("IncomeService"), std::string::npos);
}

UTEST(Status, EmbedsTheSameBrowserAssetsAsOtherRuntimes) {
  EXPECT_GT(servicelib::status::web::kStatusHtml.size(), 1000);
  EXPECT_NE(servicelib::status::web::kStatusHtml.find("new vis.DataSet"),
            std::string_view::npos);
  EXPECT_NE(servicelib::status::web::kStatusHtml.find(
                "window.setTimeout(refreshNetwork, 1000)"),
            std::string_view::npos);
  EXPECT_GT(servicelib::status::web::kVisJavaScript.size(), 100000);
  EXPECT_GT(servicelib::status::web::kVisCss.size(), 10000);
}

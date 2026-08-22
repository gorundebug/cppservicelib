#include <any>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <userver/utest/utest.hpp>

#include <servicelib/runtime/serde/serdeimpl.hpp>

namespace {

using servicelib::serde::SerdeData;
using servicelib::serde::SerdeError;
using servicelib::serde::SerdeLimits;

SerdeData Bytes(std::initializer_list<std::uint8_t> values) {
  SerdeData result;
  result.reserve(values.size());
  for (const auto value : values) {
    result.push_back(static_cast<std::byte>(value));
  }
  return result;
}

void Append(SerdeData& destination, const SerdeData& source) {
  destination.insert(destination.end(), source.begin(), source.end());
}

SerdeData Frame(const SerdeData& payload) {
  SerdeData result;
  servicelib::serde::detail::put_size(result, payload.size());
  Append(result, payload);
  return result;
}

template <typename T, typename Serde>
void ExpectRoundTrip(const Serde& serde, const T& value) {
  EXPECT_EQ(serde.Deserialize(serde.Serialize(value)), value);
}

UTEST(Serde, PrimitiveWireFormatMatchesGo) {
  const servicelib::serde::BoolSerde boolSerde;
  EXPECT_EQ(boolSerde.Serialize(false), Bytes({0x00}));
  EXPECT_EQ(boolSerde.Serialize(true), Bytes({0x01}));
  EXPECT_TRUE(boolSerde.Deserialize(Bytes({0xff})));

  const servicelib::serde::Int16Serde int16Serde;
  EXPECT_EQ(int16Serde.Serialize(std::numeric_limits<std::int16_t>::min()),
            Bytes({0x00, 0x00}));
  EXPECT_EQ(int16Serde.Serialize(-1), Bytes({0x7f, 0xff}));
  EXPECT_EQ(int16Serde.Serialize(0), Bytes({0x80, 0x00}));
  EXPECT_EQ(int16Serde.Serialize(std::numeric_limits<std::int16_t>::max()),
            Bytes({0xff, 0xff}));

  const servicelib::serde::Int32Serde int32Serde;
  EXPECT_EQ(int32Serde.Serialize(-1), Bytes({0x7f, 0xff, 0xff, 0xff}));
  EXPECT_EQ(int32Serde.Serialize(0), Bytes({0x80, 0x00, 0x00, 0x00}));

  const servicelib::serde::Int64Serde int64Serde;
  EXPECT_EQ(int64Serde.Serialize(-1),
            Bytes({0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}));
  EXPECT_EQ(int64Serde.Serialize(0),
            Bytes({0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}));

  const servicelib::serde::UInt32Serde uint32Serde;
  EXPECT_EQ(uint32Serde.Serialize(0x01020304u),
            Bytes({0x01, 0x02, 0x03, 0x04}));

  const servicelib::serde::RuneSerde runeSerde;
  EXPECT_EQ(runeSerde.Serialize(U'Ж'), Bytes({0x00, 0x00, 0x04, 0x16}));
}

UTEST(Serde, FloatingPointRoundTripPreservesBits) {
  const servicelib::serde::Float32Serde floatSerde;
  const float floatNan = std::bit_cast<float>(std::uint32_t{0x7fc01234u});
  const float decodedFloat =
      floatSerde.Deserialize(floatSerde.Serialize(floatNan));
  EXPECT_EQ(std::bit_cast<std::uint32_t>(decodedFloat), 0x7fc01234u);
  EXPECT_EQ(floatSerde.Serialize(1.0f), Bytes({0x3f, 0x80, 0x00, 0x00}));

  const servicelib::serde::Float64Serde doubleSerde;
  const double doubleNan =
      std::bit_cast<double>(std::uint64_t{0x7ff8000000001234ull});
  const double decodedDouble =
      doubleSerde.Deserialize(doubleSerde.Serialize(doubleNan));
  EXPECT_EQ(std::bit_cast<std::uint64_t>(decodedDouble), 0x7ff8000000001234ull);
  ExpectRoundTrip(doubleSerde, -0.0);
}

UTEST(Serde, StringBytesAndSerializeToUseLengthPrefixAndAppend) {
  const servicelib::serde::StringSerde stringSerde;
  const std::string stringValue{"A\0B", 3};
  EXPECT_EQ(
      stringSerde.Serialize(stringValue),
      Bytes({0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 'A', 0x00, 'B'}));
  ExpectRoundTrip(stringSerde, stringValue);

  const servicelib::serde::UInt8ArraySerde bytesSerde;
  const std::vector<std::uint8_t> bytesValue{0x00, 0x7f, 0xff};
  ExpectRoundTrip(bytesSerde, bytesValue);

  SerdeData output = Bytes({0xaa, 0xbb});
  stringSerde.SerializeTo(output, "x");
  EXPECT_EQ(output, Bytes({0xaa, 0xbb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                           0x01, 'x'}));
}

UTEST(Serde, FixedAndFramedArraysRoundTrip) {
  const servicelib::serde::Int16ArraySerde fixedSerde;
  const std::vector<std::int16_t> fixed{-32768, -1, 0, 32767};
  EXPECT_EQ(fixedSerde.Serialize(fixed),
            Bytes({0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00,
                   0x7f, 0xff, 0x80, 0x00, 0xff, 0xff}));
  ExpectRoundTrip(fixedSerde, fixed);

  const auto stringSerde = std::make_shared<servicelib::serde::StringSerde>();
  const servicelib::serde::ArraySerde<std::string> framedSerde{stringSerde};
  const std::vector<std::string> framed{"one", "", "three"};
  ExpectRoundTrip(framedSerde, framed);

  const auto defaultSerde =
      servicelib::serde::MakeDefaultSerde<std::vector<std::string>>();
  EXPECT_FALSE(defaultSerde->IsStub());
  ExpectRoundTrip(*defaultSerde, framed);
}

UTEST(Serde, MapRoundTripAndMalformedCountMismatch) {
  using Map = std::unordered_map<std::string, std::int32_t>;
  const auto serde = servicelib::serde::MakeDefaultSerde<Map>();
  const Map value{{"alpha", 1}, {"beta", -2}, {"", 0}};
  ExpectRoundTrip(*serde, value);

  const servicelib::serde::StringArraySerde keysSerde;
  const servicelib::serde::Int32ArraySerde valuesSerde;
  SerdeData malformed = Frame(keysSerde.Serialize({"a", "b"}));
  Append(malformed, Frame(valuesSerde.Serialize({1})));
  EXPECT_THROW(static_cast<void>(serde->Deserialize(malformed)), SerdeError);
}

UTEST(Serde, StreamWrappersAndKeyValueSerde) {
  auto valueSerde = std::make_shared<servicelib::serde::StringSerde>();
  auto streamSerde = servicelib::serde::MakeStreamSerde<std::string>(valueSerde);
  valueSerde.reset();
  EXPECT_FALSE(streamSerde->IsKeyValue());
  EXPECT_EQ(streamSerde->Deserialize(streamSerde->Serialize("payload")),
            "payload");
  EXPECT_NE(streamSerde->ValueSerializer(), nullptr);

  using KeyValue = std::pair<std::int32_t, std::string>;
  auto keyValueSerde =
      servicelib::serde::MakeStreamKeyValueSerde<std::int32_t, std::string,
                                             KeyValue>(
          std::make_shared<servicelib::serde::Int32Serde>(),
          std::make_shared<servicelib::serde::StringSerde>());
  const KeyValue value{-7, "seven"};
  EXPECT_TRUE(keyValueSerde->IsKeyValue());
  EXPECT_EQ(keyValueSerde->Deserialize(keyValueSerde->Serialize(value)), value);
  EXPECT_EQ(
      keyValueSerde->DeserializeKeyValue(keyValueSerde->SerializeKey(value),
                                         keyValueSerde->SerializeValue(value)),
      value);
}

UTEST(Serde, TypeErasureRejectsWrongObjectType) {
  const servicelib::serde::Int32Serde typedSerde;
  const servicelib::serde::Serializer& serializer = typedSerde;
  const auto encoded = serializer.SerializeObj(std::any{std::int32_t{42}});
  EXPECT_EQ(std::any_cast<std::int32_t>(serializer.DeserializeObj(encoded)),
            42);
  EXPECT_THROW(static_cast<void>(serializer.SerializeObj(std::any{"wrong"})),
               std::invalid_argument);

  servicelib::serde::StubSerde<std::string> stub;
  EXPECT_TRUE(stub.IsStub());
  EXPECT_THROW(static_cast<void>(stub.Serialize("value")), std::runtime_error);
  EXPECT_THROW(static_cast<void>(stub.Deserialize({})), std::runtime_error);
  EXPECT_THROW(
      static_cast<void>(servicelib::serde::MakeStreamSerde<std::string>(nullptr)),
      std::invalid_argument);
}

UTEST(Serde, LimitsAreEnforcedOnEncodeAndDecode) {
  const servicelib::serde::StringSerde stringSerde{
      SerdeLimits{.maxStringBytes = 3}};
  EXPECT_THROW(static_cast<void>(stringSerde.Serialize("four")), SerdeError);

  const auto encodedString = servicelib::serde::StringSerde{}.Serialize("four");
  EXPECT_THROW(static_cast<void>(stringSerde.Deserialize(encodedString)),
               SerdeError);

  const servicelib::serde::UInt8ArraySerde bytesSerde{SerdeLimits{.maxBytes = 2}};
  EXPECT_THROW(static_cast<void>(
                   bytesSerde.Serialize(std::vector<std::uint8_t>{1, 2, 3})),
               SerdeError);

  const servicelib::serde::Int32ArraySerde arraySerde{
      SerdeLimits{.maxContainerElements = 1}};
  EXPECT_THROW(
      static_cast<void>(arraySerde.Serialize(std::vector<std::int32_t>{1, 2})),
      SerdeError);

  const servicelib::serde::StringSerde totalLimited{
      SerdeLimits{.maxTotalBytes = 8}};
  EXPECT_THROW(static_cast<void>(totalLimited.Deserialize(encodedString)),
               SerdeError);
}

UTEST(Serde, TruncatedFramesReportTheReadOffset) {
  const servicelib::serde::StringSerde stringSerde;
  auto truncatedString =
      Bytes({0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 'a', 'b'});
  try {
    static_cast<void>(stringSerde.Deserialize(truncatedString));
    FAIL() << "expected SerdeError";
  } catch (const SerdeError& error) {
    EXPECT_EQ(error.offset(), 8);
  }

  const servicelib::serde::Int32ArraySerde fixedSerde;
  EXPECT_THROW(static_cast<void>(fixedSerde.Deserialize(
                   Bytes({0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x80,
                          0x00, 0x00, 0x01}))),
               SerdeError);

  const servicelib::serde::ArraySerde<std::string> framedSerde{
      std::make_shared<servicelib::serde::StringSerde>()};
  EXPECT_THROW(static_cast<void>(framedSerde.Deserialize(
                   Bytes({0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
                          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 'x'}))),
               SerdeError);
}

UTEST(Serde, CompositeConstructorsRejectNullDependencies) {
  EXPECT_THROW(
      static_cast<void>(servicelib::serde::ArraySerde<std::string>{nullptr}),
      std::invalid_argument);

  using Map = std::unordered_map<std::string, std::int32_t>;
  EXPECT_THROW(
      static_cast<void>(servicelib::serde::MapSerde<std::string, std::int32_t, Map>{
          nullptr,
          servicelib::serde::MakeDefaultSerde<std::vector<std::int32_t>>()}),
      std::invalid_argument);
}

}  // namespace

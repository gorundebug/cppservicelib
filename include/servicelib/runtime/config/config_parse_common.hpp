/*
 * config_parse_common.hpp
 * C++ streams API — shared building blocks for YAML config parsing:
 *  - PropertiesMap + FindProperty: the C++ analog of Go's
 *    `Properties map[string]interface{} \`yaml:",inline" mapstructure:",remain"\``
 *    field present on nearly every config type. mapstructure's ",remain"
 *    catches every YAML key that didn't match a named struct field; yaml.v3's
 *    ",inline" writes it back out flattened at the same level on the
 *    round-trip (app_to_yaml.go). There's no reflection in C++ to do this
 *    automatically, so ParseRemainingProperties() takes the set of keys
 *    each type's Parse() already consumed and captures everything else.
 *
 *    Each config type gets its own trailing `PropertiesMap properties;`
 *    data member (not a shared base class): several call sites construct
 *    these types with C++20 designated initializers
 *    (`PoolConfig{.name = ..., .executorsCount = ...}`), which requires
 *    them to stay pure aggregates with no base class — designated
 *    initializers interact with base-class subobjects in ways that are
 *    subtle and compiler-version-dependent. A plain trailing data member
 *    has no such restriction: omitted trailing aggregate members have
 *    always been legal to leave out of an initializer list.
 *  - ParseFunctionFields: the six functionXxx/publicFunction fields shared
 *    by most stream and endpoint config types.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <initializer_list>
#include <string>
#include <string_view>
#include <unordered_map>

#include <userver/formats/yaml/value.hpp>

namespace servicelib::config {

using PropertiesMap = std::unordered_map<std::string, userver::formats::yaml::Value>;

namespace detail {

inline const userver::formats::yaml::Value* FindProperty(const PropertiesMap& properties,
                                                          const std::string& name) {
  auto it = properties.find(name);
  return it != properties.end() ? &it->second : nullptr;
}

// Fills the six functionXxx/publicFunction fields shared by every stream
// and endpoint config type that names a user-provided handler function.
// T must have: functionName, functionPackage, publicFunction,
// functionDescription, functionInitializerGroup, functionModule.
template <typename T>
void ParseFunctionFields(const userver::formats::yaml::Value& value, T& result) {
  result.functionName = value["functionName"].template As<std::string>("");
  result.functionPackage = value["functionPackage"].template As<std::string>("");
  result.publicFunction = value["publicFunction"].template As<bool>(false);
  result.functionDescription = value["functionDescription"].template As<std::string>("");
  result.functionInitializerGroup = value["functionInitializerGroup"].template As<std::string>("");
  result.functionModule = value["functionModule"].template As<std::string>("");
}

// Captures every object key in `value` that isn't in `knownKeys` into
// `*properties`. Call this last in each type's Parse(), after every named
// field has been read — `knownKeys` must list exactly those fields' YAML
// keys, since C++ has no mapstructure-style reflection to infer them
// automatically. A no-op if `value` isn't an object (defensive only —
// Parse is never invoked on a non-object Value in practice).
inline void ParseRemainingProperties(const userver::formats::yaml::Value& value,
                                     std::initializer_list<std::string_view> knownKeys,
                                     PropertiesMap& properties) {
  if (!value.IsObject()) {
    return;
  }
  for (auto it = value.begin(); it != value.end(); ++it) {
    const std::string key = it.GetName();
    bool known = false;
    for (const auto& k : knownKeys) {
      if (k == key) {
        known = true;
        break;
      }
    }
    if (!known) {
      properties.emplace(key, *it);
    }
  }
}

}  // namespace detail

}  // namespace servicelib::config

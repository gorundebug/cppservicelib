/*
 * config_substitution.hpp
 * C++ streams API — override-merge for the config loader.
 *
 * $var/#env/#fallback/#file substitution is handled natively by userver's
 * yaml_config::YamlConfig (see loader.hpp) — there is no hand-rolled
 * placeholder or environment-variable substitution here. This file is only
 * the one piece userver doesn't provide out of the box: merging a base
 * config tree with an optional override tree, mirroring Go's
 * viper.MergeConfig (mapstructure merge semantics).
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <userver/formats/yaml/value.hpp>
#include <userver/formats/yaml/value_builder.hpp>

namespace servicelib::config {

// Recursively merges `overrides` on top of `base`: object keys present in
// both are merged key-by-key (recursively, if both sides are objects at
// that key); any key present only in `overrides`, or where either side
// isn't an object, has `overrides`'s value win outright (arrays are
// replaced wholesale, never concatenated or merged element-wise). Mirrors
// Go's viper.MergeConfig (mapstructure merge semantics).
inline userver::formats::yaml::Value DeepMerge(
    const userver::formats::yaml::Value& base,
    const userver::formats::yaml::Value& overrides) {
  if (!base.IsObject() || !overrides.IsObject()) {
    return overrides;
  }

  userver::formats::yaml::ValueBuilder builder(base);
  for (auto it = overrides.begin(); it != overrides.end(); ++it) {
    const auto& key = it.GetName();
    const auto baseChild = base[key];
    builder[key] = baseChild.IsMissing() ? *it : DeepMerge(baseChild, *it);
  }
  return builder.ExtractValue();
}

}  // namespace servicelib::config

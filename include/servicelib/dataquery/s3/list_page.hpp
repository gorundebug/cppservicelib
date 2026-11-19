#pragma once

#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <pugixml.hpp>

namespace servicelib::dataquery::s3::detail {

// ListObjects v1 markers are object keys, not opaque V2 continuation tokens.
// Parse only pagination here; the native XML page remains available to the
// application mapper, including fields unknown to this SDK version.
inline std::optional<std::string> nextListMarker(std::string_view xml,
                                                std::string_view previous) {
  pugi::xml_document document;
  if (!document.load_buffer(xml.data(), xml.size()))
    throw std::runtime_error("invalid S3 listing XML");
  const auto root = document.child("ListBucketResult");
  if (!root) throw std::runtime_error("missing S3 ListBucketResult");
  const std::string_view truncated = root.child("IsTruncated").child_value();
  if (truncated == "false") return std::nullopt;
  if (truncated != "true") throw std::runtime_error("invalid S3 listing truncation flag");
  std::string next = root.child("NextMarker").child_value();
  if (next.empty()) {
    for (auto object : root.children("Contents")) next = object.child("Key").child_value();
  }
  if (next.empty() || next <= previous)
    throw std::runtime_error("S3 listing did not advance its marker");
  return next;
}
}  // namespace servicelib::dataquery::s3::detail

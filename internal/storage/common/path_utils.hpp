#pragma once

#include <filesystem>
#include <stdexcept>
#include <string>

namespace payload::storage::common {

inline void ValidatePayloadId(const std::string& payload_id) {
  if (payload_id.empty()) {
    throw std::invalid_argument("payload id must not be empty");
  }
  for (char c : payload_id) {
    if (c == '/' || c == '\\' || c == '\0') {
      throw std::invalid_argument("payload id contains invalid character");
    }
  }
  if (payload_id == "." || payload_id == "..") {
    throw std::invalid_argument("payload id must not be a relative path component");
  }
}

inline std::filesystem::path PayloadPath(const std::filesystem::path& root, const std::string& payload_id) {
  ValidatePayloadId(payload_id);
  return root / (payload_id + ".bin");
}

} // namespace payload::storage::common
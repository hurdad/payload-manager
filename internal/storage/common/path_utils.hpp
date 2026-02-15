#pragma once

#include <filesystem>
#include <string>

namespace payload::storage::common {

inline std::filesystem::path PayloadPath(const std::filesystem::path& root, const std::string& payload_id) {
  return root / (payload_id + ".bin");
}

} // namespace payload::storage::common

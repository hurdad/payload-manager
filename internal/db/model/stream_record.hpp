#pragma once

#include <cstdint>
#include <string>

namespace payload::db::model {

struct StreamRecord {
  uint64_t    stream_id = 0;
  std::string stream_namespace;
  std::string name;
  uint64_t    retention_max_entries = 0;
  uint64_t    retention_max_age_sec = 0;
  uint64_t    created_at_ms         = 0;
};

} // namespace payload::db::model

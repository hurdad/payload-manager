#pragma once

#include <cstdint>
#include <string>

namespace payload::db::model {

struct StreamEntryRecord {
  uint64_t stream_id = 0;
  uint64_t offset = 0;
  std::string payload_uuid;
  uint64_t event_time_ms = 0;
  uint64_t append_time_ms = 0;
  uint64_t duration_ns = 0;
  std::string tags;
};

}

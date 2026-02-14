#pragma once

#include <cstdint>
#include <string>

namespace payload::db::model {

struct StreamConsumerOffsetRecord {
  uint64_t    stream_id = 0;
  std::string consumer_group;
  uint64_t    offset        = 0;
  uint64_t    updated_at_ms = 0;
};

} // namespace payload::db::model

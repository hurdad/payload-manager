#pragma once

#include <string>
#include <cstdint>

#include "payload/manager/core/v1/types.pb.h"
#include "payload/manager/v1.hpp"

namespace payload::db::model {

/*
  Persistent payload row.

  IMPORTANT:
  - This is the authoritative state machine record.
  - Version is used for optimistic concurrency / lease fencing.
  - ID should eventually become 16-byte binary UUID.
*/

struct PayloadRecord {
  std::string id;  // UUID (temporary string form; future: std::array<uint8_t,16>)

  payload::manager::v1::Tier tier =
      payload::manager::v1::TIER_UNSPECIFIED;

  payload::manager::v1::PayloadState state =
      payload::manager::v1::PAYLOAD_STATE_UNSPECIFIED;

  uint64_t size_bytes = 0;

  // Monotonic placement/state version
  uint64_t version = 0;

  // Optional expiration (0 = none)
  uint64_t expires_at_ms = 0;
};

}

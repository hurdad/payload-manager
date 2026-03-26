#pragma once

#include <cstdint>
#include <string>

#include "internal/util/uuid.hpp"
#include "payload/manager/core/v1/types.pb.h"
#include "payload/manager/v1.hpp"

namespace payload::db::model {

/*
  Persistent payload row.

  IMPORTANT:
  - This is the authoritative state machine record.
  - Version is used for optimistic concurrency / lease fencing.
*/

struct PayloadRecord {
  payload::util::UUID id{};

  payload::manager::v1::Tier tier = payload::manager::v1::TIER_UNSPECIFIED;

  payload::manager::v1::PayloadState state = payload::manager::v1::PAYLOAD_STATE_UNSPECIFIED;

  uint64_t size_bytes = 0;

  // Monotonic placement/state version
  uint64_t version = 0;

  // Wall-clock creation time (ms since epoch); 0 if not recorded (legacy rows).
  uint64_t created_at_ms = 0;

  // Optional expiration (0 = none)
  uint64_t expires_at_ms = 0;

  // If true, payload is never automatically evicted or expired.
  bool no_evict = false;

  // Advisory eviction priority (EvictionPriority enum; 0 = UNSPECIFIED = NORMAL).
  int eviction_priority = 0;

  // Preferred tier to spill into when evicted (Tier enum; 0 = UNSPECIFIED → TIER_DISK).
  int spill_target = 0;
};

} // namespace payload::db::model

#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "internal/model/location.hpp"
#include "internal/model/state_machine.hpp"
#include "internal/model/tier.hpp"

namespace payload::manager::internal::model {

using Uuid = std::array<std::byte, 16>;

struct Payload {
  Uuid uuid{};
  std::string name;
  std::string group_id;

  std::uint64_t size_bytes = 0;
  std::uint64_t compressed_size_bytes = 0;

  Tier current_tier = Tier::kUnspecified;
  std::vector<Tier> available_tiers;
  Location location;

  PayloadState state = PayloadState::kUnspecified;

  std::chrono::system_clock::time_point created_at{};
  std::chrono::system_clock::time_point last_accessed_at{};
  std::chrono::system_clock::time_point last_spilled_at{};

  std::uint64_t access_count = 0;
  std::string checksum;
  bool require_durability = false;
  bool pinned = false;

  bool spill_pending = false;
  std::uint32_t spill_attempts = 0;
  std::string last_spill_error;

  std::map<std::string, std::string> attributes;
};

}  // namespace payload::manager::internal::model

#pragma once

#include <cstdint>
#include <string>
#include <variant>

#include "internal/model/tier.hpp"

namespace payload::manager::internal::model {

struct GpuLocation {
  std::uint32_t device_id = 0;
  std::string ipc_handle;
  std::uint64_t length_bytes = 0;
};

struct RamLocation {
  std::string shm_name;
  std::uint32_t slab_id = 0;
  std::uint64_t block_index = 0;
  std::uint64_t length_bytes = 0;
};

struct DiskLocation {
  std::string path;
  std::uint64_t offset_bytes = 0;
  std::uint64_t length_bytes = 0;
};

using Location = std::variant<std::monostate, GpuLocation, RamLocation, DiskLocation>;

inline Tier TierFromLocation(const Location& location) {
  if (std::holds_alternative<GpuLocation>(location)) {
    return Tier::kGpu;
  }
  if (std::holds_alternative<RamLocation>(location)) {
    return Tier::kRam;
  }
  if (std::holds_alternative<DiskLocation>(location)) {
    return Tier::kDisk;
  }
  return Tier::kUnspecified;
}

}  // namespace payload::manager::internal::model

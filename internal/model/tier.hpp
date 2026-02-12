#pragma once

#include <cstdint>
#include <string_view>

namespace payload::manager::internal::model {

enum class Tier : std::uint8_t {
  kUnspecified = 0,
  kGpu = 1,
  kRam = 2,
  kDisk = 3,
  kObject = 4,
};

constexpr std::string_view ToString(Tier tier) {
  switch (tier) {
    case Tier::kGpu:
      return "gpu";
    case Tier::kRam:
      return "ram";
    case Tier::kDisk:
      return "disk";
    case Tier::kObject:
      return "object";
    case Tier::kUnspecified:
    default:
      return "unspecified";
  }
}

}  // namespace payload::manager::internal::model

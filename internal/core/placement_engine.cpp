#include "placement_engine.hpp"

#include "payload/manager/v1.hpp"

namespace payload::core {

using namespace payload::manager::v1;

bool PlacementEngine::IsHigherTier(Tier a, Tier b) {
  return static_cast<int>(a) < static_cast<int>(b);
}

Tier PlacementEngine::NextLowerTier(Tier t) {
  switch (t) {
    case TIER_GPU:
      return TIER_RAM;
    case TIER_RAM:
      return TIER_DISK;
    case TIER_DISK:
      return TIER_OBJECT;
    default:
      return TIER_OBJECT;
  }
}

} // namespace payload::core

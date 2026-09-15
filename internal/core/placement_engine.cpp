#include "placement_engine.hpp"

#include "payload/manager/v1.hpp"

namespace payload::core {

using namespace payload::manager::v1;

namespace {

/*
  Speed rank, fastest first. Lower rank == higher tier.

  This is deliberately not derived from the Tier enum's numeric value. Enum
  values are assigned in the order tiers were added and can only ever be
  appended, so they do not track speed: TIER_VOID (5) and TIER_RAM_RING (6)
  both sort after TIER_OBJECT (4) under a raw ordinal compare, which is
  backwards. Rank is what the ordering actually means, so it is stated here.

  TIER_RAM_RING is not part of the demotion chain — ring slots rotate rather
  than spill — but it ranks just above TIER_RAM because a ring slot is the same
  tmpfs with the per-capture mmap already paid for. That keeps a
  min_residency_tier of TIER_RAM satisfied by a ring-resident payload instead of
  provoking a pointless promotion.

  TIER_VOID ranks last: spilling into it discards the payload, so it is below
  every tier that actually retains bytes.

  TIER_DISK_COLD sits between TIER_DISK_HOT and TIER_OBJECT: slower local media,
  still local. Note its enum value (7) is the highest of all, which is exactly
  why this table exists rather than an ordinal compare.
*/
int TierRank(Tier t) {
  switch (t) {
    case TIER_GPU:
      return 0;
    case TIER_RAM_RING:
      return 1;
    case TIER_RAM:
      return 2;
    case TIER_DISK_HOT:
      return 3;
    case TIER_DISK_COLD:
      return 4;
    case TIER_OBJECT:
      return 5;
    case TIER_VOID:
      return 6;
    default:
      // TIER_UNSPECIFIED and any value from a newer peer we do not know about.
      return 7;
  }
}

} // namespace

bool PlacementEngine::IsHigherTier(Tier a, Tier b) {
  return TierRank(a) < TierRank(b);
}

Tier PlacementEngine::NextLowerTier(Tier t) {
  switch (t) {
    case TIER_GPU:
      return TIER_RAM;
    case TIER_RAM:
      return TIER_DISK_HOT;
    case TIER_DISK_HOT:
      return TIER_DISK_COLD;
    case TIER_DISK_COLD:
      return TIER_OBJECT;
    default:
      return TIER_OBJECT;
  }
}

} // namespace payload::core

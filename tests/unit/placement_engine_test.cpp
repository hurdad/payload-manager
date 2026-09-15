#include "internal/core/placement_engine.hpp"

#include <gtest/gtest.h>

#include <iterator>

using payload::core::PlacementEngine;
using namespace payload::manager::v1;

// ---------------------------------------------------------------------------
// IsHigherTier
// ---------------------------------------------------------------------------

TEST(PlacementEngine, GpuIsHigherThanRam) {
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_GPU, TIER_RAM));
}

TEST(PlacementEngine, RamIsHigherThanDisk) {
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_RAM, TIER_DISK_HOT));
}

TEST(PlacementEngine, DiskIsHigherThanObject) {
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_DISK_HOT, TIER_OBJECT));
}

TEST(PlacementEngine, GpuIsHigherThanDisk) {
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_GPU, TIER_DISK_HOT));
}

TEST(PlacementEngine, GpuIsHigherThanObject) {
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_GPU, TIER_OBJECT));
}

TEST(PlacementEngine, RamIsHigherThanObject) {
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_RAM, TIER_OBJECT));
}

TEST(PlacementEngine, SameTierIsNotHigher) {
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_GPU, TIER_GPU));
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_RAM, TIER_RAM));
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_DISK_HOT, TIER_DISK_HOT));
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_OBJECT, TIER_OBJECT));
}

TEST(PlacementEngine, LowerTierIsNotHigher) {
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_RAM, TIER_GPU));
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_DISK_HOT, TIER_RAM));
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_OBJECT, TIER_DISK_HOT));
}

// ---------------------------------------------------------------------------
// NextLowerTier
// ---------------------------------------------------------------------------

TEST(PlacementEngine, GpuNextLowerIsRam) {
  EXPECT_EQ(PlacementEngine::NextLowerTier(TIER_GPU), TIER_RAM);
}

TEST(PlacementEngine, RamNextLowerIsDisk) {
  EXPECT_EQ(PlacementEngine::NextLowerTier(TIER_RAM), TIER_DISK_HOT);
}

TEST(PlacementEngine, DiskNextLowerIsDiskCold) {
  // NextLowerTier describes the full chain regardless of what is configured.
  // Skipping an unconfigured cold tier is PayloadManager::GetDiskHotSpillTarget's
  // job, not this function's.
  EXPECT_EQ(PlacementEngine::NextLowerTier(TIER_DISK_HOT), TIER_DISK_COLD);
}

TEST(PlacementEngine, DiskColdNextLowerIsObject) {
  EXPECT_EQ(PlacementEngine::NextLowerTier(TIER_DISK_COLD), TIER_OBJECT);
}

TEST(PlacementEngine, ObjectNextLowerIsObject) {
  // Object is the lowest tier; it maps to itself (no lower tier exists).
  EXPECT_EQ(PlacementEngine::NextLowerTier(TIER_OBJECT), TIER_OBJECT);
}

// TIER_UNSPECIFIED is the protobuf default (value 0).  It falls through to
// the default branch in NextLowerTier and must return TIER_OBJECT rather than
// crashing or returning an invalid value.
TEST(PlacementEngine, UnspecifiedNextLowerIsSafelyObject) {
  EXPECT_EQ(PlacementEngine::NextLowerTier(TIER_UNSPECIFIED), TIER_OBJECT)
      << "TIER_UNSPECIFIED must be handled by the default branch and return TIER_OBJECT";
}

// ---------------------------------------------------------------------------
// Rank ordering for tiers outside the demotion chain
//
// IsHigherTier used to compare raw Tier enum values. Enum values can only be
// appended, so they track the order tiers were added rather than their speed:
// TIER_VOID (5) and TIER_RAM_RING (6) both sort after TIER_OBJECT (4). These
// cases pin the intended ordering so a future tier cannot silently inherit the
// wrong rank by virtue of its enum value.
// ---------------------------------------------------------------------------

TEST(PlacementEngine, RingIsHigherThanRam) {
  // A ring slot is the same tmpfs as the RAM tier with the per-capture mmap
  // already paid for, so it is not slower than RAM. Under the old ordinal
  // compare TIER_RAM (2) ranked above TIER_RAM_RING (6), which made a
  // min_residency_tier of TIER_RAM try to promote a ring-resident payload.
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_RAM_RING, TIER_RAM));
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_RAM, TIER_RAM_RING));
}

TEST(PlacementEngine, RingIsHigherThanDurableTiers) {
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_RAM_RING, TIER_DISK_HOT));
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_RAM_RING, TIER_OBJECT));
}

TEST(PlacementEngine, GpuIsHigherThanRing) {
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_GPU, TIER_RAM_RING));
}

TEST(PlacementEngine, DiskColdSitsBetweenDiskAndObject) {
  // TIER_DISK_COLD has the highest enum value (7) of any tier, so an ordinal
  // compare would rank it below TIER_OBJECT (4). This is the case the rank
  // table exists for.
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_DISK_HOT, TIER_DISK_COLD));
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_DISK_COLD, TIER_OBJECT));
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_DISK_COLD, TIER_DISK_HOT));
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_OBJECT, TIER_DISK_COLD));
}

TEST(PlacementEngine, DiskColdIsLowerThanVolatileTiers) {
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_GPU, TIER_DISK_COLD));
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_RAM, TIER_DISK_COLD));
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_RAM_RING, TIER_DISK_COLD));
}

TEST(PlacementEngine, VoidIsLowerThanEveryRetainingTier) {
  // Spilling into TIER_VOID discards the payload, so it must rank below every
  // tier that actually keeps the bytes — otherwise a min_residency_tier of
  // TIER_OBJECT would permit a spill that deletes the payload.
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_GPU, TIER_VOID));
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_RAM_RING, TIER_VOID));
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_RAM, TIER_VOID));
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_DISK_HOT, TIER_VOID));
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_DISK_COLD, TIER_VOID));
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_OBJECT, TIER_VOID));
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_VOID, TIER_OBJECT));
}

TEST(PlacementEngine, UnspecifiedIsLowestRank) {
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_OBJECT, TIER_UNSPECIFIED));
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_VOID, TIER_UNSPECIFIED));
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_UNSPECIFIED, TIER_VOID));
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_UNSPECIFIED, TIER_UNSPECIFIED));
}

TEST(PlacementEngine, OrderingIsStrictAndTotalOverTheFullChain) {
  // Every tier compared against every other: exactly one direction may hold,
  // and never both. This is what makes IsHigherTier safe to use as the
  // min_residency_tier predicate.
  const Tier kByRank[] = {TIER_GPU, TIER_RAM_RING, TIER_RAM, TIER_DISK_HOT, TIER_DISK_COLD, TIER_OBJECT, TIER_VOID, TIER_UNSPECIFIED};

  for (const Tier a : kByRank) {
    EXPECT_FALSE(PlacementEngine::IsHigherTier(a, a)) << "tier " << static_cast<int>(a) << " ranked above itself";
  }

  for (size_t i = 0; i < std::size(kByRank); ++i) {
    for (size_t j = i + 1; j < std::size(kByRank); ++j) {
      EXPECT_TRUE(PlacementEngine::IsHigherTier(kByRank[i], kByRank[j]))
          << static_cast<int>(kByRank[i]) << " should rank above " << static_cast<int>(kByRank[j]);
      EXPECT_FALSE(PlacementEngine::IsHigherTier(kByRank[j], kByRank[i]))
          << static_cast<int>(kByRank[j]) << " should not rank above " << static_cast<int>(kByRank[i]);
    }
  }
}

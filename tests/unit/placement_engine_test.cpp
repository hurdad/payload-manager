#include "internal/core/placement_engine.hpp"

#include <gtest/gtest.h>

using payload::core::PlacementEngine;
using namespace payload::manager::v1;

// ---------------------------------------------------------------------------
// IsHigherTier
// ---------------------------------------------------------------------------

TEST(PlacementEngine, GpuIsHigherThanRam) {
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_GPU, TIER_RAM));
}

TEST(PlacementEngine, RamIsHigherThanDisk) {
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_RAM, TIER_DISK));
}

TEST(PlacementEngine, DiskIsHigherThanObject) {
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_DISK, TIER_OBJECT));
}

TEST(PlacementEngine, GpuIsHigherThanDisk) {
  EXPECT_TRUE(PlacementEngine::IsHigherTier(TIER_GPU, TIER_DISK));
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
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_DISK, TIER_DISK));
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_OBJECT, TIER_OBJECT));
}

TEST(PlacementEngine, LowerTierIsNotHigher) {
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_RAM, TIER_GPU));
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_DISK, TIER_RAM));
  EXPECT_FALSE(PlacementEngine::IsHigherTier(TIER_OBJECT, TIER_DISK));
}

// ---------------------------------------------------------------------------
// NextLowerTier
// ---------------------------------------------------------------------------

TEST(PlacementEngine, GpuNextLowerIsRam) {
  EXPECT_EQ(PlacementEngine::NextLowerTier(TIER_GPU), TIER_RAM);
}

TEST(PlacementEngine, RamNextLowerIsDisk) {
  EXPECT_EQ(PlacementEngine::NextLowerTier(TIER_RAM), TIER_DISK);
}

TEST(PlacementEngine, DiskNextLowerIsObject) {
  EXPECT_EQ(PlacementEngine::NextLowerTier(TIER_DISK), TIER_OBJECT);
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

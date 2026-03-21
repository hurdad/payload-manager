#include "internal/core/placement_engine.hpp"

#include <cassert>
#include <iostream>

namespace {

using payload::core::PlacementEngine;
using namespace payload::manager::v1;

// ---------------------------------------------------------------------------
// IsHigherTier
// ---------------------------------------------------------------------------

void TestGpuIsHigherThanRam() {
  assert(PlacementEngine::IsHigherTier(TIER_GPU, TIER_RAM));
}

void TestRamIsHigherThanDisk() {
  assert(PlacementEngine::IsHigherTier(TIER_RAM, TIER_DISK));
}

void TestDiskIsHigherThanObject() {
  assert(PlacementEngine::IsHigherTier(TIER_DISK, TIER_OBJECT));
}

void TestGpuIsHigherThanDisk() {
  assert(PlacementEngine::IsHigherTier(TIER_GPU, TIER_DISK));
}

void TestGpuIsHigherThanObject() {
  assert(PlacementEngine::IsHigherTier(TIER_GPU, TIER_OBJECT));
}

void TestRamIsHigherThanObject() {
  assert(PlacementEngine::IsHigherTier(TIER_RAM, TIER_OBJECT));
}

void TestSameTierIsNotHigher() {
  assert(!PlacementEngine::IsHigherTier(TIER_GPU, TIER_GPU));
  assert(!PlacementEngine::IsHigherTier(TIER_RAM, TIER_RAM));
  assert(!PlacementEngine::IsHigherTier(TIER_DISK, TIER_DISK));
  assert(!PlacementEngine::IsHigherTier(TIER_OBJECT, TIER_OBJECT));
}

void TestLowerTierIsNotHigher() {
  assert(!PlacementEngine::IsHigherTier(TIER_RAM, TIER_GPU));
  assert(!PlacementEngine::IsHigherTier(TIER_DISK, TIER_RAM));
  assert(!PlacementEngine::IsHigherTier(TIER_OBJECT, TIER_DISK));
}

// ---------------------------------------------------------------------------
// NextLowerTier
// ---------------------------------------------------------------------------

void TestGpuNextLowerIsRam() {
  assert(PlacementEngine::NextLowerTier(TIER_GPU) == TIER_RAM);
}

void TestRamNextLowerIsDisk() {
  assert(PlacementEngine::NextLowerTier(TIER_RAM) == TIER_DISK);
}

void TestDiskNextLowerIsObject() {
  assert(PlacementEngine::NextLowerTier(TIER_DISK) == TIER_OBJECT);
}

void TestObjectNextLowerIsObject() {
  // Object is the lowest tier; it maps to itself (no lower tier exists).
  assert(PlacementEngine::NextLowerTier(TIER_OBJECT) == TIER_OBJECT);
}

} // namespace

int main() {
  TestGpuIsHigherThanRam();
  TestRamIsHigherThanDisk();
  TestDiskIsHigherThanObject();
  TestGpuIsHigherThanDisk();
  TestGpuIsHigherThanObject();
  TestRamIsHigherThanObject();
  TestSameTierIsNotHigher();
  TestLowerTierIsNotHigher();

  TestGpuNextLowerIsRam();
  TestRamNextLowerIsDisk();
  TestDiskNextLowerIsObject();
  TestObjectNextLowerIsObject();

  std::cout << "payload_manager_unit_placement_engine: pass\n";
  return 0;
}

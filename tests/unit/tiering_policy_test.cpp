#include "internal/tiering/tiering_policy.hpp"

#include <gtest/gtest.h>

#include <memory>

#include "internal/metadata/metadata_cache.hpp"
#include "internal/tiering/pressure_state.hpp"

namespace {

using payload::manager::v1::PayloadID;
using payload::manager::v1::PayloadMetadata;
using payload::metadata::MetadataCache;
using payload::tiering::PressureState;
using payload::tiering::TieringPolicy;

PayloadID MakePayloadID(const std::string& value) {
  PayloadID id;
  id.set_value(value);
  return id;
}

void PutMetadata(MetadataCache& cache, const std::string& id_value) {
  auto            id = MakePayloadID(id_value);
  PayloadMetadata metadata;
  *metadata.mutable_id() = id;
  cache.Put(id, metadata);
}

} // namespace

TEST(TieringPolicy, NoPressureReturnsNoVictim) {
  auto cache  = std::make_shared<MetadataCache>();
  auto policy = TieringPolicy(cache);

  PressureState state;
  state.ram_limit = 100;
  state.gpu_limit = 100;
  state.ram_bytes.store(100);
  state.gpu_bytes.store(100);

  EXPECT_FALSE(policy.ChooseRamEviction(state).has_value());
  EXPECT_FALSE(policy.ChooseGpuEviction(state).has_value());
}

TEST(TieringPolicy, PressureWithEmptyCacheReturnsNoVictim) {
  auto cache  = std::make_shared<MetadataCache>();
  auto policy = TieringPolicy(cache);

  PressureState state;
  state.ram_limit = 0;
  state.gpu_limit = 0;
  state.ram_bytes.store(1);
  state.gpu_bytes.store(1);

  EXPECT_FALSE(policy.ChooseRamEviction(state).has_value());
  EXPECT_FALSE(policy.ChooseGpuEviction(state).has_value());
}

TEST(TieringPolicy, PressureSelectsLeastRecentlyUsedVictim) {
  auto cache  = std::make_shared<MetadataCache>();
  auto policy = TieringPolicy(cache);

  PutMetadata(*cache, "payload-a");
  PutMetadata(*cache, "payload-b");
  PutMetadata(*cache, "payload-c");

  // Mark payload-a and payload-c as recently used.
  (void)cache->Get(MakePayloadID("payload-a"));
  (void)cache->Get(MakePayloadID("payload-c"));

  PressureState state;
  state.ram_limit = 0;
  state.gpu_limit = 0;
  state.ram_bytes.store(1);
  state.gpu_bytes.store(1);

  const auto ram_victim = policy.ChooseRamEviction(state);
  const auto gpu_victim = policy.ChooseGpuEviction(state);

  ASSERT_TRUE(ram_victim.has_value());
  ASSERT_TRUE(gpu_victim.has_value());
  EXPECT_EQ(ram_victim->value(), "payload-b");
  EXPECT_EQ(gpu_victim->value(), "payload-b");
}

// Fix 2: RAM predicate only admits RAM-tier payloads; GPU predicate admits only
// GPU-tier payloads.
TEST(TieringPolicy, TierSpecificPredicatesSelectCorrectVictim) {
  auto cache = std::make_shared<MetadataCache>();

  PutMetadata(*cache, "ram-payload");
  PutMetadata(*cache, "gpu-payload");

  // RAM predicate: only "ram-payload" is eligible.
  auto is_ram = [](const PayloadID& id) { return id.value() == "ram-payload"; };
  // GPU predicate: only "gpu-payload" is eligible.
  auto is_gpu = [](const PayloadID& id) { return id.value() == "gpu-payload"; };

  auto policy = TieringPolicy(cache, is_ram, is_gpu);

  PressureState state;
  state.ram_limit = 0;
  state.gpu_limit = 0;
  state.ram_bytes.store(1);
  state.gpu_bytes.store(1);

  const auto ram_victim = policy.ChooseRamEviction(state);
  const auto gpu_victim = policy.ChooseGpuEviction(state);

  ASSERT_TRUE(ram_victim.has_value());
  ASSERT_TRUE(gpu_victim.has_value());
  EXPECT_EQ(ram_victim->value(), "ram-payload");
  EXPECT_EQ(gpu_victim->value(), "gpu-payload");
}

// Fix 2: When the RAM predicate rejects all payloads, no RAM victim is chosen
// even under pressure.
TEST(TieringPolicy, RamPredicateRejectingAllYieldsNoVictim) {
  auto cache = std::make_shared<MetadataCache>();
  PutMetadata(*cache, "payload-a");
  PutMetadata(*cache, "payload-b");

  // Both predicates always return false.
  auto is_ram = [](const PayloadID&) { return false; };
  auto is_gpu = [](const PayloadID&) { return false; };

  auto policy = TieringPolicy(cache, is_ram, is_gpu);

  PressureState state;
  state.ram_limit = 0;
  state.gpu_limit = 0;
  state.ram_bytes.store(1);
  state.gpu_bytes.store(1);

  EXPECT_FALSE(policy.ChooseRamEviction(state).has_value());
  EXPECT_FALSE(policy.ChooseGpuEviction(state).has_value());
}

#include "internal/metadata/metadata_cache.hpp"
#include "internal/tiering/pressure_state.hpp"
#include "internal/tiering/tiering_policy.hpp"

#include <cassert>
#include <iostream>
#include <memory>

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

void TestNoPressureReturnsNoVictim() {
  auto cache  = std::make_shared<MetadataCache>();
  auto policy = TieringPolicy(cache);

  PressureState state;
  state.ram_limit = 100;
  state.gpu_limit = 100;
  state.ram_bytes.store(100);
  state.gpu_bytes.store(100);

  assert(!policy.ChooseRamEviction(state).has_value());
  assert(!policy.ChooseGpuEviction(state).has_value());
}

void TestPressureWithEmptyCacheReturnsNoVictim() {
  auto cache  = std::make_shared<MetadataCache>();
  auto policy = TieringPolicy(cache);

  PressureState state;
  state.ram_limit = 0;
  state.gpu_limit = 0;
  state.ram_bytes.store(1);
  state.gpu_bytes.store(1);

  assert(!policy.ChooseRamEviction(state).has_value());
  assert(!policy.ChooseGpuEviction(state).has_value());
}

void TestPressureSelectsLeastRecentlyUsedVictim() {
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

  assert(ram_victim.has_value());
  assert(gpu_victim.has_value());
  assert(ram_victim->value() == "payload-b");
  assert(gpu_victim->value() == "payload-b");
}

} // namespace

int main() {
  TestNoPressureReturnsNoVictim();
  TestPressureWithEmptyCacheReturnsNoVictim();
  TestPressureSelectsLeastRecentlyUsedVictim();

  std::cout << "payload_manager_unit_tiering_policy: pass\n";
  return 0;
}

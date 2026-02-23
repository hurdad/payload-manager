#include "tiering_policy.hpp"

#include "internal/metadata/metadata_cache.hpp"
#include "payload/manager/v1.hpp"

namespace payload::tiering {

using namespace payload::manager::v1;

namespace {

std::optional<PayloadID> ChooseVictimFromMetadataCache(const std::shared_ptr<payload::metadata::MetadataCache>& cache) {
  if (!cache) {
    return std::nullopt;
  }

  return cache->GetLeastRecentlyUsedId();
}

} // namespace

TieringPolicy::TieringPolicy(std::shared_ptr<payload::metadata::MetadataCache> cache) : cache_(std::move(cache)) {
}

std::optional<PayloadID> TieringPolicy::ChooseRamEviction(const PressureState& state) {
  if (!state.RamPressure()) return std::nullopt;

  return ChooseVictimFromMetadataCache(cache_);
}

std::optional<PayloadID> TieringPolicy::ChooseGpuEviction(const PressureState& state) {
  if (!state.GpuPressure()) return std::nullopt;

  return ChooseVictimFromMetadataCache(cache_);
}

} // namespace payload::tiering

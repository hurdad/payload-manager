#include "tiering_policy.hpp"

#include "internal/metadata/metadata_cache.hpp"
#include "payload/manager/v1.hpp"

namespace payload::tiering {

using namespace payload::manager::v1;

TieringPolicy::TieringPolicy(std::shared_ptr<payload::metadata::MetadataCache>          cache,
                             std::function<bool(const payload::manager::v1::PayloadID&)> is_evictable)
    : cache_(std::move(cache)), is_evictable_(std::move(is_evictable)) {
}

namespace {

std::optional<PayloadID> ChooseVictimFromMetadataCache(const std::shared_ptr<payload::metadata::MetadataCache>&        cache,
                                                       const std::function<bool(const payload::manager::v1::PayloadID&)>& is_evictable) {
  if (!cache) {
    return std::nullopt;
  }

  if (is_evictable) {
    return cache->GetLeastRecentlyUsedId(is_evictable);
  }

  return cache->GetLeastRecentlyUsedId();
}

} // namespace

std::optional<PayloadID> TieringPolicy::ChooseRamEviction(const PressureState& state) {
  if (!state.RamPressure()) return std::nullopt;

  return ChooseVictimFromMetadataCache(cache_, is_evictable_);
}

std::optional<PayloadID> TieringPolicy::ChooseGpuEviction(const PressureState& state) {
  if (!state.GpuPressure()) return std::nullopt;

  return ChooseVictimFromMetadataCache(cache_, is_evictable_);
}

} // namespace payload::tiering

#include "tiering_policy.hpp"

#include "internal/metadata/metadata_cache.hpp"
#include "payload/manager/v1.hpp"

namespace payload::tiering {

using namespace payload::manager::v1;

TieringPolicy::TieringPolicy(std::shared_ptr<payload::metadata::MetadataCache>           cache,
                             std::function<bool(const payload::manager::v1::PayloadID&)> is_ram_evictable,
                             std::function<bool(const payload::manager::v1::PayloadID&)> is_gpu_evictable)
    : cache_(std::move(cache)), is_ram_evictable_(std::move(is_ram_evictable)), is_gpu_evictable_(std::move(is_gpu_evictable)) {
}

namespace {

std::optional<PayloadID> ChooseVictimFromMetadataCache(const std::shared_ptr<payload::metadata::MetadataCache>&           cache,
                                                       const std::function<bool(const payload::manager::v1::PayloadID&)>& predicate) {
  if (!cache) {
    return std::nullopt;
  }

  if (predicate) {
    return cache->GetLeastRecentlyUsedId(predicate);
  }

  return cache->GetLeastRecentlyUsedId();
}

} // namespace

std::optional<PayloadID> TieringPolicy::ChooseRamEviction(const PressureState& state) {
  if (!state.RamPressure()) return std::nullopt;

  return ChooseVictimFromMetadataCache(cache_, is_ram_evictable_);
}

std::optional<PayloadID> TieringPolicy::ChooseGpuEviction(const PressureState& state) {
  if (!state.GpuPressure()) return std::nullopt;

  return ChooseVictimFromMetadataCache(cache_, is_gpu_evictable_);
}

} // namespace payload::tiering

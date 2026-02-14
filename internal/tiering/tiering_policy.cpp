#include "tiering_policy.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "payload/manager/v1_compat.hpp"

namespace payload::tiering {

using namespace payload::manager::v1;

TieringPolicy::TieringPolicy(std::shared_ptr<payload::metadata::MetadataCache> cache)
    : cache_(std::move(cache)) {}

std::optional<PayloadID>
TieringPolicy::ChooseRamEviction(const PressureState& state) {

  if (!state.RamPressure())
    return std::nullopt;

  // simple heuristic placeholder
  // later: LRU / LFU / cost model

  return std::nullopt;
}

std::optional<PayloadID>
TieringPolicy::ChooseGpuEviction(const PressureState& state) {

  if (!state.GpuPressure())
    return std::nullopt;

  return std::nullopt;
}

}

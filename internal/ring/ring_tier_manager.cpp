#include "internal/ring/ring_tier_manager.hpp"

#include <stdexcept>

#include "spdlog/spdlog.h"

namespace payload::ring {

/*static*/
std::unique_ptr<RingTierManager> RingTierManager::Build(const payload::runtime::config::RingTierConfig& cfg, const std::string& default_shm_prefix) {
  std::unique_ptr<RingTierManager> mgr(new RingTierManager());
  for (const auto& def : cfg.rings()) {
    if (def.ring_id().empty()) {
      throw std::invalid_argument("RingDefinition: ring_id is required");
    }
    if (mgr->rings_.contains(def.ring_id())) {
      throw std::invalid_argument("RingDefinition: duplicate ring_id '" + def.ring_id() + "'");
    }
    Ring::Config rc{
        .ring_id         = def.ring_id(),
        .n_slots         = def.n_slots() > 0 ? def.n_slots() : 4,
        .slot_size_bytes = def.slot_size_bytes(),
        .shm_prefix      = !def.shm_prefix().empty() ? def.shm_prefix() : default_shm_prefix,
    };
    if (def.exhaustion_policy() != payload::manager::core::v1::RING_EXHAUSTION_POLICY_UNSPECIFIED) {
      rc.exhaustion_policy = def.exhaustion_policy();
    }
    mgr->rings_.emplace(def.ring_id(), std::make_unique<Ring>(std::move(rc)));
  }
  spdlog::info("payload-manager: ring tier configured with {} ring(s)", mgr->rings_.size());
  return mgr;
}

Ring* RingTierManager::GetRing(const std::string& ring_id) {
  auto it = rings_.find(ring_id);
  return it == rings_.end() ? nullptr : it->second.get();
}

std::vector<std::string> RingTierManager::RingIds() const {
  std::vector<std::string> out;
  out.reserve(rings_.size());
  for (const auto& [id, _] : rings_) out.push_back(id);
  return out;
}

} // namespace payload::ring

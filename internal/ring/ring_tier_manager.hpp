#pragma once

// Owns every Ring configured in PM's static config. Built by
// factory::Build from RuntimeConfig.storage.ring.rings[]; lives for
// the PM process lifetime; passed to RingService as a shared
// dependency.

#include <memory>
#include <string>
#include <unordered_map>

#include "config/config.pb.h"
#include "internal/ring/ring.hpp"

namespace payload::ring {

class RingTierManager {
 public:
  // Build from the RingTierConfig sub-message of RuntimeConfig.storage.
  // Each RingDefinition becomes a Ring. The `default_shm_prefix` is
  // RamTierConfig.shm_prefix (or "pm" if empty) — RingDefinitions
  // that don't set their own shm_prefix inherit this.
  static std::unique_ptr<RingTierManager> Build(const payload::runtime::config::RingTierConfig& cfg, const std::string& default_shm_prefix);

  // Returns nullptr if ring_id is not configured. Caller surfaces
  // NOT_FOUND.
  Ring* GetRing(const std::string& ring_id);

  // Snapshot of configured ring ids. For diagnostics / admin endpoints.
  std::vector<std::string> RingIds() const;

  // Number of configured rings. Quick check whether the ring tier is
  // active at all (factory can skip wiring PayloadRingService when 0).
  size_t Count() const {
    return rings_.size();
  }

 private:
  RingTierManager() = default;

  std::unordered_map<std::string, std::unique_ptr<Ring>> rings_;
};

} // namespace payload::ring

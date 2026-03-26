#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "internal/db/api/repository.hpp"
#include "internal/util/uuid.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/storage/storage_factory.hpp"
#include "payload/manager/core/v1/id.pb.h"
#include "payload/manager/core/v1/placement.pb.h"
#include "payload/manager/core/v1/policy.pb.h"
#include "payload/manager/core/v1/types.pb.h"
#include "payload/manager/runtime/v1/lease.pb.h"
#include "payload/manager/v1.hpp"

namespace payload::lease {
class LeaseManager;
}

namespace payload::metadata {
class MetadataCache;
}

namespace payload::core {

class PayloadManager {
 public:
  PayloadManager(payload::storage::StorageFactory::TierMap storage, std::shared_ptr<payload::lease::LeaseManager> lease_mgr,
                 std::shared_ptr<payload::db::Repository> repository, std::shared_ptr<payload::metadata::MetadataCache> metadata_cache = nullptr);

  payload::manager::v1::PayloadDescriptor Allocate(uint64_t size_bytes, payload::manager::v1::Tier preferred, uint64_t ttl_ms = 0,
                                                   bool no_evict = false, const payload::manager::core::v1::EvictionPolicy& eviction_policy = {});
  void                                    ExpireStale();
  payload::manager::v1::PayloadDescriptor Commit(const payload::manager::v1::PayloadID& id);
  void                                    Delete(const payload::manager::v1::PayloadID& id, bool force);

  bool                       IsEvictionExempt(const payload::manager::v1::PayloadID& id) const;
  payload::manager::v1::Tier GetSpillTarget(const payload::manager::v1::PayloadID& id) const;

  // Returns a snapshot of per-tier byte totals (keyed by Tier enum int value).
  std::unordered_map<int, uint64_t> GetTierBytes() const;

  payload::manager::v1::PayloadDescriptor        ResolveSnapshot(const payload::manager::v1::PayloadID& id);
  payload::manager::v1::AcquireReadLeaseResponse AcquireReadLease(
      const payload::manager::v1::PayloadID& id, payload::manager::v1::Tier min_tier, uint64_t min_duration_ms,
      payload::manager::core::v1::PromotionPolicy promotion_policy = payload::manager::core::v1::PROMOTION_POLICY_UNSPECIFIED);

  void HydrateCaches();

  void                                    ReleaseLease(const payload::manager::v1::LeaseID& lease_id);
  payload::manager::v1::PayloadDescriptor Promote(const payload::manager::v1::PayloadID& id, payload::manager::v1::Tier target);
  void                                    ExecuteSpill(const payload::manager::v1::PayloadID& id, payload::manager::v1::Tier target, bool fsync);
  void                                    Prefetch(const payload::manager::v1::PayloadID& id, payload::manager::v1::Tier target);
  void                                    Pin(const payload::manager::v1::PayloadID& id, uint64_t duration_ms);
  void                                    Unpin(const payload::manager::v1::PayloadID& id);

 private:
  static payload::util::UUID Key(const payload::manager::v1::PayloadID& id);

  void                                    CacheSnapshot(const payload::manager::v1::PayloadDescriptor& descriptor);
  void                                    PopulateLocation(payload::manager::v1::PayloadDescriptor* descriptor);
  payload::manager::v1::PayloadDescriptor PromoteUnlocked(const payload::manager::v1::PayloadID& id, payload::manager::v1::Tier target);
  std::shared_ptr<std::shared_mutex>      PayloadMutex(const payload::manager::v1::PayloadID& id);

  payload::storage::StorageFactory::TierMap         storage_;
  std::shared_ptr<payload::lease::LeaseManager>     lease_mgr_;
  std::shared_ptr<payload::db::Repository>          repository_;
  std::shared_ptr<payload::metadata::MetadataCache> metadata_cache_;
  std::string                                       shm_prefix_{"pm"};

  // Serializes Delete with AcquireReadLease to prevent TOCTOU on lease checks.
  mutable std::mutex delete_mutex_;

  // Snapshot cache consistency model:
  // - ResolveSnapshot first serves reads from this cache.
  // - Repository reads are only used on cache misses and during explicit refresh (HydrateCaches).
  // - Mutations routed through PayloadManager (Allocate/Commit/Promote/Delete) refresh or invalidate
  //   cache entries synchronously with successful transaction commits.
  // - Out-of-band repository writes can be stale until HydrateCaches() is called.
  mutable std::shared_mutex                                                          snapshot_cache_mutex_;
  std::unordered_map<payload::util::UUID, payload::manager::v1::PayloadDescriptor> snapshot_cache_;

  mutable std::mutex                                                                    payload_mutexes_guard_;
  mutable std::unordered_map<payload::util::UUID, std::shared_ptr<std::shared_mutex>> payload_mutexes_;

  struct PinState {
    std::optional<uint64_t> expires_at_ms;
  };

  mutable std::mutex                                  pins_guard_;
  std::unordered_map<payload::util::UUID, PinState> pins_;

  bool IsPinnedLocked(const payload::util::UUID& key, uint64_t now_ms);
  void SweepExpiredPins();

  // IDs that must never be automatically evicted (no_evict=true or EVICTION_PRIORITY_NEVER).
  mutable std::mutex                   no_evict_guard_;
  std::unordered_set<payload::util::UUID> no_evict_ids_;

  // Preferred spill tier per payload (default TIER_DISK when absent).
  mutable std::mutex                                                    spill_targets_guard_;
  std::unordered_map<payload::util::UUID, payload::manager::v1::Tier> spill_targets_;

  // Per-tier byte totals and payload counts for occupancy metrics.
  mutable std::mutex                tier_bytes_guard_;
  std::unordered_map<int, uint64_t> tier_bytes_;
  mutable std::mutex                tier_count_guard_;
  std::unordered_map<int, uint64_t> tier_count_;

  void UpdateTierBytes(payload::manager::v1::Tier tier, int64_t delta);
  void UpdateTierCount(payload::manager::v1::Tier tier, int64_t delta);
};

} // namespace payload::core

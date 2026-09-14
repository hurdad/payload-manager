#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "internal/db/api/repository.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/storage/storage_factory.hpp"
#include "internal/tiering/pressure_state.hpp"
#include "internal/util/uuid.hpp"
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

  // Returns the upload URI for a TIER_OBJECT payload (e.g. "s3://bucket/prefix/<uuid>.bin").
  // Empty string when no object store is configured.
  std::string GetObjectUploadPath(const payload::manager::v1::PayloadID& id) const;

  // Register an externally-uploaded object-tier payload as DURABLE.
  // Called after the client has uploaded bytes to GetObjectUploadPath(id).
  // Transitions the payload ALLOCATED→DURABLE and writes the sidecar JSON.
  void Import(const payload::manager::v1::PayloadID& id, uint64_t size_bytes);

  bool                       IsEvictionExempt(const payload::manager::v1::PayloadID& id) const;
  payload::manager::v1::Tier GetSpillTarget(const payload::manager::v1::PayloadID& id) const;
  // Returns the terminal tier for a disk-resident payload under eviction pressure.
  // Respects per-payload TIER_VOID overrides; defaults to TIER_OBJECT.
  payload::manager::v1::Tier GetDiskSpillTarget(const payload::manager::v1::PayloadID& id) const;

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
  mutable std::shared_mutex                                                        snapshot_cache_mutex_;
  std::unordered_map<payload::util::UUID, payload::manager::v1::PayloadDescriptor> snapshot_cache_;

  mutable std::mutex                                                                  payload_mutexes_guard_;
  mutable std::unordered_map<payload::util::UUID, std::shared_ptr<std::shared_mutex>> payload_mutexes_;

  struct PinState {
    std::optional<uint64_t> expires_at_ms;
  };

  mutable std::mutex                                pins_guard_;
  std::unordered_map<payload::util::UUID, PinState> pins_;

  bool IsPinnedLocked(const payload::util::UUID& key, uint64_t now_ms);
  void SweepExpiredPins();

  // IDs that must never be automatically evicted (no_evict=true or EVICTION_PRIORITY_NEVER).
  mutable std::mutex                      no_evict_guard_;
  std::unordered_set<payload::util::UUID> no_evict_ids_;

  // Preferred spill tier per payload (default TIER_DISK when absent).
  mutable std::mutex                                                  spill_targets_guard_;
  std::unordered_map<payload::util::UUID, payload::manager::v1::Tier> spill_targets_;

  // Per-tier byte totals and payload counts for occupancy metrics.
  mutable std::mutex                tier_bytes_guard_;
  std::unordered_map<int, uint64_t> tier_bytes_;
  mutable std::mutex                tier_count_guard_;
  std::unordered_map<int, uint64_t> tier_count_;

  void UpdateTierBytes(payload::manager::v1::Tier tier, int64_t delta);
  void UpdateTierCount(payload::manager::v1::Tier tier, int64_t delta);

  /*
    Admission control for a tier's hard cap.

    Without this, an allocation that does not fit still succeeds: shm_open,
    ftruncate and mmap only touch metadata and address space, so the tmpfs
    overcommit is not discovered until the *producer* writes into the mapping
    and takes SIGBUS. Refusing here turns that into a RESOURCE_EXHAUSTED the
    caller can act on.

    The check and the increment happen under one lock, so concurrent
    allocations cannot each observe room for the last slot and both proceed.
    The returned guard releases the reservation unless Commit() is called, so
    a failed backend allocation or database insert does not leak tier bytes.
  */
  class TierReservation {
   public:
    TierReservation(PayloadManager* owner, payload::manager::v1::Tier tier, uint64_t bytes) : owner_(owner), tier_(tier), bytes_(bytes) {
    }
    TierReservation(TierReservation&& other) noexcept : owner_(other.owner_), tier_(other.tier_), bytes_(other.bytes_) {
      other.owner_ = nullptr;
    }
    TierReservation(const TierReservation&)            = delete;
    TierReservation& operator=(const TierReservation&) = delete;
    TierReservation& operator=(TierReservation&&)      = delete;

    ~TierReservation() {
      if (owner_ != nullptr) {
        owner_->UpdateTierBytes(tier_, -static_cast<int64_t>(bytes_));
      }
    }

    // Keep the bytes: the payload is now durably recorded.
    void Commit() {
      owner_ = nullptr;
    }

   private:
    PayloadManager*            owner_;
    payload::manager::v1::Tier tier_;
    uint64_t                   bytes_;
  };

  // Throws payload::util::ResourceExhausted when the tier cannot take the bytes.
  TierReservation ReserveTierBytes(payload::manager::v1::Tier tier, uint64_t size_bytes);

  // Hard cap for a tier, or UINT64_MAX when uncapped / not configured.
  uint64_t TierLimit(payload::manager::v1::Tier tier) const;

  // Optional: when unset (the default, and the case in most unit tests) no
  // admission control is applied and behaviour matches the original code.
  std::shared_ptr<payload::tiering::PressureState> pressure_state_;

 public:
  // Wired by the factory once the configured tier limits are known.
  void SetPressureState(std::shared_ptr<payload::tiering::PressureState> state) {
    pressure_state_ = std::move(state);
  }
};

} // namespace payload::core

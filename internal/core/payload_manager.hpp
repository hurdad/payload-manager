#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "internal/db/api/repository.hpp"
#include "internal/storage/storage_factory.hpp"
#include "payload/manager/core/v1/id.pb.h"
#include "payload/manager/core/v1/placement.pb.h"
#include "payload/manager/core/v1/types.pb.h"
#include "payload/manager/runtime/v1/lease.pb.h"
#include "payload/manager/v1.hpp"

namespace payload::lease {
class LeaseManager;
}
namespace payload::metadata {
class MetadataCache;
}
namespace payload::lineage {
class LineageGraph;
}

namespace payload::core {

class PayloadManager {
 public:
  PayloadManager(payload::storage::StorageFactory::TierMap storage, std::shared_ptr<payload::lease::LeaseManager> lease_mgr,
                 std::shared_ptr<payload::metadata::MetadataCache> metadata, std::shared_ptr<payload::lineage::LineageGraph> lineage,
                 std::shared_ptr<payload::db::Repository> repository);

  payload::manager::v1::PayloadDescriptor Allocate(uint64_t size_bytes, payload::manager::v1::Tier preferred);
  payload::manager::v1::PayloadDescriptor Commit(const payload::manager::v1::PayloadID& id);
  void                                    Delete(const payload::manager::v1::PayloadID& id, bool force);

  payload::manager::v1::PayloadDescriptor        ResolveSnapshot(const payload::manager::v1::PayloadID& id);
  payload::manager::v1::AcquireReadLeaseResponse AcquireReadLease(const payload::manager::v1::PayloadID& id, payload::manager::v1::Tier min_tier,
                                                                  uint64_t min_duration_ms);

  void HydrateCaches();

  void                                    ReleaseLease(const payload::manager::v1::LeaseID& lease_id);
  payload::manager::v1::PayloadDescriptor Promote(const payload::manager::v1::PayloadID& id, payload::manager::v1::Tier target);
  void                                    ExecuteSpill(const payload::manager::v1::PayloadID& id, payload::manager::v1::Tier target, bool fsync);
  void                                    Prefetch(const payload::manager::v1::PayloadID& id, payload::manager::v1::Tier target);
  void                                    Pin(const payload::manager::v1::PayloadID& id, uint64_t duration_ms);
  void                                    Unpin(const payload::manager::v1::PayloadID& id);

 private:
  static std::string Key(const payload::manager::v1::PayloadID& id);

  void                                    CacheSnapshot(const payload::manager::v1::PayloadDescriptor& descriptor);
  void                                    PopulateLocation(payload::manager::v1::PayloadDescriptor* descriptor);
  payload::manager::v1::PayloadDescriptor PromoteUnlocked(const payload::manager::v1::PayloadID& id, payload::manager::v1::Tier target);
  std::shared_ptr<std::shared_mutex>      PayloadMutex(const payload::manager::v1::PayloadID& id);

  payload::storage::StorageFactory::TierMap     storage_;
  std::shared_ptr<payload::lease::LeaseManager> lease_mgr_;
  std::shared_ptr<payload::db::Repository>      repository_;

  // Serializes Delete with AcquireReadLease to prevent TOCTOU on lease checks.
  mutable std::mutex delete_mutex_;

  // Snapshot cache consistency model:
  // - ResolveSnapshot first serves reads from this cache.
  // - Repository reads are only used on cache misses and during explicit refresh (HydrateCaches).
  // - Mutations routed through PayloadManager (Allocate/Commit/Promote/Delete) refresh or invalidate
  //   cache entries synchronously with successful transaction commits.
  // - Out-of-band repository writes can be stale until HydrateCaches() is called.
  mutable std::shared_mutex                                                snapshot_cache_mutex_;
  std::unordered_map<std::string, payload::manager::v1::PayloadDescriptor> snapshot_cache_;

  mutable std::mutex                                                          payload_mutexes_guard_;
  mutable std::unordered_map<std::string, std::shared_ptr<std::shared_mutex>> payload_mutexes_;

  struct PinState {
    std::optional<uint64_t> expires_at_ms;
  };

  mutable std::mutex                        pins_guard_;
  std::unordered_map<std::string, PinState> pins_;

  bool IsPinnedLocked(const std::string& key, uint64_t now_ms);
};

} // namespace payload::core

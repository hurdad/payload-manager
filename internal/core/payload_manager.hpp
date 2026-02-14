#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

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
                 std::shared_ptr<payload::metadata::MetadataCache> metadata, std::shared_ptr<payload::lineage::LineageGraph> lineage);

  payload::manager::v1::PayloadDescriptor Allocate(uint64_t size_bytes, payload::manager::v1::Tier preferred);
  payload::manager::v1::PayloadDescriptor Commit(const payload::manager::v1::PayloadID& id);
  void                                    Delete(const payload::manager::v1::PayloadID& id, bool force);

  payload::manager::v1::PayloadDescriptor        ResolveSnapshot(const payload::manager::v1::PayloadID& id);
  payload::manager::v1::AcquireReadLeaseResponse AcquireReadLease(const payload::manager::v1::PayloadID& id, payload::manager::v1::Tier min_tier,
                                                                  uint64_t min_duration_ms);

  void                                    ReleaseLease(const std::string& lease_id);
  payload::manager::v1::PayloadDescriptor Promote(const payload::manager::v1::PayloadID& id, payload::manager::v1::Tier target);
  void                                    ExecuteSpill(const payload::manager::v1::PayloadID& id, payload::manager::v1::Tier target, bool /*fsync*/);

 private:
  static std::string Key(const payload::manager::v1::PayloadID& id);

  payload::storage::StorageFactory::TierMap     storage_;
  std::shared_ptr<payload::lease::LeaseManager> lease_mgr_;

  std::mutex                                                               mutex_;
  std::unordered_map<std::string, payload::manager::v1::PayloadDescriptor> payloads_;
};

} // namespace payload::core

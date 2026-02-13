#pragma once

#include <memory>
#include <string>
#include <vector>

#include "payload/manager/v1/id.pb.h"
#include "payload/manager/v1/lease.pb.h"
#include "payload/manager/v1/types.pb.h"
#include "payload/manager/v1/placement.pb.h"   // contains PayloadDescriptor

namespace payload::lease { class LeaseManager; }
namespace payload::storage { class StorageRouter; }
namespace payload::db { class PayloadRepository; }

namespace payload::core {

/*
  PayloadManager

  Central correctness authority.

  Owns invariants:
    - lifecycle transitions
    - lease fencing
    - placement stability
    - tier guarantees

  IMPORTANT:
  All public methods return PayloadDescriptor because it is the
  canonical state view of a payload.
*/
class PayloadManager {
public:
    PayloadManager(
        std::shared_ptr<payload::lease::LeaseManager> lease_mgr,
        std::shared_ptr<payload::storage::StorageRouter> storage,
        std::shared_ptr<payload::db::PayloadRepository> repo
    );

    // ----- lifecycle -----

    payload::manager::v1::PayloadDescriptor
    Allocate(uint64_t size_bytes,
             payload::manager::v1::Tier preferred);

    payload::manager::v1::PayloadDescriptor
    Commit(const payload::manager::v1::PayloadID& id);

    void Delete(const payload::manager::v1::PayloadID& id, bool force);

    // ----- read path -----

    // advisory snapshot (may move immediately after return)
    payload::manager::v1::PayloadDescriptor
    ResolveSnapshot(const payload::manager::v1::PayloadID& id);

    // authoritative stable descriptor
    payload::manager::v1::AcquireReadLeaseResponse
    AcquireReadLease(const payload::manager::v1::PayloadID& id,
                     payload::manager::v1::Tier min_tier,
                     uint64_t min_duration_ms);

    void ReleaseLease(const std::string& lease_id);

    // ----- tiering -----

    payload::manager::v1::PayloadDescriptor
    Promote(const payload::manager::v1::PayloadID& id,
            payload::manager::v1::Tier target);

private:
    std::shared_ptr<payload::lease::LeaseManager> lease_mgr_;
    std::shared_ptr<payload::storage::StorageRouter> storage_;
    std::shared_ptr<payload::db::PayloadRepository> repo_;

    // Ensures descriptor is readable (exists + active + location valid)
    void EnsureReadable(const payload::manager::v1::PayloadDescriptor& desc) const;
};

} // namespace payload::core

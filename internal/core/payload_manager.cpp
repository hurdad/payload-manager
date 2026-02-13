#include "payload_manager.hpp"

#include <stdexcept>

using namespace payload::manager::v1;

namespace payload::core {

PayloadManager::PayloadManager(
    std::shared_ptr<payload::lease::LeaseManager> lease_mgr,
    std::shared_ptr<payload::storage::StorageRouter> storage,
    std::shared_ptr<payload::db::PayloadRepository> repo)
    : lease_mgr_(std::move(lease_mgr)),
      storage_(std::move(storage)),
      repo_(std::move(repo))
{
}

/*
  Allocate

  Creates payload in ALLOCATED state and reserves storage capacity.
*/
PayloadDescriptor
PayloadManager::Allocate(uint64_t size_bytes, Tier preferred)
{
    auto tx = repo_->Begin();

    // storage decides placement
    auto placement = storage_->Allocate(size_bytes, preferred);

    PayloadDescriptor desc;
    *desc.mutable_uuid() = placement.uuid();
    desc.set_tier(preferred);
    desc.set_state(PAYLOAD_STATE_ALLOCATED);
    desc.set_version(1);

    *desc.mutable_location() = placement.location();

    repo_->InsertPayload(*tx, desc);

    tx->Commit();
    return desc;
}

/*
  Commit

  Makes payload visible to readers.
*/
PayloadDescriptor
PayloadManager::Commit(const PayloadID& id)
{
    auto tx = repo_->Begin();

    auto desc = repo_->GetPayload(*tx, id.uuid());
    if (!desc)
        throw std::runtime_error("commit: payload not found");

    if (desc->state() != PAYLOAD_STATE_ALLOCATED)
        throw std::runtime_error("commit: invalid state");

    desc->set_state(PAYLOAD_STATE_ACTIVE);
    desc->set_version(desc->version() + 1);

    repo_->UpdatePayload(*tx, *desc);

    tx->Commit();
    return *desc;
}

/*
  Delete

  Removes payload (honors leases unless force).
*/
void PayloadManager::Delete(const PayloadID& id, bool force)
{
    auto tx = repo_->Begin();

    if (!force && lease_mgr_->HasActiveLease(id.uuid()))
        throw std::runtime_error("delete: active lease");

    auto desc = repo_->GetPayload(*tx, id.uuid());
    if (!desc)
        return;

    storage_->Remove(*desc);
    repo_->DeletePayload(*tx, id.uuid());

    tx->Commit();
}

/*
  ResolveSnapshot

  Advisory lookup only — may move immediately.
*/
PayloadDescriptor
PayloadManager::ResolveSnapshot(const PayloadID& id)
{
    auto tx = repo_->Begin();

    auto desc = repo_->GetPayload(*tx, id.uuid());
    if (!desc)
        throw std::runtime_error("resolve: not found");

    tx->Commit();
    return *desc;
}

/*
  AcquireReadLease

  Guarantees descriptor stability for lease duration.
*/
AcquireReadLeaseResponse
PayloadManager::AcquireReadLease(
    const PayloadID& id,
    Tier min_tier,
    uint64_t min_duration_ms)
{
    auto tx = repo_->Begin();

    auto desc = repo_->GetPayload(*tx, id.uuid());
    if (!desc)
        throw std::runtime_error("lease: not found");

    // ensure tier requirement
    if (desc->tier() < min_tier) {
        auto promoted = storage_->Promote(*desc, min_tier);
        *desc = promoted;
        repo_->UpdatePayload(*tx, *desc);
    }

    EnsureReadable(*desc);

    // create lease AFTER stable location ensured
    auto lease = lease_mgr_->CreateLease(desc->uuid(), min_duration_ms);

    AcquireReadLeaseResponse resp;
    *resp.mutable_payload_descriptor() = *desc;
    resp.set_lease_id(lease.lease_id);
    *resp.mutable_lease_expires_at() = lease.expires_at;

    tx->Commit();
    return resp;
}

void PayloadManager::ReleaseLease(const std::string& lease_id)
{
    lease_mgr_->Release(lease_id);
}

/*
  Promote

  Explicit tier movement.
*/
PayloadDescriptor
PayloadManager::Promote(const PayloadID& id, Tier target)
{
    auto tx = repo_->Begin();

    auto desc = repo_->GetPayload(*tx, id.uuid());
    if (!desc)
        throw std::runtime_error("promote: not found");

    auto promoted = storage_->Promote(*desc, target);

    promoted.set_version(desc->version() + 1);
    repo_->UpdatePayload(*tx, promoted);

    tx->Commit();
    return promoted;
}

/*
  EnsureReadable

  Validates descriptor correctness before returning to client.
*/
void PayloadManager::EnsureReadable(const PayloadDescriptor& desc) const
{
    if (desc.state() != PAYLOAD_STATE_ACTIVE &&
        desc.state() != PAYLOAD_STATE_DURABLE)
        throw std::runtime_error("payload not readable");

    if (!storage_->Exists(desc))
        throw std::runtime_error("payload location missing");
}

}

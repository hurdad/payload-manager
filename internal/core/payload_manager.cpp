#include "payload_manager.hpp"

#include <stdexcept>

#include "internal/lease/lease_manager.hpp"
#include "internal/util/time.hpp"
#include "internal/util/uuid.hpp"
#include "payload/manager/v1.hpp"

namespace payload::core {

using namespace payload::manager::v1;

namespace {

bool IsReadableState(PayloadState state) {
    return state == PAYLOAD_STATE_ACTIVE ||
           state == PAYLOAD_STATE_SPILLING ||
           state == PAYLOAD_STATE_DURABLE;
}

} // namespace

PayloadManager::PayloadManager(
    payload::storage::StorageFactory::TierMap storage,
    std::shared_ptr<payload::lease::LeaseManager> lease_mgr,
    std::shared_ptr<payload::metadata::MetadataCache>,
    std::shared_ptr<payload::lineage::LineageGraph>)
    : storage_(std::move(storage)), lease_mgr_(std::move(lease_mgr)) {}

std::string PayloadManager::Key(const PayloadID& id) { return id.value(); }

PayloadDescriptor PayloadManager::Allocate(uint64_t size_bytes, Tier preferred) {
    PayloadDescriptor desc;
    *desc.mutable_id() = payload::util::ToProto(payload::util::GenerateUUID());
    desc.set_tier(preferred);
    desc.set_state(PAYLOAD_STATE_ALLOCATED);
    desc.set_version(1);
    *desc.mutable_created_at() = payload::util::ToProto(payload::util::Now());

    RamLocation ram;
    ram.set_length_bytes(size_bytes);
    ram.set_slab_id(0);
    ram.set_block_index(0);
    ram.set_shm_name("payload");
    *desc.mutable_ram() = ram;

    std::lock_guard<std::mutex> lock(mutex_);
    payloads_[Key(desc.id())] = desc;
    return desc;
}

PayloadDescriptor PayloadManager::Commit(const PayloadID& id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = payloads_.find(Key(id));
    if (it == payloads_.end()) throw std::runtime_error("commit: payload not found");
    it->second.set_state(PAYLOAD_STATE_ACTIVE);
    it->second.set_version(it->second.version() + 1);
    return it->second;
}

void PayloadManager::Delete(const PayloadID& id, bool force) {
    if (!force && lease_mgr_->HasActiveLeases(id)) {
        throw std::runtime_error("delete: active lease");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    payloads_.erase(Key(id));
}

PayloadDescriptor PayloadManager::ResolveSnapshot(const PayloadID& id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = payloads_.find(Key(id));
    if (it == payloads_.end()) throw std::runtime_error("resolve: not found");
    return it->second;
}

AcquireReadLeaseResponse PayloadManager::AcquireReadLease(const PayloadID& id,
                                                          Tier min_tier,
                                                          uint64_t min_duration_ms) {
    auto desc = ResolveSnapshot(id);
    if (desc.tier() < min_tier) {
      desc = Promote(id, min_tier);
    }
    if (!IsReadableState(desc.state())) {
        throw std::runtime_error("acquire lease: payload not committed");
    }
    auto lease = lease_mgr_->Acquire(id, desc, min_duration_ms);

    AcquireReadLeaseResponse resp;
    *resp.mutable_payload_descriptor() = desc;
    resp.set_lease_id(lease.lease_id);
    *resp.mutable_lease_expires_at() = payload::util::ToProto(lease.expires_at);
    return resp;
}

void PayloadManager::ReleaseLease(const std::string& lease_id) { lease_mgr_->Release(lease_id); }

PayloadDescriptor PayloadManager::Promote(const PayloadID& id, Tier target) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = payloads_.find(Key(id));
    if (it == payloads_.end()) throw std::runtime_error("promote: not found");
    it->second.set_tier(target);
    it->second.set_version(it->second.version() + 1);
    return it->second;
}

void PayloadManager::ExecuteSpill(const PayloadID& id, Tier target, bool) {
    (void)Promote(id, target);
}

} // namespace payload::core

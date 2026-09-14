#include "internal/service/ring_service.hpp"

#include "internal/util/errors.hpp"

namespace payload::service {

namespace runtimev1 = payload::manager::runtime::v1;

RingService::RingService(payload::ring::RingTierManager* ring_mgr, payload::ring::RingLeaseTable* lease_table)
    : ring_mgr_(ring_mgr), lease_table_(lease_table) {
}

runtimev1::AcquireRingSlotResponse RingService::AcquireRingSlot(const runtimev1::AcquireRingSlotRequest& req) {
  auto* ring = ring_mgr_->GetRing(req.ring_id());
  if (!ring) throw payload::util::NotFound("ring not configured: " + req.ring_id());

  auto acq = ring->Acquire();
  if (!acq) {
    // Every slot is currently leased — DROP_NEW policy returns
    // RESOURCE_EXHAUSTED. (BLOCK policy is v2 — for now any caller
    // wanting back-pressure handles it client-side by retrying.)
    throw payload::util::ResourceExhausted("ring '" + req.ring_id() + "' has no available slots (all currently leased)");
  }

  runtimev1::AcquireRingSlotResponse resp;
  resp.set_slot_idx(acq->slot_idx);
  resp.set_generation(acq->generation);
  resp.set_shm_name(acq->shm_name);
  resp.set_capacity_bytes(acq->capacity_bytes);
  return resp;
}

runtimev1::CommitRingSlotResponse RingService::CommitRingSlot(const runtimev1::CommitRingSlotRequest& req) {
  auto* ring = ring_mgr_->GetRing(req.ring_id());
  if (!ring) throw payload::util::NotFound("ring not configured: " + req.ring_id());

  if (!ring->Commit(req.slot_idx(), req.generation(), req.size_bytes())) {
    // Either the (slot, gen) doesn't match an outstanding Acquire
    // (double commit, stale handle), the slot index is OOB, or
    // size_bytes exceeded capacity.
    throw payload::util::InvalidState("CommitRingSlot rejected for ring '" + req.ring_id() + "' slot " + std::to_string(req.slot_idx()) + " gen " +
                                      std::to_string(req.generation()));
  }

  runtimev1::CommitRingSlotResponse resp;
  // committed_at is informational; clients use it for end-to-end latency.
  const auto now     = std::chrono::system_clock::now();
  const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
  const auto nanos   = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count() % 1'000'000'000;
  resp.mutable_committed_at()->set_seconds(seconds);
  resp.mutable_committed_at()->set_nanos(static_cast<int32_t>(nanos));
  return resp;
}

runtimev1::MapRingResponse RingService::MapRing(const runtimev1::MapRingRequest& req) {
  auto* ring = ring_mgr_->GetRing(req.ring_id());
  if (!ring) throw payload::util::NotFound("ring not configured: " + req.ring_id());

  runtimev1::MapRingResponse resp;
  resp.set_n_slots(ring->n_slots());
  resp.set_slot_capacity_bytes(ring->slot_size_bytes());
  for (const auto& name : ring->SlotShmNames()) resp.add_slot_shm_names(name);
  resp.set_backing_tier(payload::manager::core::v1::TIER_RAM_RING);
  return resp;
}

runtimev1::LeaseRingSlotResponse RingService::LeaseRingSlot(const runtimev1::LeaseRingSlotRequest& req) {
  auto* ring = ring_mgr_->GetRing(req.ring_id());
  if (!ring) throw payload::util::NotFound("ring not configured: " + req.ring_id());

  auto grant = ring->Lease(req.slot_idx(), req.generation());
  if (!grant) {
    // Generation mismatch (consumer was too slow; producer already
    // recycled the slot) or slot_idx OOB. Caller should skip this
    // capture and wait for the next event.
    throw payload::util::InvalidState("LeaseRingSlot rejected for ring '" + req.ring_id() + "' slot " + std::to_string(req.slot_idx()) + " gen " +
                                      std::to_string(req.generation()) + " (generation mismatch or invalid slot)");
  }

  const auto lease_id = lease_table_->Insert(req.ring_id(), grant->slot_idx, grant->generation);

  runtimev1::LeaseRingSlotResponse resp;
  resp.set_lease_id(payload::ring::RingLeaseTable::ToBytes(lease_id));
  resp.set_slot_idx(grant->slot_idx);
  resp.set_generation(grant->generation);
  return resp;
}

void RingService::ReleaseRingSlot(const runtimev1::ReleaseRingSlotRequest& req) {
  // Tolerate empty / malformed lease_id (return OK silently). The RPC
  // contract is idempotent: a retry on a release that already
  // succeeded should not fail.
  auto id = payload::ring::RingLeaseTable::FromBytes(req.lease_id());
  if (!id) return;

  auto rec = lease_table_->Remove(*id);
  if (!rec) return; // unknown / already released

  // We don't trust the original ring_id from the table (could be
  // tampered if a future change adds untrusted lease handles), but
  // it's our own record so it's safe. Look up the ring and decref.
  auto* ring = ring_mgr_->GetRing(rec->ring_id);
  if (!ring) return; // ring went away under us (config reload?) — no-op
  ring->Release(rec->slot_idx, rec->generation);
}

} // namespace payload::service

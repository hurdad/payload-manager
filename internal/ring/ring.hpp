#pragma once

// Single TIER_RAM_RING ring: N pre-allocated POSIX shm slots, rotated
// across captures. See ring_tier_manager.hpp for the multi-ring
// container that owns one of these per configured ring_id.
//
// Concurrency model: one mutex per Ring guarding the slot table.
// Acquire / Commit / Lease / Release all take it briefly. At realistic
// capture rates (1-1000 Hz) the contention is negligible vs the
// payload-write work the producer does between Acquire and Commit.
// Per-slot locks are a future optimization once profiling motivates
// it.
//
// Slot lifecycle (per slot, per generation):
//   AVAILABLE  -- Acquire -->  WRITING
//   WRITING    -- Commit  -->  READABLE   (refcount may then go 0 -> N -> 0)
//   READABLE   -- (refcount==0 + next Acquire wins this slot) -->  WRITING (gen++)
//
// Acquire scans for AVAILABLE or (READABLE & refcount==0) slots
// round-robin from the last acquired index. Mismatch on consumer
// generation (consumer was slow) -> FAILED_PRECONDITION at Lease.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "payload/manager/core/v1/ring_slot.pb.h"

namespace payload::ring {

enum class SlotStatus : uint8_t {
  kAvailable = 0, // Never written, or its prior generation was released by all consumers.
  kWriting   = 1, // Producer holds it; no consumer can lease this generation yet.
  kReadable  = 2, // Committed; lease-able. Refcount may be 0..N.
};

// Per-slot state. Reserved at ring construction time; mutated by
// Ring::{Acquire,Commit,Lease,Release}. The host_va / shm_name are
// stable for the slot's lifetime (until ring teardown).
struct Slot {
  uint32_t    idx = 0;
  std::string shm_name;                 // POSIX shm name, e.g. "/pm-ring-radio_iq-slot0"
  size_t      capacity_bytes = 0;       // ftruncated size; fixed for slot lifetime
  void*       host_va        = nullptr; // PM's mmap, kept open for the ring's lifetime
  int         fd             = -1;      // shm fd, closed on teardown

  // Mutable state. Protected by Ring::mu_.
  SlotStatus                            status     = SlotStatus::kAvailable;
  uint64_t                              generation = 0; // monotonically bumps on each Acquire
  uint32_t                              refcount   = 0; // number of live Leases against `generation`
  uint64_t                              size_bytes = 0; // bytes written this generation; set by Commit
  std::chrono::steady_clock::time_point committed_at{};
};

// Outcome of Acquire: a reservation the producer fills in and then
// commits. If no slot is available (every slot's refcount > 0) the
// optional is empty -- caller surfaces RESOURCE_EXHAUSTED per the
// ring's exhaustion policy. (BLOCK policy is parked for v2; v1 is
// DROP_NEW only.)
struct AcquireResult {
  uint32_t    slot_idx;
  uint64_t    generation;
  std::string shm_name;
  size_t      capacity_bytes;
};

// Outcome of Lease: bumped refcount on (slot_idx, generation). The
// consumer's prior MapRing call gave it shm_names + capacities; this
// just authorizes the read for this specific (slot, generation).
struct LeaseGrant {
  uint32_t slot_idx;
  uint64_t generation;
};

// Single ring. Owned by RingTierManager; created from a RingDefinition
// at PM startup, destroyed at PM shutdown.
class Ring {
 public:
  // Definition copied from the static config. PM creates and owns
  // every slot's shm region; producers / consumers never call shm_open
  // for the writable side.
  struct Config {
    std::string                                      ring_id;
    uint32_t                                         n_slots;
    size_t                                           slot_size_bytes;
    std::string                                      shm_prefix; // e.g. "pm" -> slot names "/pm-ring-<id>-slot<i>"
    payload::manager::core::v1::RingExhaustionPolicy exhaustion_policy = payload::manager::core::v1::RING_EXHAUSTION_POLICY_DROP_NEW;
  };

  explicit Ring(Config cfg);
  ~Ring();

  Ring(const Ring&)            = delete;
  Ring& operator=(const Ring&) = delete;

  // Accessors safe to call without locking (immutable).
  const std::string& ring_id() const {
    return cfg_.ring_id;
  }
  uint32_t n_slots() const {
    return cfg_.n_slots;
  }
  size_t slot_size_bytes() const {
    return cfg_.slot_size_bytes;
  }

  // Snapshot the per-slot shm names, in slot_idx order. Used to
  // populate MapRingResponse. Immutable post-construction.
  std::vector<std::string> SlotShmNames() const;

  // --- Producer ---

  // Reserve the next available slot. Returns nullopt when every slot
  // is currently leased (refcount > 0) and the policy is DROP_NEW.
  // Caller surfaces RESOURCE_EXHAUSTED to the producer.
  std::optional<AcquireResult> Acquire();

  // Mark (slot_idx, generation) as committed. size_bytes is what
  // the producer actually wrote (<= slot_size_bytes). Returns false
  // if the (slot, generation) doesn't match an outstanding Acquire
  // (double-commit, stale handle).
  bool Commit(uint32_t slot_idx, uint64_t generation, uint64_t size_bytes);

  // --- Consumer ---

  // Look up (slot_idx, generation) and bump refcount. Returns
  // nullopt if the slot's generation no longer matches (consumer was
  // slow; producer has since recycled the slot). Caller surfaces
  // FAILED_PRECONDITION.
  std::optional<LeaseGrant> Lease(uint32_t slot_idx, uint64_t generation);

  // Drop a lease on (slot_idx, generation). Idempotent w.r.t. wrong
  // (slot, generation) — silently no-ops if the lease was for a
  // generation that's since advanced (already released by everyone
  // else). Caller's lease_table guarantees no double-release of the
  // same lease_id.
  void Release(uint32_t slot_idx, uint64_t generation);

  // Diagnostics: snapshot of per-slot {status, generation, refcount,
  // size_bytes}. Useful for /admin endpoints, metrics, tests.
  struct SlotSnapshot {
    uint32_t   slot_idx;
    SlotStatus status;
    uint64_t   generation;
    uint32_t   refcount;
    uint64_t   size_bytes;
  };
  std::vector<SlotSnapshot> Snapshot() const;

 private:
  // Find the next slot eligible for Acquire. Caller must hold mu_.
  // Searches round-robin from acquire_cursor_; returns nullptr if
  // every slot has refcount > 0 (or is WRITING).
  Slot* PickAcquireSlot_();

  const Config      cfg_;
  std::vector<Slot> slots_;
  uint32_t          acquire_cursor_ = 0; // round-robin starting index for next Acquire

  mutable std::mutex mu_;
  // condition_variable cv_;  // For RING_EXHAUSTION_POLICY_BLOCK (v2)
};

} // namespace payload::ring

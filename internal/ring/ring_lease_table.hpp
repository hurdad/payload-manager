#pragma once

// Server-side ring lease table. Opaque lease_ids issued by
// LeaseRingSlot map back to (ring_id, slot_idx, generation) so
// ReleaseRingSlot can decrement the right slot's refcount without
// trusting the client to round-trip those fields.
//
// Lease_ids are 16-byte randoms (UUID-ish). The map is keyed by the
// raw bytes; one global mutex (lease churn is one per consumer per
// capture cycle, so contention is low). If profiling later shows
// this lock matters, shard by hash bucket.

#include <array>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace payload::ring {

using LeaseId = std::array<uint8_t, 16>;

struct LeaseRecord {
  std::string ring_id;
  uint32_t    slot_idx;
  uint64_t    generation;
  // When LeaseRingSlot granted this lease. A consumer that dies before
  // ReleaseRingSlot leaves the slot's refcount up forever, so the ring
  // ages leases out from here. Steady clock: this measures a duration
  // held, and must not move when the wall clock does.
  std::chrono::steady_clock::time_point granted_at{};
};

// Hash/eq for the 16-byte LeaseId. Must be defined before the
// unordered_map<...> template-arg site below — forward declarations
// leave the types incomplete and unordered_map's default ctor gets
// deleted under GCC 13's _Hashtable.
struct LeaseIdHash {
  size_t operator()(const LeaseId& id) const noexcept;
};
struct LeaseIdEq {
  bool operator()(const LeaseId& a, const LeaseId& b) const noexcept {
    return a == b;
  }
};

class RingLeaseTable {
 public:
  RingLeaseTable();
  ~RingLeaseTable() = default;

  RingLeaseTable(const RingLeaseTable&)            = delete;
  RingLeaseTable& operator=(const RingLeaseTable&) = delete;

  // Mint a new lease_id and record (ring_id, slot_idx, generation)
  // against it. Caller has already bumped the slot's refcount.
  LeaseId Insert(std::string ring_id, uint32_t slot_idx, uint64_t generation);

  // Look up + remove. Returns nullopt if the lease_id is unknown
  // (already released, or never existed). ReleaseRingSlot is
  // idempotent at the protocol level so unknown lease_ids return
  // OK; the caller skips the Ring::Release call when this returns
  // nullopt.
  std::optional<LeaseRecord> Remove(const LeaseId& id);

  // Remove every lease on `ring_id` granted at or before `cutoff`, and
  // return them so the caller can drop the matching slot refcounts.
  //
  // Scoped to one ring because the only caller is that ring's exhausted
  // AcquireRingSlot path: it reclaims exactly the leases standing in the
  // way, and leaves an unrelated ring's slow consumers alone. The table
  // is flat, so this is a full scan — outstanding leases are bounded by
  // (slots x consumers), i.e. hundreds, and this only runs on an
  // exhausted ring.
  std::vector<LeaseRecord> RemoveExpiredForRing(const std::string& ring_id, std::chrono::steady_clock::time_point cutoff);

  // Convenience overloads for wire encoding (bytes <-> LeaseId).
  // The proto carries `bytes lease_id = 1;` so callers see raw
  // strings; these helpers wrap the size check.
  static std::optional<LeaseId> FromBytes(const std::string& bytes);
  static std::string            ToBytes(const LeaseId& id);

  // Diagnostics. Snapshot of current outstanding lease count.
  size_t Size() const;

 private:
  LeaseId NewLeaseId_();

  mutable std::mutex                                               mu_;
  std::unordered_map<LeaseId, LeaseRecord, LeaseIdHash, LeaseIdEq> table_;
};

} // namespace payload::ring

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
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace payload::ring {

using LeaseId = std::array<uint8_t, 16>;

struct LeaseRecord {
  std::string ring_id;
  uint32_t    slot_idx;
  uint64_t    generation;
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

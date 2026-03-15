#pragma once

#include <chrono>
#include <cstdint>
#include <memory>

#include "lease.hpp"
#include "lease_table.hpp"
#include "payload/manager/v1.hpp"

namespace payload::lease {

class LeaseManager {
 public:
  // default_lease_ms: duration used when the caller passes min_duration_ms=0.
  // max_lease_ms:     upper bound clamped regardless of what the caller requests (0 = no cap).
  explicit LeaseManager(uint64_t default_lease_ms = 20'000, uint64_t max_lease_ms = 120'000);

  Lease Acquire(const payload::manager::v1::PayloadID& id, const payload::manager::v1::PayloadDescriptor& payload_descriptor,
                uint64_t min_duration_ms);

  void Release(const payload::manager::v1::LeaseID& lease_id);

  bool     HasActiveLeases(const payload::manager::v1::PayloadID& id);
  uint32_t CountActiveLeases(const payload::manager::v1::PayloadID& id);

  void InvalidateAll(const payload::manager::v1::PayloadID& id);

 private:
  LeaseTable table_;
  uint64_t   default_lease_ms_;
  uint64_t   max_lease_ms_;

  static payload::manager::v1::LeaseID GenerateLeaseID();
};

} // namespace payload::lease

#include "lease_manager.hpp"

#include "internal/util/uuid.hpp"
#include "payload/manager/v1.hpp"

namespace payload::lease {

LeaseManager::LeaseManager(uint64_t default_lease_ms, uint64_t max_lease_ms) : default_lease_ms_(default_lease_ms), max_lease_ms_(max_lease_ms) {
}

payload::manager::v1::LeaseID LeaseManager::GenerateLeaseID() {
  return payload::util::ToLeaseProto(payload::util::GenerateUUID());
}

Lease LeaseManager::Acquire(const payload::manager::v1::PayloadID& id, const payload::manager::v1::PayloadDescriptor& payload_descriptor,
                            uint64_t min_duration_ms) {
  // Apply default: a caller that passes 0 gets the configured default duration.
  uint64_t duration_ms = (min_duration_ms == 0) ? default_lease_ms_ : min_duration_ms;

  // Apply max cap: clamp if a non-zero ceiling is configured.
  if (max_lease_ms_ > 0 && duration_ms > max_lease_ms_) {
    duration_ms = max_lease_ms_;
  }

  Lease lease;
  lease.lease_id           = GenerateLeaseID();
  lease.payload_id         = id;
  lease.payload_descriptor = payload_descriptor;
  lease.expires_at         = std::chrono::system_clock::now() + std::chrono::milliseconds(duration_ms);

  return table_.Insert(lease);
}

void LeaseManager::Release(const payload::manager::v1::LeaseID& lease_id) {
  table_.Remove(lease_id);
}

bool LeaseManager::HasActiveLeases(const payload::manager::v1::PayloadID& id) {
  return table_.HasActive(id);
}

uint32_t LeaseManager::CountActiveLeases(const payload::manager::v1::PayloadID& id) {
  return table_.CountActive(id);
}

void LeaseManager::InvalidateAll(const payload::manager::v1::PayloadID& id) {
  table_.RemoveAll(id);
}

bool LeaseManager::WaitUntilNoLeases(const payload::manager::v1::PayloadID& id, std::chrono::steady_clock::time_point deadline) {
  return table_.WaitUntilNoLeases(id, deadline);
}

} // namespace payload::lease

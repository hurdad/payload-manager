#include "internal/lease/lease_manager.hpp"
#include <random>

namespace payload::lease {

LeaseManager::LeaseManager() = default;

static std::string RandomID() {
  static thread_local std::mt19937_64 rng{std::random_device{}()};
  return std::to_string(rng());
}

std::string LeaseManager::GenerateLeaseID() {
  return RandomID();
}

Lease LeaseManager::Acquire(const payload::manager::v1::PayloadID& id,
                            const payload::manager::v1::Placement& placement,
                            uint64_t min_duration_ms) {

  Lease lease;
  lease.lease_id = GenerateLeaseID();
  lease.payload_id = id;
  lease.placement = placement;
  lease.expires_at = std::chrono::system_clock::now()
      + std::chrono::milliseconds(min_duration_ms);

  return table_.Insert(lease);
}

void LeaseManager::Release(const std::string& lease_id) {
  table_.Remove(lease_id);
}

bool LeaseManager::HasActiveLeases(const payload::manager::v1::PayloadID& id) {
  return table_.HasActive(id);
}

void LeaseManager::InvalidateAll(const payload::manager::v1::PayloadID& id) {
  table_.RemoveAll(id);
}

}

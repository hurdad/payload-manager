#pragma once

#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "lease.hpp"
#include "payload/manager/v1.hpp"

namespace payload::lease {

class LeaseTable {
 public:
  Lease Insert(const Lease& lease);

  void Remove(const payload::manager::v1::LeaseID& lease_id);

  bool     HasActive(const payload::manager::v1::PayloadID& id);
  uint32_t CountActive(const payload::manager::v1::PayloadID& id);

  // Mark all active leases for the payload as invalidated so that HasActive
  // returns false immediately, but keep them in the table until the lease
  // holders call Remove(). This allows WaitUntilNoLeases to detect when all
  // in-flight lease holders have finished and called ReleaseLease.
  void RemoveAll(const payload::manager::v1::PayloadID& id);

  // Block until all leases (active and invalidated) for the given payload have
  // been explicitly released via Remove(), or the deadline is reached.
  // Returns true on success (no leases remain), false on timeout.
  bool WaitUntilNoLeases(const payload::manager::v1::PayloadID& id, std::chrono::steady_clock::time_point deadline);

 private:
  using Clock = std::chrono::system_clock;

  std::mutex              mutex_;
  std::condition_variable release_cv_;

  std::unordered_map<std::string, Lease>            leases_;
  std::unordered_multimap<std::string, std::string> by_payload_;
  // Lease IDs that have been invalidated by RemoveAll but not yet Released.
  std::unordered_set<std::string> invalidated_leases_;

  static std::string Key(const payload::manager::v1::PayloadID& id);
  static std::string Key(const payload::manager::v1::LeaseID& id);
  static bool        IsExpired(const Lease& lease, Clock::time_point now);
  bool               HasActiveLocked(const std::string& payload_key, Clock::time_point now);
  bool               HasAnyLocked(const std::string& payload_key) const;
};

} // namespace payload::lease

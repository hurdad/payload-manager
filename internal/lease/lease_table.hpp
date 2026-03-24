#pragma once

#include <condition_variable>
#include <mutex>
#include <unordered_map>
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

  void RemoveAll(const payload::manager::v1::PayloadID& id);

  // Block until no active leases remain for the given payload, or the deadline
  // is reached. Returns true if the wait succeeded (no active leases), false on
  // timeout.
  bool WaitUntilNoLeases(const payload::manager::v1::PayloadID& id,
                         std::chrono::steady_clock::time_point  deadline);

 private:
  using Clock = std::chrono::system_clock;

  std::mutex              mutex_;
  std::condition_variable release_cv_;

  std::unordered_map<std::string, Lease>            leases_;
  std::unordered_multimap<std::string, std::string> by_payload_;

  static std::string Key(const payload::manager::v1::PayloadID& id);
  static std::string Key(const payload::manager::v1::LeaseID& id);
  static bool        IsExpired(const Lease& lease, Clock::time_point now);
  bool               HasActiveLocked(const std::string& payload_key, Clock::time_point now);
};

} // namespace payload::lease

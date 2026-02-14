#pragma once

#include <mutex>
#include <unordered_map>
#include <vector>

#include "lease.hpp"
#include "payload/manager/v1.hpp"

namespace payload::lease {

class LeaseTable {
 public:
  Lease Insert(const Lease& lease);

  void Remove(const std::string& lease_id);

  bool HasActive(const payload::manager::v1::PayloadID& id);

  void RemoveAll(const payload::manager::v1::PayloadID& id);

 private:
  using Clock = std::chrono::system_clock;

  std::mutex mutex_;

  std::unordered_map<std::string, Lease>            leases_;
  std::unordered_multimap<std::string, std::string> by_payload_;

  static std::string Key(const payload::manager::v1::PayloadID& id);
  static bool        IsExpired(const Lease& lease, Clock::time_point now);
};

} // namespace payload::lease

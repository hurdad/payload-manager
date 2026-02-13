#pragma once

#include <unordered_map>
#include <mutex>
#include <vector>

#include "lease.hpp"

namespace payload::lease {

class LeaseTable {
public:
  Lease Insert(const Lease& lease);

  void Remove(const std::string& lease_id);

  bool HasActive(const payload::manager::v1::PayloadID& id);

  void RemoveAll(const payload::manager::v1::PayloadID& id);

private:
  std::mutex mutex_;

  std::unordered_map<std::string, Lease> leases_;
  std::unordered_multimap<std::string, std::string> by_payload_;

  static std::string Key(const payload::manager::v1::PayloadID& id);
};

}

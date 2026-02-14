#pragma once

#include <chrono>
#include <memory>

#include "lease.hpp"
#include "lease_table.hpp"
#include "payload/manager/v1.hpp"

namespace payload::lease {

class LeaseManager {
 public:
  LeaseManager();

  Lease Acquire(const payload::manager::v1::PayloadID& id, const payload::manager::v1::PayloadDescriptor& payload_descriptor,
                uint64_t min_duration_ms);

  void Release(const std::string& lease_id);

  bool HasActiveLeases(const payload::manager::v1::PayloadID& id);

  void InvalidateAll(const payload::manager::v1::PayloadID& id);

 private:
  LeaseTable table_;

  static std::string GenerateLeaseID();
};

} // namespace payload::lease

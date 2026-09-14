#pragma once

// Service-layer adapter for the TIER_RAM_RING tier. Translates from
// the PayloadRingService proto messages to RingTierManager +
// RingLeaseTable calls, throwing util::* error types for non-OK
// outcomes (grpc/ring_server.cpp maps those to gRPC statuses via the
// existing ToStatus helper).

#include <memory>

#include "google/protobuf/empty.pb.h"
#include "internal/ring/ring_lease_table.hpp"
#include "internal/ring/ring_tier_manager.hpp"
#include "payload/manager/runtime/v1/ring.pb.h"

namespace payload::service {

class RingService {
 public:
  // ring_mgr + lease_table are owned by factory::Build; the service
  // holds non-owning pointers and outlives neither. Both arguments
  // are required (no per-call null checks).
  RingService(payload::ring::RingTierManager* ring_mgr, payload::ring::RingLeaseTable* lease_table);

  // --- Producer ---

  payload::manager::runtime::v1::AcquireRingSlotResponse AcquireRingSlot(const payload::manager::runtime::v1::AcquireRingSlotRequest& req);

  payload::manager::runtime::v1::CommitRingSlotResponse CommitRingSlot(const payload::manager::runtime::v1::CommitRingSlotRequest& req);

  // --- Consumer ---

  payload::manager::runtime::v1::MapRingResponse MapRing(const payload::manager::runtime::v1::MapRingRequest& req);

  payload::manager::runtime::v1::LeaseRingSlotResponse LeaseRingSlot(const payload::manager::runtime::v1::LeaseRingSlotRequest& req);

  void ReleaseRingSlot(const payload::manager::runtime::v1::ReleaseRingSlotRequest& req);

 private:
  payload::ring::RingTierManager* ring_mgr_;
  payload::ring::RingLeaseTable*  lease_table_;
};

} // namespace payload::service

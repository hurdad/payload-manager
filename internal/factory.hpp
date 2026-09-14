#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <vector>

#include "config/config.pb.h"
#include "internal/ring/ring_lease_table.hpp"
#include "internal/ring/ring_tier_manager.hpp"

namespace payload::factory {

struct Application {
  std::vector<std::unique_ptr<grpc::Service>> grpc_services;
  std::vector<std::shared_ptr<void>>          background_workers;

  // TIER_RAM_RING state. Held here so it outlives the services that use it —
  // RingService holds non-owning pointers into both.
  std::unique_ptr<payload::ring::RingTierManager> ring_manager;
  std::shared_ptr<payload::ring::RingLeaseTable>  ring_lease_table;
};

Application Build(const payload::runtime::config::RuntimeConfig& config);

} // namespace payload::factory

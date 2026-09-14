#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <vector>

#include "config/config.pb.h"
#include "internal/ring/ring_lease_table.hpp"
#include "internal/ring/ring_tier_manager.hpp"

namespace payload::factory {

struct Application {
  // TIER_RAM_RING state. Declared first so it is destroyed last, and torn
  // down explicitly below so that stays true if anyone reorders these.
  std::unique_ptr<payload::ring::RingTierManager> ring_manager;
  std::shared_ptr<payload::ring::RingLeaseTable>  ring_lease_table;

  std::vector<std::unique_ptr<grpc::Service>> grpc_services;
  std::vector<std::shared_ptr<void>>          background_workers;

  Application()                                  = default;
  Application(Application&&) noexcept            = default;
  Application& operator=(Application&&) noexcept = default;
  Application(const Application&)                = delete;
  Application& operator=(const Application&)     = delete;

  ~Application() {
    // Teardown order is load-bearing, so spell it out rather than leaving
    // it to member declaration order. RingMetricsPublisher lives in
    // background_workers and runs a thread that dereferences ring_manager
    // until its destructor joins; RingService lives in grpc_services and
    // holds raw pointers into both ring members. Destroy the users before
    // the things they point into.
    background_workers.clear();
    grpc_services.clear();
    ring_lease_table.reset();
    ring_manager.reset();
  }
};

Application Build(const payload::runtime::config::RuntimeConfig& config);

} // namespace payload::factory

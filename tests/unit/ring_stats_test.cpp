// Ring slot accounting as it reaches operators: Ring::GetStats, the
// gauge publisher that samples it, and the admin Stats RPC that reports
// it.
//
// Ring slots are pre-allocated and recycled in place, so they show up in
// none of the payload counts or tier bytes the rest of Stats reports.
// These tests pin down the numbers that replace them — above all
// slots_available, which is what decides whether a producer's next
// capture lands or is dropped.

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "config/config.pb.h"
#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/factory.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/ring/ring_lease_table.hpp"
#include "internal/ring/ring_metrics_publisher.hpp"
#include "internal/ring/ring_tier_manager.hpp"
#include "internal/service/admin_service.hpp"
#include "internal/service/ring_service.hpp"
#include "internal/service/service_context.hpp"
#include "payload/manager/v1.hpp"

namespace payload {
namespace {

namespace pm_cfg     = payload::runtime::config;
namespace pm_core_v1 = payload::manager::core::v1;
namespace pm_rt_v1   = payload::manager::runtime::v1;

std::string StatsPrefix(const std::string& tag) {
  return "pm-ring-stats-test-" + std::to_string(getpid()) + "-" + tag;
}

pm_cfg::RingTierConfig MakeConfig(const std::string& tag, uint32_t n_slots, uint64_t slot_bytes = 4096) {
  pm_cfg::RingTierConfig cfg;
  auto*                  def = cfg.add_rings();
  def->set_ring_id("r");
  def->set_n_slots(n_slots);
  def->set_slot_size_bytes(slot_bytes);
  def->set_shm_prefix(StatsPrefix(tag));
  def->set_exhaustion_policy(pm_core_v1::RING_EXHAUSTION_POLICY_DROP_NEW);
  return cfg;
}

// Drives the real service so the accounting is exercised through the same
// transitions a producer and consumer would cause.
struct RingHarness {
  std::unique_ptr<ring::RingTierManager> mgr;
  ring::RingLeaseTable                   lease_table;
  std::unique_ptr<service::RingService>  svc;

  explicit RingHarness(const pm_cfg::RingTierConfig& cfg) : mgr(ring::RingTierManager::Build(cfg, "pm")) {
    svc = std::make_unique<service::RingService>(mgr.get(), &lease_table);
  }

  ring::Ring::Stats Stats() const {
    return mgr->GetRing("r")->GetStats();
  }

  pm_rt_v1::AcquireRingSlotResponse Acquire() {
    pm_rt_v1::AcquireRingSlotRequest req;
    req.set_ring_id("r");
    return svc->AcquireRingSlot(req);
  }

  void Commit(uint32_t slot_idx, uint64_t generation, uint64_t size_bytes = 0) {
    pm_rt_v1::CommitRingSlotRequest req;
    req.set_ring_id("r");
    req.set_slot_idx(slot_idx);
    req.set_generation(generation);
    req.set_size_bytes(size_bytes);
    svc->CommitRingSlot(req);
  }

  pm_rt_v1::LeaseRingSlotResponse Lease(uint32_t slot_idx, uint64_t generation) {
    pm_rt_v1::LeaseRingSlotRequest req;
    req.set_ring_id("r");
    req.set_slot_idx(slot_idx);
    req.set_generation(generation);
    return svc->LeaseRingSlot(req);
  }

  void Release(const std::string& lease_id) {
    pm_rt_v1::ReleaseRingSlotRequest req;
    req.set_lease_id(lease_id);
    svc->ReleaseRingSlot(req);
  }
};

} // namespace

// ---------------------------------------------------------------------------
// Ring::GetStats
// ---------------------------------------------------------------------------

TEST(RingStats, FreshRingIsEntirelyAvailable) {
  RingHarness h{MakeConfig("fresh", 4, 8192)};

  const auto s = h.Stats();
  EXPECT_EQ(s.slots_total, 4u);
  EXPECT_EQ(s.slots_available, 4u);
  EXPECT_EQ(s.slots_writing, 0u);
  EXPECT_EQ(s.slots_leased, 0u);
  EXPECT_EQ(s.leases_active, 0u);
  EXPECT_EQ(s.slots_reclaimed, 0u);
  EXPECT_EQ(s.slot_capacity_bytes, 8192u);
}

TEST(RingStats, AnAcquiredSlotIsWritingAndNotAvailable) {
  RingHarness h{MakeConfig("writing", 3)};

  const auto acq = h.Acquire();
  const auto s   = h.Stats();
  EXPECT_EQ(s.slots_writing, 1u);
  EXPECT_EQ(s.slots_available, 2u) << "a slot being written is not headroom";
  EXPECT_EQ(s.slots_leased, 0u);

  // Committing hands it straight back: readable with refcount 0 is
  // acquirable again.
  h.Commit(acq.slot_idx(), acq.generation());
  const auto after = h.Stats();
  EXPECT_EQ(after.slots_writing, 0u);
  EXPECT_EQ(after.slots_available, 3u);
}

TEST(RingStats, ALeasedSlotCountsAsLeasedAndNotAvailable) {
  RingHarness h{MakeConfig("leased", 3)};

  const auto acq = h.Acquire();
  h.Commit(acq.slot_idx(), acq.generation());
  const auto lease = h.Lease(acq.slot_idx(), acq.generation());

  const auto s = h.Stats();
  EXPECT_EQ(s.slots_leased, 1u);
  EXPECT_EQ(s.leases_active, 1u);
  EXPECT_EQ(s.slots_available, 2u);
  EXPECT_EQ(s.slots_writing, 0u);

  h.Release(lease.lease_id());
  const auto after = h.Stats();
  EXPECT_EQ(after.slots_leased, 0u);
  EXPECT_EQ(after.leases_active, 0u);
  EXPECT_EQ(after.slots_available, 3u);
}

TEST(RingStats, SeveralConsumersOnOneSlotRaiseLeasesActiveNotSlotsLeased) {
  // leases_active is the sum of refcounts, so the two diverge as soon as
  // more than one consumer reads the same capture — which is the normal
  // fan-out case, not an edge case.
  RingHarness h{MakeConfig("fanout", 2)};

  const auto acq = h.Acquire();
  h.Commit(acq.slot_idx(), acq.generation());
  const auto a = h.Lease(acq.slot_idx(), acq.generation());
  const auto b = h.Lease(acq.slot_idx(), acq.generation());
  const auto c = h.Lease(acq.slot_idx(), acq.generation());

  const auto s = h.Stats();
  EXPECT_EQ(s.slots_leased, 1u);
  EXPECT_EQ(s.leases_active, 3u);
  EXPECT_EQ(s.slots_available, 1u);

  // The slot only frees up when the last reader lets go.
  h.Release(a.lease_id());
  h.Release(b.lease_id());
  EXPECT_EQ(h.Stats().slots_available, 1u);
  h.Release(c.lease_id());
  EXPECT_EQ(h.Stats().slots_available, 2u);
}

TEST(RingStats, ExhaustionShowsAsZeroAvailable) {
  RingHarness h{MakeConfig("exhausted", 1)};

  const auto acq = h.Acquire();
  h.Commit(acq.slot_idx(), acq.generation());
  (void)h.Lease(acq.slot_idx(), acq.generation());

  EXPECT_EQ(h.Stats().slots_available, 0u) << "zero available is what makes the next AcquireRingSlot fail";
}

TEST(RingStats, ReclaimedCountTracksSlotsTakenBackFromDeadProducers) {
  auto  cfg = MakeConfig("reclaimed", 1);
  auto* def = cfg.mutable_rings(0);
  def->set_slot_write_timeout_ms(20);
  RingHarness h{cfg};

  (void)h.Acquire(); // producer dies mid-write
  EXPECT_EQ(h.Stats().slots_reclaimed, 0u);

  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  (void)h.Acquire(); // reclaims

  EXPECT_EQ(h.Stats().slots_reclaimed, 1u);
}

// ---------------------------------------------------------------------------
// RingMetricsPublisher
//
// Metrics::SetRingStats compiles to a no-op without OTEL, so what is
// worth pinning here is that sampling is safe to call and safe to shut
// down — the publisher holds a non-owning manager pointer and runs on its
// own thread.
// ---------------------------------------------------------------------------

TEST(RingMetricsPublisher, PublishOnceSamplesEveryConfiguredRing) {
  auto  cfg  = MakeConfig("publisher", 2);
  auto* def2 = cfg.add_rings();
  def2->set_ring_id("r2");
  def2->set_n_slots(3);
  def2->set_slot_size_bytes(4096);
  def2->set_shm_prefix(StatsPrefix("publisher-2"));

  auto                       mgr = ring::RingTierManager::Build(cfg, "pm");
  ring::RingMetricsPublisher publisher(mgr.get());
  EXPECT_NO_THROW(publisher.PublishOnce());
}

TEST(RingMetricsPublisher, IsInertWithoutARingManager) {
  ring::RingMetricsPublisher publisher(nullptr);
  EXPECT_NO_THROW(publisher.PublishOnce());
  EXPECT_NO_THROW(publisher.Start()); // nothing to sample: must not spawn a thread
  EXPECT_NO_THROW(publisher.Stop());
}

TEST(RingMetricsPublisher, StartsAndStopsCleanly) {
  auto                       cfg = MakeConfig("publisher-lifecycle", 2);
  auto                       mgr = ring::RingTierManager::Build(cfg, "pm");
  ring::RingMetricsPublisher publisher(mgr.get(), std::chrono::milliseconds(5));

  publisher.Start();
  publisher.Start(); // idempotent — must not spawn a second thread
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  publisher.Stop();
  publisher.Stop(); // idempotent
}

TEST(RingMetricsPublisher, DestructorStopsTheThread) {
  auto cfg = MakeConfig("publisher-dtor", 2);
  auto mgr = ring::RingTierManager::Build(cfg, "pm");
  {
    ring::RingMetricsPublisher publisher(mgr.get(), std::chrono::milliseconds(5));
    publisher.Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  } // must join here rather than detach onto a dangling manager
  SUCCEED();
}

TEST(RingMetricsPublisher, ApplicationTearsDownThePublisherBeforeTheRingManager) {
  // factory::Build leaves a running publisher in Application::background_workers
  // holding a raw pointer to Application::ring_manager. Member destruction runs
  // in reverse declaration order, so without Application's explicit teardown the
  // manager is freed while the publisher's thread is still sampling it — a
  // use-after-free on every shutdown with a ring tier configured. Reproduce the
  // exact ownership shape and destroy it.
  // Several rings and a zero interval keep the sampling thread inside the
  // manager essentially all the time, so the window between freeing it and
  // joining the thread is wide enough for a sanitizer to catch rather than
  // a microsecond that usually closes unobserved.
  pm_cfg::RingTierConfig cfg;
  for (int i = 0; i < 8; ++i) {
    auto* def = cfg.add_rings();
    def->set_ring_id("r" + std::to_string(i));
    def->set_n_slots(8);
    def->set_slot_size_bytes(256);
    def->set_shm_prefix(StatsPrefix("app-teardown-" + std::to_string(i)));
  }

  {
    factory::Application app;
    app.ring_manager = ring::RingTierManager::Build(cfg, "pm");

    {
      // Hand sole ownership to background_workers, as factory::Build does:
      // a local shared_ptr kept alive here would be destroyed before `app`
      // and would join the thread early, hiding the very race under test.
      auto publisher = std::make_shared<ring::RingMetricsPublisher>(app.ring_manager.get(), std::chrono::milliseconds(0));
      publisher->Start();
      app.background_workers.push_back(std::move(publisher));
    }

    // Let the sampling thread get well into its loop before teardown.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  SUCCEED();
}

// ---------------------------------------------------------------------------
// AdminService::Stats
// ---------------------------------------------------------------------------

namespace {

struct AdminHarness {
  std::shared_ptr<lease::LeaseManager>                   lease_mgr = std::make_shared<lease::LeaseManager>();
  std::shared_ptr<payload::db::memory::MemoryRepository> repo      = std::make_shared<payload::db::memory::MemoryRepository>();
  std::shared_ptr<core::PayloadManager>  manager{std::make_shared<core::PayloadManager>(storage::StorageFactory::TierMap{}, lease_mgr, repo)};
  std::unique_ptr<ring::RingTierManager> rings;
  std::unique_ptr<service::AdminService> admin;

  explicit AdminHarness(ring::RingTierManager* ring_mgr) {
    service::ServiceContext ctx;
    ctx.manager    = manager;
    ctx.repository = repo;
    ctx.ring_mgr   = ring_mgr;
    admin          = std::make_unique<service::AdminService>(ctx);
  }
};

} // namespace

TEST(AdminStats, ReportsPerRingSlotAccounting) {
  RingHarness  ring_h{MakeConfig("admin", 4, 2048)};
  AdminHarness h{ring_h.mgr.get()};

  const auto acq = ring_h.Acquire();
  ring_h.Commit(acq.slot_idx(), acq.generation());
  (void)ring_h.Lease(acq.slot_idx(), acq.generation());
  (void)ring_h.Acquire(); // a second slot left mid-write

  const auto resp = h.admin->Stats(payload::manager::v1::StatsRequest{});
  ASSERT_EQ(resp.rings_size(), 1);

  const auto& r = resp.rings(0);
  EXPECT_EQ(r.ring_id(), "r");
  EXPECT_EQ(r.slots_total(), 4u);
  EXPECT_EQ(r.slots_writing(), 1u);
  EXPECT_EQ(r.slots_leased(), 1u);
  EXPECT_EQ(r.leases_active(), 1u);
  EXPECT_EQ(r.slots_available(), 2u);
  EXPECT_EQ(r.slot_capacity_bytes(), 2048u);
  EXPECT_EQ(r.slots_reclaimed(), 0u);
}

TEST(AdminStats, ReportsEveryConfiguredRing) {
  auto  cfg  = MakeConfig("admin-multi", 2);
  auto* def2 = cfg.add_rings();
  def2->set_ring_id("r2");
  def2->set_n_slots(5);
  def2->set_slot_size_bytes(4096);
  def2->set_shm_prefix(StatsPrefix("admin-multi-2"));

  auto         mgr = ring::RingTierManager::Build(cfg, "pm");
  AdminHarness h{mgr.get()};

  const auto resp = h.admin->Stats(payload::manager::v1::StatsRequest{});
  ASSERT_EQ(resp.rings_size(), 2);

  std::unordered_map<std::string, uint32_t> totals;
  for (const auto& r : resp.rings()) totals[r.ring_id()] = r.slots_total();
  EXPECT_EQ(totals["r"], 2u);
  EXPECT_EQ(totals["r2"], 5u);
}

TEST(AdminStats, OmitsRingsWhenNoRingTierIsConfigured) {
  // The overwhelmingly common deployment has no ring tier at all; Stats
  // must not grow an empty section for it.
  AdminHarness h{nullptr};

  const auto resp = h.admin->Stats(payload::manager::v1::StatsRequest{});
  EXPECT_EQ(resp.rings_size(), 0);
}

} // namespace payload

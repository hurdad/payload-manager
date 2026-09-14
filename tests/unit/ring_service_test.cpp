// Service-layer tests for RingService + RingTierManager. Complements
// ring_tier_test.cpp (which covers the Ring + RingLeaseTable core
// directly). Together they exercise the full PM ring tier minus the
// gRPC adapter (which is a mechanical try/catch wrapper covered
// implicitly by every other service's gRPC test).

#include "internal/service/ring_service.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <string>

#include "config/config.pb.h"
#include "internal/ring/ring_lease_table.hpp"
#include "internal/ring/ring_tier_manager.hpp"
#include "internal/util/errors.hpp"
#include "payload/manager/v1.hpp"

namespace payload {
namespace {

namespace pm_cfg     = payload::runtime::config;
namespace pm_core_v1 = payload::manager::core::v1;
namespace pm_rt_v1   = payload::manager::runtime::v1;

std::string PidPrefix(const std::string& tag) {
  return "pm-svc-test-" + std::to_string(getpid()) + "-" + tag;
}

// Build a RingTierConfig with a single ring of N slots × bytes.
pm_cfg::RingTierConfig MakeRingConfig(const std::string& ring_id, uint32_t n_slots, uint64_t slot_bytes, const std::string& shm_prefix) {
  pm_cfg::RingTierConfig cfg;
  auto*                  def = cfg.add_rings();
  def->set_ring_id(ring_id);
  def->set_n_slots(n_slots);
  def->set_slot_size_bytes(slot_bytes);
  def->set_shm_prefix(shm_prefix);
  def->set_exhaustion_policy(pm_core_v1::RING_EXHAUSTION_POLICY_DROP_NEW);
  return cfg;
}

// ---------------------------------------------------------------------------
// RingTierManager
// ---------------------------------------------------------------------------

TEST(RingTierManager, BuildsConfiguredRingsAndLooksUpById) {
  auto cfg = MakeRingConfig("r1", 2, 4096, PidPrefix("multi-1"));
  // Add a second ring with different shape.
  auto* def2 = cfg.add_rings();
  def2->set_ring_id("r2");
  def2->set_n_slots(4);
  def2->set_slot_size_bytes(8192);
  def2->set_shm_prefix(PidPrefix("multi-2"));

  auto mgr = ring::RingTierManager::Build(cfg, "pm");
  EXPECT_EQ(mgr->Count(), 2u);

  auto* r1 = mgr->GetRing("r1");
  ASSERT_NE(r1, nullptr);
  EXPECT_EQ(r1->n_slots(), 2u);
  EXPECT_EQ(r1->slot_size_bytes(), 4096u);

  auto* r2 = mgr->GetRing("r2");
  ASSERT_NE(r2, nullptr);
  EXPECT_EQ(r2->n_slots(), 4u);
  EXPECT_EQ(r2->slot_size_bytes(), 8192u);

  EXPECT_EQ(mgr->GetRing("nonexistent"), nullptr);
}

TEST(RingTierManager, DuplicateRingIdRejected) {
  pm_cfg::RingTierConfig cfg;
  auto*                  d1 = cfg.add_rings();
  d1->set_ring_id("dup");
  d1->set_n_slots(2);
  d1->set_slot_size_bytes(4096);
  d1->set_shm_prefix(PidPrefix("dup-1"));
  auto* d2 = cfg.add_rings();
  d2->set_ring_id("dup");
  d2->set_n_slots(2);
  d2->set_slot_size_bytes(4096);
  d2->set_shm_prefix(PidPrefix("dup-2"));

  EXPECT_THROW(ring::RingTierManager::Build(cfg, "pm"), std::invalid_argument);
}

TEST(RingTierManager, EmptyRingIdRejected) {
  pm_cfg::RingTierConfig cfg;
  auto*                  d = cfg.add_rings();
  d->set_ring_id("");
  d->set_n_slots(1);
  d->set_slot_size_bytes(4096);

  EXPECT_THROW(ring::RingTierManager::Build(cfg, "pm"), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// RingService
// ---------------------------------------------------------------------------

struct ServiceFixture {
  std::unique_ptr<ring::RingTierManager> mgr;
  ring::RingLeaseTable                   lease_table;
  std::unique_ptr<service::RingService>  svc;

  ServiceFixture(const std::string& ring_id, uint32_t n_slots, uint64_t slot_bytes, const std::string& shm_tag) {
    auto cfg = MakeRingConfig(ring_id, n_slots, slot_bytes, PidPrefix(shm_tag));
    mgr      = ring::RingTierManager::Build(cfg, "pm");
    svc      = std::make_unique<service::RingService>(mgr.get(), &lease_table);
  }
};

TEST(RingService, AcquireCommitMapLeaseReleaseRoundTrip) {
  ServiceFixture f("svc-rt", 2, 4096, "rt");

  pm_rt_v1::AcquireRingSlotRequest acq_req;
  acq_req.set_ring_id("svc-rt");
  auto acq = f.svc->AcquireRingSlot(acq_req);
  EXPECT_GE(acq.slot_idx(), 0u);
  EXPECT_EQ(acq.generation(), 1u);
  EXPECT_EQ(acq.capacity_bytes(), 4096u);
  EXPECT_FALSE(acq.shm_name().empty());

  pm_rt_v1::CommitRingSlotRequest c_req;
  c_req.set_ring_id("svc-rt");
  c_req.set_slot_idx(acq.slot_idx());
  c_req.set_generation(acq.generation());
  c_req.set_size_bytes(1024);
  auto commit_resp = f.svc->CommitRingSlot(c_req);
  EXPECT_TRUE(commit_resp.has_committed_at());

  pm_rt_v1::MapRingRequest map_req;
  map_req.set_ring_id("svc-rt");
  auto map_resp = f.svc->MapRing(map_req);
  EXPECT_EQ(map_resp.n_slots(), 2u);
  EXPECT_EQ(map_resp.slot_capacity_bytes(), 4096u);
  ASSERT_EQ(map_resp.slot_shm_names_size(), 2);
  EXPECT_EQ(map_resp.backing_tier(), pm_core_v1::TIER_RAM_RING);

  pm_rt_v1::LeaseRingSlotRequest l_req;
  l_req.set_ring_id("svc-rt");
  l_req.set_slot_idx(acq.slot_idx());
  l_req.set_generation(acq.generation());
  auto lease_resp = f.svc->LeaseRingSlot(l_req);
  EXPECT_FALSE(lease_resp.lease_id().empty());
  EXPECT_EQ(lease_resp.slot_idx(), acq.slot_idx());
  EXPECT_EQ(lease_resp.generation(), acq.generation());
  EXPECT_EQ(f.lease_table.Size(), 1u);

  pm_rt_v1::ReleaseRingSlotRequest rel_req;
  rel_req.set_lease_id(lease_resp.lease_id());
  f.svc->ReleaseRingSlot(rel_req);
  EXPECT_EQ(f.lease_table.Size(), 0u);
}

TEST(RingService, MissingRingThrowsNotFound) {
  ServiceFixture f("only", 1, 4096, "missing");

  pm_rt_v1::AcquireRingSlotRequest req;
  req.set_ring_id("does-not-exist");
  EXPECT_THROW(f.svc->AcquireRingSlot(req), payload::util::NotFound);

  pm_rt_v1::MapRingRequest map_req;
  map_req.set_ring_id("does-not-exist");
  EXPECT_THROW(f.svc->MapRing(map_req), payload::util::NotFound);

  pm_rt_v1::LeaseRingSlotRequest lease_req;
  lease_req.set_ring_id("does-not-exist");
  lease_req.set_slot_idx(0);
  lease_req.set_generation(1);
  EXPECT_THROW(f.svc->LeaseRingSlot(lease_req), payload::util::NotFound);
}

TEST(RingService, AcquireExhaustionThrowsResourceExhausted) {
  ServiceFixture f("xhst", 1, 4096, "exhaust");

  // Acquire + commit + lease the one slot — now ring is exhausted.
  pm_rt_v1::AcquireRingSlotRequest acq_req;
  acq_req.set_ring_id("xhst");
  auto acq = f.svc->AcquireRingSlot(acq_req);

  pm_rt_v1::CommitRingSlotRequest c_req;
  c_req.set_ring_id("xhst");
  c_req.set_slot_idx(acq.slot_idx());
  c_req.set_generation(acq.generation());
  c_req.set_size_bytes(0);
  f.svc->CommitRingSlot(c_req);

  pm_rt_v1::LeaseRingSlotRequest l_req;
  l_req.set_ring_id("xhst");
  l_req.set_slot_idx(acq.slot_idx());
  l_req.set_generation(acq.generation());
  (void)f.svc->LeaseRingSlot(l_req);

  // Next Acquire should fail with ResourceExhausted (DROP_NEW policy).
  EXPECT_THROW(f.svc->AcquireRingSlot(acq_req), payload::util::ResourceExhausted);
}

TEST(RingService, CommitMismatchThrowsInvalidState) {
  ServiceFixture f("commitfail", 1, 4096, "commit");

  // Commit on a slot we never acquired — InvalidState.
  pm_rt_v1::CommitRingSlotRequest c_req;
  c_req.set_ring_id("commitfail");
  c_req.set_slot_idx(0);
  c_req.set_generation(99);
  c_req.set_size_bytes(0);
  EXPECT_THROW(f.svc->CommitRingSlot(c_req), payload::util::InvalidState);
}

TEST(RingService, LeaseStaleGenerationThrowsInvalidState) {
  ServiceFixture f("stale", 1, 4096, "stale");

  // First cycle: acquire + commit, but skip the lease.
  pm_rt_v1::AcquireRingSlotRequest acq_req;
  acq_req.set_ring_id("stale");
  auto                            a1 = f.svc->AcquireRingSlot(acq_req);
  pm_rt_v1::CommitRingSlotRequest c1;
  c1.set_ring_id("stale");
  c1.set_slot_idx(a1.slot_idx());
  c1.set_generation(a1.generation());
  c1.set_size_bytes(0);
  f.svc->CommitRingSlot(c1);
  // Producer rolls forward (refcount==0, so re-acquire bumps generation).
  auto a2 = f.svc->AcquireRingSlot(acq_req);
  EXPECT_EQ(a2.slot_idx(), a1.slot_idx());
  EXPECT_EQ(a2.generation(), a1.generation() + 1);

  // Stale consumer tries to lease the previous generation — refused.
  pm_rt_v1::LeaseRingSlotRequest l_req;
  l_req.set_ring_id("stale");
  l_req.set_slot_idx(a1.slot_idx());
  l_req.set_generation(a1.generation());
  EXPECT_THROW(f.svc->LeaseRingSlot(l_req), payload::util::InvalidState);
}

TEST(RingService, ReleaseUnknownLeaseIsNoOp) {
  ServiceFixture f("noop", 1, 4096, "noop");

  pm_rt_v1::ReleaseRingSlotRequest req;
  // Empty lease_id — silently OK.
  EXPECT_NO_THROW(f.svc->ReleaseRingSlot(req));

  // 16 random bytes that don't match any lease — also silently OK
  // (PM treats unknown lease_ids as already-released).
  req.set_lease_id(std::string(16, '\0'));
  EXPECT_NO_THROW(f.svc->ReleaseRingSlot(req));
}

TEST(RingService, MapRingShmNamesEncodeRingAndSlot) {
  ServiceFixture f("named", 3, 1024, "named");

  pm_rt_v1::MapRingRequest req;
  req.set_ring_id("named");
  auto resp = f.svc->MapRing(req);
  ASSERT_EQ(resp.slot_shm_names_size(), 3);
  // Names contain ring_id + slot index. Exact format is
  // /<prefix>-ring-<ring_id>-slot<i>; assert the ring_id substring is
  // present so the test stays robust to prefix changes.
  for (int i = 0; i < 3; ++i) {
    EXPECT_NE(resp.slot_shm_names(i).find("named"), std::string::npos) << "slot " << i << " name: " << resp.slot_shm_names(i);
    EXPECT_NE(resp.slot_shm_names(i).find("slot" + std::to_string(i)), std::string::npos) << "slot " << i << " name: " << resp.slot_shm_names(i);
  }
}

TEST(RingService, MultipleRingsLeaseStateIsIsolated) {
  pm_cfg::RingTierConfig cfg;
  for (int i = 0; i < 2; ++i) {
    auto* def = cfg.add_rings();
    def->set_ring_id("r" + std::to_string(i));
    def->set_n_slots(1);
    def->set_slot_size_bytes(4096);
    def->set_shm_prefix(PidPrefix("iso-" + std::to_string(i)));
  }
  auto                 mgr = ring::RingTierManager::Build(cfg, "pm");
  ring::RingLeaseTable lease_table;
  service::RingService svc(mgr.get(), &lease_table);

  // Exhaust r0; r1 should still be acquirable.
  pm_rt_v1::AcquireRingSlotRequest acq0;
  acq0.set_ring_id("r0");
  auto                            a0 = svc.AcquireRingSlot(acq0);
  pm_rt_v1::CommitRingSlotRequest c0;
  c0.set_ring_id("r0");
  c0.set_slot_idx(a0.slot_idx());
  c0.set_generation(a0.generation());
  c0.set_size_bytes(0);
  svc.CommitRingSlot(c0);
  pm_rt_v1::LeaseRingSlotRequest l0;
  l0.set_ring_id("r0");
  l0.set_slot_idx(a0.slot_idx());
  l0.set_generation(a0.generation());
  (void)svc.LeaseRingSlot(l0);
  EXPECT_THROW(svc.AcquireRingSlot(acq0), payload::util::ResourceExhausted);

  // r1 unaffected.
  pm_rt_v1::AcquireRingSlotRequest acq1;
  acq1.set_ring_id("r1");
  EXPECT_NO_THROW(svc.AcquireRingSlot(acq1));
}

} // namespace
} // namespace payload

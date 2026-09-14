// Service-layer tests for RingService + RingTierManager. Complements
// ring_tier_test.cpp (which covers the Ring + RingLeaseTable core
// directly). Together they exercise the full PM ring tier minus the
// gRPC adapter (which is a mechanical try/catch wrapper covered
// implicitly by every other service's gRPC test).

#include "internal/service/ring_service.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <string>
#include <thread>

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

// ---------------------------------------------------------------------------
// Reclaiming reservations whose owner died
//
// Neither WRITING nor a non-zero refcount clears on its own, so before
// these timeouts a producer or consumer that died mid-capture retired a
// slot for the life of the process. Both paths run only on an exhausted
// ring; the timeouts here are tiny so the tests stay fast.
// ---------------------------------------------------------------------------

namespace {

// A one-slot ring makes exhaustion exact: one abandoned reservation is
// the difference between a working ring and a dead one.
pm_cfg::RingTierConfig MakeReclaimConfig(const std::string& tag, uint64_t write_timeout_ms, uint64_t lease_ttl_ms, uint32_t n_slots = 1) {
  auto  cfg = MakeRingConfig("r", n_slots, 4096, PidPrefix(tag));
  auto* def = cfg.mutable_rings(0);
  def->set_slot_write_timeout_ms(write_timeout_ms);
  def->set_lease_ttl_ms(lease_ttl_ms);
  return cfg;
}

pm_rt_v1::AcquireRingSlotResponse AcquireOn(service::RingService& svc, const std::string& ring_id) {
  pm_rt_v1::AcquireRingSlotRequest req;
  req.set_ring_id(ring_id);
  return svc.AcquireRingSlot(req);
}

void CommitOn(service::RingService& svc, const std::string& ring_id, uint32_t slot_idx, uint64_t generation, uint64_t size_bytes = 0) {
  pm_rt_v1::CommitRingSlotRequest req;
  req.set_ring_id(ring_id);
  req.set_slot_idx(slot_idx);
  req.set_generation(generation);
  req.set_size_bytes(size_bytes);
  svc.CommitRingSlot(req);
}

pm_rt_v1::LeaseRingSlotResponse LeaseOn(service::RingService& svc, const std::string& ring_id, uint32_t slot_idx, uint64_t generation) {
  pm_rt_v1::LeaseRingSlotRequest req;
  req.set_ring_id(ring_id);
  req.set_slot_idx(slot_idx);
  req.set_generation(generation);
  return svc.LeaseRingSlot(req);
}

} // namespace

TEST(RingReclaim, TakesBackASlotFromAProducerThatNeverCommitted) {
  auto                 cfg = MakeReclaimConfig("reclaim-writing", /*write_timeout_ms=*/20, /*lease_ttl_ms=*/60'000);
  auto                 mgr = ring::RingTierManager::Build(cfg, "pm");
  ring::RingLeaseTable lease_table;
  service::RingService svc(mgr.get(), &lease_table);

  const auto abandoned = AcquireOn(svc, "r"); // producer dies here
  EXPECT_EQ(mgr->GetRing("r")->reclaimed_writing_slots(), 0u);

  std::this_thread::sleep_for(std::chrono::milliseconds(60));

  pm_rt_v1::AcquireRingSlotResponse after;
  ASSERT_NO_THROW(after = AcquireOn(svc, "r"));
  EXPECT_EQ(after.slot_idx(), abandoned.slot_idx());
  EXPECT_GT(after.generation(), abandoned.generation()) << "reclaim must bump the generation";
  EXPECT_EQ(mgr->GetRing("r")->reclaimed_writing_slots(), 1u);
}

TEST(RingReclaim, LateCommitFromTheReclaimedProducerIsRejected) {
  // The zombie producer wakes up and commits. Its bytes must not be
  // published over the capture the new owner is writing.
  auto                 cfg = MakeReclaimConfig("reclaim-late-commit", 20, 60'000);
  auto                 mgr = ring::RingTierManager::Build(cfg, "pm");
  ring::RingLeaseTable lease_table;
  service::RingService svc(mgr.get(), &lease_table);

  const auto abandoned = AcquireOn(svc, "r");
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  const auto reowned = AcquireOn(svc, "r");

  pm_rt_v1::CommitRingSlotRequest late;
  late.set_ring_id("r");
  late.set_slot_idx(abandoned.slot_idx());
  late.set_generation(abandoned.generation());
  late.set_size_bytes(128);
  EXPECT_THROW(svc.CommitRingSlot(late), payload::util::InvalidState);

  // The new owner still commits normally.
  EXPECT_NO_THROW(CommitOn(svc, "r", reowned.slot_idx(), reowned.generation(), 64));
  EXPECT_EQ(mgr->GetRing("r")->Snapshot()[0].size_bytes, 64u);
}

TEST(RingReclaim, LeavesASlotAloneUntilTheWriteTimeoutElapses) {
  // A producer that is merely slow must keep its slot: reclaiming early
  // would let the next producer write over bytes it is still filling in.
  auto                 cfg = MakeReclaimConfig("reclaim-not-yet", /*write_timeout_ms=*/60'000, 60'000);
  auto                 mgr = ring::RingTierManager::Build(cfg, "pm");
  ring::RingLeaseTable lease_table;
  service::RingService svc(mgr.get(), &lease_table);

  (void)AcquireOn(svc, "r");
  EXPECT_THROW(AcquireOn(svc, "r"), payload::util::ResourceExhausted);
  EXPECT_EQ(mgr->GetRing("r")->reclaimed_writing_slots(), 0u);
}

TEST(RingReclaim, TakesBackASlotFromAConsumerThatNeverReleased) {
  auto                 cfg = MakeReclaimConfig("reclaim-lease", 60'000, /*lease_ttl_ms=*/20);
  auto                 mgr = ring::RingTierManager::Build(cfg, "pm");
  ring::RingLeaseTable lease_table;
  service::RingService svc(mgr.get(), &lease_table);

  const auto acq = AcquireOn(svc, "r");
  CommitOn(svc, "r", acq.slot_idx(), acq.generation());
  (void)LeaseOn(svc, "r", acq.slot_idx(), acq.generation()); // consumer dies here
  EXPECT_EQ(lease_table.Size(), 1u);

  // Still inside the TTL: the slot stays pinned.
  EXPECT_THROW(AcquireOn(svc, "r"), payload::util::ResourceExhausted);

  std::this_thread::sleep_for(std::chrono::milliseconds(60));

  EXPECT_NO_THROW((void)AcquireOn(svc, "r"));
  EXPECT_EQ(lease_table.Size(), 0u) << "the expired lease record must be dropped too";
}

TEST(RingReclaim, ForcedLeaseExpiryDoesNotDoubleDecrementOnALateRelease) {
  // The "dead" consumer was alive after all and releases its lease. The
  // record is gone, so this must be a no-op rather than a second decref
  // that would steal the slot from whoever holds it now.
  auto                 cfg = MakeReclaimConfig("reclaim-late-release", 60'000, 20);
  auto                 mgr = ring::RingTierManager::Build(cfg, "pm");
  ring::RingLeaseTable lease_table;
  service::RingService svc(mgr.get(), &lease_table);

  const auto first = AcquireOn(svc, "r");
  CommitOn(svc, "r", first.slot_idx(), first.generation());
  const auto zombie_lease = LeaseOn(svc, "r", first.slot_idx(), first.generation());

  std::this_thread::sleep_for(std::chrono::milliseconds(60));

  // Reclaim, then set the slot up with a live lease from a new consumer.
  const auto second = AcquireOn(svc, "r");
  CommitOn(svc, "r", second.slot_idx(), second.generation());
  (void)LeaseOn(svc, "r", second.slot_idx(), second.generation());
  ASSERT_EQ(mgr->GetRing("r")->Snapshot()[0].refcount, 1u);

  pm_rt_v1::ReleaseRingSlotRequest late;
  late.set_lease_id(zombie_lease.lease_id());
  EXPECT_NO_THROW(svc.ReleaseRingSlot(late));

  EXPECT_EQ(mgr->GetRing("r")->Snapshot()[0].refcount, 1u) << "the late release stole the new consumer's lease";
}

TEST(RingReclaim, LeaseSweepIsScopedToTheExhaustedRing) {
  auto  cfg  = MakeReclaimConfig("reclaim-isolation", 60'000, 20);
  auto* def2 = cfg.add_rings();
  def2->set_ring_id("r2");
  def2->set_n_slots(1);
  def2->set_slot_size_bytes(4096);
  def2->set_shm_prefix(PidPrefix("reclaim-isolation-2"));
  def2->set_lease_ttl_ms(20);

  auto                 mgr = ring::RingTierManager::Build(cfg, "pm");
  ring::RingLeaseTable lease_table;
  service::RingService svc(mgr.get(), &lease_table);

  for (const auto& id : {std::string("r"), std::string("r2")}) {
    const auto acq = AcquireOn(svc, id);
    CommitOn(svc, id, acq.slot_idx(), acq.generation());
    (void)LeaseOn(svc, id, acq.slot_idx(), acq.generation());
  }
  ASSERT_EQ(lease_table.Size(), 2u);

  std::this_thread::sleep_for(std::chrono::milliseconds(60));

  // Acquiring on "r" must not sweep r2's lease out from under it.
  EXPECT_NO_THROW((void)AcquireOn(svc, "r"));
  EXPECT_EQ(lease_table.Size(), 1u);
  EXPECT_EQ(mgr->GetRing("r2")->Snapshot()[0].refcount, 1u);
}

TEST(RingReclaim, ConfigZeroSelectsTheDefaultsRatherThanDisablingReclaim) {
  // 0 on the wire must not mean "never reclaim" — that is the bug these
  // timeouts exist to close.
  auto cfg = MakeReclaimConfig("reclaim-defaults", 0, 0);
  auto mgr = ring::RingTierManager::Build(cfg, "pm");
  EXPECT_EQ(mgr->GetRing("r")->slot_write_timeout(), std::chrono::seconds(30));
  EXPECT_EQ(mgr->GetRing("r")->lease_ttl(), std::chrono::seconds(60));
}

TEST(RingReclaim, ConfiguredTimeoutsOverrideTheDefaults) {
  auto cfg = MakeReclaimConfig("reclaim-configured", 1234, 5678);
  auto mgr = ring::RingTierManager::Build(cfg, "pm");
  EXPECT_EQ(mgr->GetRing("r")->slot_write_timeout(), std::chrono::milliseconds(1234));
  EXPECT_EQ(mgr->GetRing("r")->lease_ttl(), std::chrono::milliseconds(5678));
}

} // namespace
} // namespace payload

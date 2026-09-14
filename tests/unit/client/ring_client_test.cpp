// Client-side TIER_RAM_RING helper tests (client/cpp/ring.h).
//
// These drive the real RingConsumer / RingProducer against a real
// PayloadRingService over an in-process gRPC channel rather than a mock:
// the behaviour worth pinning down here is the interaction between the
// client handles and PM's slot state machine (an uncommitted slot stays
// WRITING forever, a held lease keeps refcount above zero), and a mock
// of the server would just re-assert the client's own assumptions.

#include <grpcpp/channel.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "client/cpp/client.h"
#include "client/cpp/ring.h"
#include "config/config.pb.h"
#include "internal/grpc/ring_server.hpp"
#include "internal/ring/ring_lease_table.hpp"
#include "internal/ring/ring_tier_manager.hpp"
#include "internal/service/ring_service.hpp"

namespace {

namespace pm_cfg  = payload::runtime::config;
namespace core_v1 = payload::manager::core::v1;

using payload::manager::client::PayloadClient;
using payload::manager::client::RingConsumer;
using payload::manager::client::RingProducer;

constexpr const char* kRingId = "test_ring";

// Unique per process so concurrent test binaries don't collide in /dev/shm.
std::string ShmPrefix(const std::string& tag) {
  return "pm-ring-client-test-" + std::to_string(getpid()) + "-" + tag;
}

// Spins up RingTierManager + RingService + RingServer behind an
// in-process channel, and hands out a PayloadClient pointed at it.
class RingClientTest : public ::testing::Test {
 protected:
  void Start(const std::string& tag, uint32_t n_slots, uint64_t slot_bytes) {
    pm_cfg::RingTierConfig cfg;
    auto*                  def = cfg.add_rings();
    def->set_ring_id(kRingId);
    def->set_n_slots(n_slots);
    def->set_slot_size_bytes(slot_bytes);
    def->set_shm_prefix(ShmPrefix(tag));
    def->set_exhaustion_policy(core_v1::RING_EXHAUSTION_POLICY_DROP_NEW);

    mgr_         = payload::ring::RingTierManager::Build(cfg, "pm");
    lease_table_ = std::make_unique<payload::ring::RingLeaseTable>();
    svc_         = std::make_shared<payload::service::RingService>(mgr_.get(), lease_table_.get());
    server_impl_ = std::make_unique<payload::grpc::RingServer>(svc_);

    ::grpc::ServerBuilder builder;
    builder.RegisterService(server_impl_.get());
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);

    client_ = std::make_unique<PayloadClient>(server_->InProcessChannel(::grpc::ChannelArguments()));
  }

  void TearDown() override {
    client_.reset();
    if (server_) {
      server_->Shutdown();
      server_->Wait();
    }
    producer_.reset(); // holds mappings and a client pointer; must go first
    server_impl_.reset();
    svc_.reset();
    lease_table_.reset();
    mgr_.reset();
  }

  RingConsumer MakeConsumer() {
    return RingConsumer(client_.get(), RingConsumer::Options{.log_prefix = "test"});
  }

  /// The producer caches slot mappings, so tests share one per fixture the
  /// way a real pipeline would rather than building a mapping per capture.
  RingProducer* Producer() {
    if (!producer_) {
      producer_ = std::make_unique<RingProducer>(client_.get(), RingProducer::Options{.log_prefix = "test"});
    }
    return producer_.get();
  }

  std::unique_ptr<payload::ring::RingTierManager> mgr_;
  std::unique_ptr<payload::ring::RingLeaseTable>  lease_table_;
  std::shared_ptr<payload::service::RingService>  svc_;
  std::unique_ptr<payload::grpc::RingServer>      server_impl_;
  std::unique_ptr<::grpc::Server>                 server_;
  std::unique_ptr<PayloadClient>                  client_;
  std::unique_ptr<RingProducer>                   producer_;
};

// ---------------------------------------------------------------------------
// Round trip
// ---------------------------------------------------------------------------

TEST_F(RingClientTest, ProducerWritesAndConsumerReadsTheSameBytes) {
  Start("roundtrip", 4, 4096);

  const std::string    payload = "hello ring tier";
  core_v1::RingSlotRef ref;
  {
    auto slot = Producer()->Acquire(kRingId);
    ASSERT_TRUE(slot.valid());
    EXPECT_EQ(slot.Append(payload.data(), payload.size()), payload.size());
    ASSERT_TRUE(slot.Commit(&ref));
  }

  EXPECT_EQ(ref.ring_id(), kRingId);
  EXPECT_EQ(ref.size_bytes(), payload.size());

  auto consumer = MakeConsumer();
  auto lease    = consumer.LeaseAndOpen(ref.ring_id(), ref.slot_idx(), ref.generation(), ref.size_bytes());
  ASSERT_TRUE(lease.has_value());
  ASSERT_NE(lease->host_va, nullptr);
  EXPECT_EQ(lease->size_bytes, payload.size());
  EXPECT_EQ(std::memcmp(lease->host_va, payload.data(), payload.size()), 0);
  EXPECT_FALSE(lease->lease_id().empty());
  // dev_va is only populated in a CUDA build with register_for_gpu.
  EXPECT_EQ(lease->dev_va, nullptr);
}

TEST_F(RingClientTest, AppendAccumulatesAcrossCalls) {
  Start("append-accum", 2, 4096);

  auto slot = Producer()->Acquire(kRingId);
  ASSERT_TRUE(slot.valid());
  EXPECT_EQ(slot.Append("abc", 3), 3u);
  EXPECT_EQ(slot.Append("def", 3), 3u);
  EXPECT_EQ(slot.size(), 6u);

  core_v1::RingSlotRef ref;
  ASSERT_TRUE(slot.Commit(&ref));
  EXPECT_EQ(ref.size_bytes(), 6u);

  auto consumer = MakeConsumer();
  auto lease    = consumer.LeaseAndOpen(kRingId, ref.slot_idx(), ref.generation(), ref.size_bytes());
  ASSERT_TRUE(lease.has_value());
  EXPECT_EQ(std::memcmp(lease->host_va, "abcdef", 6), 0);
}

TEST_F(RingClientTest, AppendTruncatesAtSlotCapacity) {
  Start("truncate", 1, 8);

  auto slot = Producer()->Acquire(kRingId);
  ASSERT_TRUE(slot.valid());
  EXPECT_EQ(slot.capacity(), 8u);

  const std::string too_big(32, 'x');
  EXPECT_EQ(slot.Append(too_big.data(), too_big.size()), 8u);
  EXPECT_EQ(slot.size(), 8u);
  // Slot is full; a further append writes nothing.
  EXPECT_EQ(slot.Append("y", 1), 0u);
}

// ---------------------------------------------------------------------------
// Producer owns the reservation, not just the mapping
// ---------------------------------------------------------------------------

TEST_F(RingClientTest, AbandonedSlotIsReturnedToTheRing) {
  // The regression this guards: PM parks an acquired slot in WRITING and
  // has no reaper, so a handle destroyed without a commit used to retire
  // the slot for the life of the process. With one slot in the ring, one
  // abandoned capture would kill the ring outright.
  Start("abandon", 1, 1024);

  {
    auto slot = Producer()->Acquire(kRingId);
    ASSERT_TRUE(slot.valid());
    slot.Append("partial", 7);
    // No commit — scope exit must hand the slot back.
  }

  auto again = Producer()->Acquire(kRingId);
  EXPECT_TRUE(again.valid()) << "abandoned slot was never returned to the ring";
}

TEST_F(RingClientTest, AbandonedSlotIsPublishedEmptyNotPartial) {
  // The rescue commit must publish zero bytes: the caller never
  // announced this capture, so the partial bytes in the slot are not a
  // payload anyone should read.
  Start("abandon-empty", 1, 1024);

  uint32_t slot_idx   = 0;
  uint64_t generation = 0;
  {
    auto slot = Producer()->Acquire(kRingId);
    ASSERT_TRUE(slot.valid());
    slot.Append("partial", 7);
    slot_idx   = slot.slot_idx();
    generation = slot.generation();
  }

  auto consumer = MakeConsumer();
  auto lease    = consumer.LeaseAndOpen(kRingId, slot_idx, generation, 7);
  ASSERT_TRUE(lease.has_value()) << "rescue commit should leave the slot readable";
  EXPECT_EQ(lease->size_bytes, 7u) << "size_bytes is clamped to capacity, not to the committed size";

  // The committed size is what PM recorded, and that is what a real
  // consumer reads off the wire: zero.
  const auto snapshot = mgr_->GetRing(kRingId)->Snapshot();
  ASSERT_EQ(snapshot.size(), 1u);
  EXPECT_EQ(snapshot[0].size_bytes, 0u);
}

TEST_F(RingClientTest, RepeatedAbandonmentDoesNotDrainTheRing) {
  Start("abandon-many", 2, 256);

  for (int i = 0; i < 10; ++i) {
    auto slot = Producer()->Acquire(kRingId);
    ASSERT_TRUE(slot.valid()) << "ring drained after " << i << " abandoned captures";
  }
}

TEST_F(RingClientTest, MovedFromSlotDoesNotDoubleReturnTheReservation) {
  Start("move", 1, 256);

  core_v1::RingSlotRef ref;
  {
    auto slot = Producer()->Acquire(kRingId);
    ASSERT_TRUE(slot.valid());
    slot.Append("abc", 3);

    RingProducer::Slot moved = std::move(slot);
    ASSERT_TRUE(moved.valid());
    EXPECT_FALSE(slot.valid()); // NOLINT(bugprone-use-after-move) — checking the moved-from state
    ASSERT_TRUE(moved.Commit(&ref));
  }

  // The moved-from handle must not have issued a rescue commit of its
  // own: the slot's committed size is still the real one.
  const auto snapshot = mgr_->GetRing(kRingId)->Snapshot();
  ASSERT_EQ(snapshot.size(), 1u);
  EXPECT_EQ(snapshot[0].size_bytes, 3u);
  EXPECT_EQ(snapshot[0].generation, ref.generation());
}

TEST_F(RingClientTest, MoveAssignmentReturnsTheOverwrittenReservation) {
  Start("move-assign", 2, 256);

  auto first = Producer()->Acquire(kRingId);
  ASSERT_TRUE(first.valid());
  const uint32_t first_idx = first.slot_idx();

  auto second = Producer()->Acquire(kRingId);
  ASSERT_TRUE(second.valid());
  ASSERT_NE(second.slot_idx(), first_idx);

  // Overwriting `first` drops its reservation; it must be handed back
  // rather than stranded.
  first = std::move(second);
  ASSERT_TRUE(first.valid());

  const auto snapshot = mgr_->GetRing(kRingId)->Snapshot();
  EXPECT_NE(snapshot[first_idx].status, payload::ring::SlotStatus::kWriting) << "overwritten reservation was left in WRITING";
}

TEST_F(RingClientTest, DoubleCommitIsRefusedLocally) {
  Start("double-commit", 2, 256);

  auto slot = Producer()->Acquire(kRingId);
  ASSERT_TRUE(slot.valid());
  slot.Append("abc", 3);

  EXPECT_TRUE(slot.Commit(nullptr));
  EXPECT_FALSE(slot.Commit(nullptr));
}

TEST_F(RingClientTest, AppendAfterCommitIsIgnored) {
  Start("append-after-commit", 1, 256);

  auto slot = Producer()->Acquire(kRingId);
  ASSERT_TRUE(slot.valid());
  slot.Append("abc", 3);
  ASSERT_TRUE(slot.Commit(nullptr));

  // PM has published the slot and a consumer may be mid-read.
  EXPECT_EQ(slot.Append("def", 3), 0u);
  EXPECT_EQ(slot.size(), 3u);
}

// ---------------------------------------------------------------------------
// Consumer lease ownership
// ---------------------------------------------------------------------------

TEST_F(RingClientTest, LeaseBlocksReacquireUntilItGoesOutOfScope) {
  Start("lease-scope", 1, 256);

  core_v1::RingSlotRef ref;
  {
    auto slot = Producer()->Acquire(kRingId);
    ASSERT_TRUE(slot.valid());
    slot.Append("abc", 3);
    ASSERT_TRUE(slot.Commit(&ref));
  }

  auto consumer = MakeConsumer();
  {
    auto lease = consumer.LeaseAndOpen(kRingId, ref.slot_idx(), ref.generation(), ref.size_bytes());
    ASSERT_TRUE(lease.has_value());

    // The only slot is pinned by the live lease.
    auto blocked = Producer()->Acquire(kRingId);
    EXPECT_FALSE(blocked.valid());
  }

  // Lease destructor released it; the producer can recycle the slot again.
  auto after = Producer()->Acquire(kRingId);
  EXPECT_TRUE(after.valid()) << "lease was never released";
}

TEST_F(RingClientTest, ExplicitEarlyReleaseIsIdempotent) {
  Start("early-release", 1, 256);

  core_v1::RingSlotRef ref;
  {
    auto slot = Producer()->Acquire(kRingId);
    ASSERT_TRUE(slot.valid());
    ASSERT_TRUE(slot.Commit(&ref));
  }

  auto consumer = MakeConsumer();
  {
    auto lease = consumer.LeaseAndOpen(kRingId, ref.slot_idx(), ref.generation(), ref.size_bytes());
    ASSERT_TRUE(lease.has_value());
    lease->Release();
    EXPECT_TRUE(lease->lease_id().empty());
    lease->Release(); // no-op

    // Released early, so the slot is already acquirable inside the scope.
    auto reacquired = Producer()->Acquire(kRingId);
    EXPECT_TRUE(reacquired.valid());
  }
}

TEST_F(RingClientTest, MovedFromLeaseDoesNotReleaseEarly) {
  Start("lease-move", 1, 256);

  core_v1::RingSlotRef ref;
  {
    auto slot = Producer()->Acquire(kRingId);
    ASSERT_TRUE(slot.valid());
    ASSERT_TRUE(slot.Commit(&ref));
  }

  auto consumer = MakeConsumer();
  {
    auto lease = consumer.LeaseAndOpen(kRingId, ref.slot_idx(), ref.generation(), ref.size_bytes());
    ASSERT_TRUE(lease.has_value());

    RingConsumer::Lease moved = std::move(*lease);
    EXPECT_TRUE(lease->lease_id().empty()); // NOLINT(bugprone-use-after-move)
    EXPECT_FALSE(moved.lease_id().empty());

    // The moved-to lease still pins the slot.
    auto blocked = Producer()->Acquire(kRingId);
    EXPECT_FALSE(blocked.valid());
  }

  auto after = Producer()->Acquire(kRingId);
  EXPECT_TRUE(after.valid());
}

// ---------------------------------------------------------------------------
// Refusals and malformed input
// ---------------------------------------------------------------------------

TEST_F(RingClientTest, StaleGenerationIsRefusedNotFatal) {
  Start("stale", 1, 256);

  core_v1::RingSlotRef first;
  {
    auto slot = Producer()->Acquire(kRingId);
    ASSERT_TRUE(slot.valid());
    ASSERT_TRUE(slot.Commit(&first));
  }
  // Producer recycles the same slot before the consumer got to it.
  {
    auto slot = Producer()->Acquire(kRingId);
    ASSERT_TRUE(slot.valid());
    ASSERT_TRUE(slot.Commit(nullptr));
  }

  auto consumer = MakeConsumer();
  auto lease    = consumer.LeaseAndOpen(kRingId, first.slot_idx(), first.generation(), first.size_bytes());
  EXPECT_FALSE(lease.has_value());
}

TEST_F(RingClientTest, StaleGenerationSurfacesAsInvalidNotIoError) {
  // ring.cc keys its "expected drop" log level off this, so the mapping
  // is behaviour, not cosmetics.
  Start("stale-status", 1, 256);

  core_v1::RingSlotRef first;
  {
    auto slot = Producer()->Acquire(kRingId);
    ASSERT_TRUE(slot.valid());
    ASSERT_TRUE(slot.Commit(&first));
  }
  {
    auto slot = Producer()->Acquire(kRingId);
    ASSERT_TRUE(slot.valid());
    ASSERT_TRUE(slot.Commit(nullptr));
  }

  auto refused = client_->LeaseRingSlot(kRingId, first.slot_idx(), first.generation());
  ASSERT_FALSE(refused.ok());
  EXPECT_TRUE(refused.status().IsInvalid()) << refused.status().ToString();
}

TEST_F(RingClientTest, ExhaustedRingSurfacesAsCapacityError) {
  // RingProducer::Acquire documents that callers tell "ring full" from
  // a broken channel this way.
  Start("exhausted-status", 1, 256);

  core_v1::RingSlotRef ref;
  {
    auto slot = Producer()->Acquire(kRingId);
    ASSERT_TRUE(slot.valid());
    ASSERT_TRUE(slot.Commit(&ref));
  }

  auto consumer = MakeConsumer();
  auto lease    = consumer.LeaseAndOpen(kRingId, ref.slot_idx(), ref.generation(), ref.size_bytes());
  ASSERT_TRUE(lease.has_value());

  auto exhausted = client_->AcquireRingSlot(kRingId);
  ASSERT_FALSE(exhausted.ok());
  EXPECT_TRUE(exhausted.status().IsCapacityError()) << exhausted.status().ToString();
}

TEST_F(RingClientTest, OutOfRangeSlotIndexIsDropped) {
  Start("oob", 2, 256);

  auto consumer = MakeConsumer();
  ASSERT_TRUE(consumer.EnsureMapped(kRingId));
  EXPECT_FALSE(consumer.LeaseAndOpen(kRingId, 99, 1, 16).has_value());
}

TEST_F(RingClientTest, OversizedWireSizeIsClampedToSlotCapacity) {
  Start("clamp", 1, 64);

  core_v1::RingSlotRef ref;
  {
    auto slot = Producer()->Acquire(kRingId);
    ASSERT_TRUE(slot.valid());
    slot.Append("abc", 3);
    ASSERT_TRUE(slot.Commit(&ref));
  }

  auto consumer = MakeConsumer();
  // A corrupt or hostile event claims far more than the slot can hold.
  auto lease = consumer.LeaseAndOpen(kRingId, ref.slot_idx(), ref.generation(), 1ULL << 40);
  ASSERT_TRUE(lease.has_value());
  EXPECT_EQ(lease->size_bytes, 64u);
}

TEST_F(RingClientTest, UnknownRingFailsToMap) {
  Start("unknown", 1, 256);

  auto consumer = MakeConsumer();
  EXPECT_FALSE(consumer.EnsureMapped("no_such_ring"));
  EXPECT_FALSE(consumer.LeaseAndOpen("no_such_ring", 0, 1, 16).has_value());
}

TEST_F(RingClientTest, EnsureMappedIsIdempotent) {
  Start("idempotent", 3, 256);

  auto consumer = MakeConsumer();
  EXPECT_TRUE(consumer.EnsureMapped(kRingId));
  EXPECT_TRUE(consumer.EnsureMapped(kRingId));
  EXPECT_TRUE(consumer.EnsureMapped(kRingId));
}

TEST(RingConsumerNullClient, RingOpsNoOpWithoutAClient) {
  RingConsumer consumer(nullptr, RingConsumer::Options{.log_prefix = "test"});
  EXPECT_FALSE(consumer.EnsureMapped("anything"));
  EXPECT_FALSE(consumer.LeaseAndOpen("anything", 0, 1, 16).has_value());
  consumer.Release("some-lease-id"); // must not crash
}

// ---------------------------------------------------------------------------
// Producer mapping cache
//
// The producer maps a ring once and reuses it, rather than mapping and
// unmapping per capture. These cover that cache and the parity it gives
// RingConsumer; without them the reuse is invisible to the suite and a
// regression to per-capture mapping would still pass everything above.
// ---------------------------------------------------------------------------

TEST_F(RingClientTest, ProducerReusesOneMappingAcrossCaptures) {
  Start("producer-cache", 2, 256);

  // Same slot index, two captures. A producer that mapped per acquire would
  // hand back a fresh mmap each time and these addresses would differ.
  std::map<std::uint32_t, void*> first_seen;
  for (int i = 0; i < 6; ++i) {
    auto slot = Producer()->Acquire(kRingId);
    ASSERT_TRUE(slot.valid()) << "capture " << i;
    void* va = slot.data();
    ASSERT_NE(va, nullptr);

    auto [it, inserted] = first_seen.emplace(slot.slot_idx(), va);
    if (!inserted) {
      EXPECT_EQ(it->second, va) << "slot " << slot.slot_idx() << " was remapped on capture " << i;
    }
    ASSERT_TRUE(slot.Commit(nullptr));
  }
  EXPECT_EQ(first_seen.size(), 2u) << "expected both slots to come round";
}

TEST_F(RingClientTest, CommittedSlotDropsItsPointer) {
  Start("commit-drops-pointer", 1, 256);

  auto slot = Producer()->Acquire(kRingId);
  ASSERT_TRUE(slot.valid());
  ASSERT_NE(slot.data(), nullptr);

  ASSERT_TRUE(slot.Commit(nullptr));

  // The mapping is still alive — the producer owns it — but this handle no
  // longer points at it, so a late write through a stale handle faults
  // instead of quietly overwriting a slot a consumer is reading.
  EXPECT_EQ(slot.data(), nullptr);
  EXPECT_FALSE(slot.valid());
}

TEST_F(RingClientTest, ProducerEnsureMappedIsIdempotent) {
  Start("producer-idempotent", 3, 256);

  EXPECT_TRUE(Producer()->EnsureMapped(kRingId));
  EXPECT_TRUE(Producer()->EnsureMapped(kRingId));
  EXPECT_TRUE(Producer()->EnsureMapped(kRingId));

  // Still usable after the repeat calls.
  auto slot = Producer()->Acquire(kRingId);
  EXPECT_TRUE(slot.valid());
}

TEST_F(RingClientTest, ProducerUnknownRingFailsToMap) {
  Start("producer-unknown", 2, 256);

  EXPECT_FALSE(Producer()->EnsureMapped("no_such_ring"));
  // Acquire maps first, so an unmapped ring yields an invalid slot rather
  // than a reservation nobody can write to.
  EXPECT_FALSE(Producer()->Acquire("no_such_ring").valid());
}

TEST_F(RingClientTest, ProducerSlotSizeTracksAppends) {
  Start("producer-size", 1, 256);

  auto slot = Producer()->Acquire(kRingId);
  ASSERT_TRUE(slot.valid());
  EXPECT_EQ(slot.size(), 0u);
  EXPECT_EQ(slot.capacity(), 256u);

  slot.Append("abcd", 4);
  EXPECT_EQ(slot.size(), 4u);
  slot.Append("ef", 2);
  EXPECT_EQ(slot.size(), 6u);

  core_v1::RingSlotRef ref;
  ASSERT_TRUE(slot.Commit(&ref));
  EXPECT_EQ(ref.size_bytes(), 6u);
}

TEST(RingProducerNullClient, RingOpsNoOpWithoutAClient) {
  RingProducer producer(nullptr, RingProducer::Options{.log_prefix = "test"});
  EXPECT_FALSE(producer.EnsureMapped("anything"));
  EXPECT_FALSE(producer.Acquire("anything").valid());
}

} // namespace

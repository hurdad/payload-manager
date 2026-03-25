#include "internal/lease/lease_table.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

namespace {

using payload::lease::Lease;
using payload::lease::LeaseTable;
using payload::manager::v1::PayloadDescriptor;
using payload::manager::v1::PayloadID;

PayloadID MakePayloadID(const std::string& value) {
  PayloadID id;
  id.set_value(value);
  return id;
}

payload::manager::v1::LeaseID MakeLeaseID(const std::string& value) {
  payload::manager::v1::LeaseID id;
  id.set_value(value);
  return id;
}

Lease MakeLease(const std::string& lease_id, const PayloadID& payload_id, std::chrono::system_clock::time_point expires_at) {
  Lease lease;
  lease.lease_id           = MakeLeaseID(lease_id);
  lease.payload_id         = payload_id;
  lease.payload_descriptor = PayloadDescriptor{};
  lease.expires_at         = expires_at;
  return lease;
}

} // namespace

TEST(LeaseTable, ExpiredLeaseIsInactive) {
  LeaseTable table;
  const auto payload = MakePayloadID("payload-expired");

  table.Insert(MakeLease("lease-expired", payload, std::chrono::system_clock::now() - std::chrono::seconds(1)));

  EXPECT_FALSE(table.HasActive(payload));
}

TEST(LeaseTable, MixedExpiredAndActiveLeases) {
  LeaseTable table;
  const auto payload = MakePayloadID("payload-mixed");

  table.Insert(MakeLease("lease-expired", payload, std::chrono::system_clock::now() - std::chrono::seconds(1)));
  table.Insert(MakeLease("lease-active", payload, std::chrono::system_clock::now() + std::chrono::seconds(30)));

  EXPECT_TRUE(table.HasActive(payload));

  table.Remove(MakeLeaseID("lease-active"));
  EXPECT_FALSE(table.HasActive(payload));
}

TEST(LeaseTable, CountActiveReflectsOnlyNonExpiredNonInvalidated) {
  LeaseTable table;
  const auto payload = MakePayloadID("payload-count");

  // Start with no leases.
  EXPECT_EQ(table.CountActive(payload), 0u);

  // Add two active leases.
  table.Insert(MakeLease("active-1", payload, std::chrono::system_clock::now() + std::chrono::seconds(30)));
  table.Insert(MakeLease("active-2", payload, std::chrono::system_clock::now() + std::chrono::seconds(30)));
  EXPECT_EQ(table.CountActive(payload), 2u);

  // An expired lease must not count.
  table.Insert(MakeLease("expired-1", payload, std::chrono::system_clock::now() - std::chrono::seconds(1)));
  EXPECT_EQ(table.CountActive(payload), 2u) << "expired lease must not be counted";

  // Invalidating via RemoveAll must drop the count to zero: invalidated leases
  // are still "held" but must not appear as active.
  table.RemoveAll(payload);
  EXPECT_EQ(table.CountActive(payload), 0u) << "invalidated leases must not be counted as active";
}

// ---------------------------------------------------------------------------
// WaitUntilNoLeases — happy path: lease removed before deadline
// ---------------------------------------------------------------------------
TEST(LeaseTable, WaitUntilNoLeasesReturnsTrueWhenLeaseRemoved) {
  LeaseTable table;
  const auto payload   = MakePayloadID("payload-wait-ok");
  const auto lease_key = MakeLeaseID("wait-lease");

  table.Insert(MakeLease("wait-lease", payload, std::chrono::system_clock::now() + std::chrono::seconds(60)));

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);

  // Remove the lease from a background thread after a short delay.
  std::thread remover([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    table.Remove(lease_key);
  });

  EXPECT_TRUE(table.WaitUntilNoLeases(payload, deadline))
      << "WaitUntilNoLeases must return true when the lease is released before the deadline";

  remover.join();
}

// ---------------------------------------------------------------------------
// WaitUntilNoLeases — timeout path: deadline exceeded returns false
// ---------------------------------------------------------------------------
TEST(LeaseTable, WaitUntilNoLeasesReturnsFalseOnTimeout) {
  LeaseTable table;
  const auto payload = MakePayloadID("payload-wait-timeout");

  table.Insert(MakeLease("held-lease", payload, std::chrono::system_clock::now() + std::chrono::seconds(60)));

  // Deadline already in the past — must return false immediately.
  const auto past_deadline = std::chrono::steady_clock::now() - std::chrono::milliseconds(1);
  EXPECT_FALSE(table.WaitUntilNoLeases(payload, past_deadline))
      << "WaitUntilNoLeases must return false when the deadline has already passed";
}

// ---------------------------------------------------------------------------
// WaitUntilNoLeases — no leases returns true immediately
// ---------------------------------------------------------------------------
TEST(LeaseTable, WaitUntilNoLeasesReturnsTrueImmediatelyWhenEmpty) {
  LeaseTable table;
  const auto payload  = MakePayloadID("payload-wait-empty");
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);

  EXPECT_TRUE(table.WaitUntilNoLeases(payload, deadline))
      << "WaitUntilNoLeases must return true immediately when no leases exist";
}

// ---------------------------------------------------------------------------
// WaitUntilNoLeases — invalidated leases block until explicit Remove()
//
// RemoveAll marks leases as invalidated (HasActive returns false), but they
// remain in the table until the holder calls Remove().  WaitUntilNoLeases
// must wait for that explicit Remove() rather than returning immediately after
// RemoveAll.
// ---------------------------------------------------------------------------
TEST(LeaseTable, WaitUntilNoLeasesWaitsForInvalidatedLeases) {
  LeaseTable table;
  const auto payload   = MakePayloadID("payload-wait-invalidated");
  const auto lease_key = MakeLeaseID("invalidated-lease");

  table.Insert(MakeLease("invalidated-lease", payload, std::chrono::system_clock::now() + std::chrono::seconds(60)));

  // Invalidate all leases for the payload — HasActive now returns false, but
  // the lease entry is still present and WaitUntilNoLeases must block.
  table.RemoveAll(payload);
  EXPECT_FALSE(table.HasActive(payload)) << "invalidated lease must not appear active";

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);

  // Simulate a lease holder that finishes after a short delay and calls Remove.
  std::thread holder([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    table.Remove(lease_key);
  });

  EXPECT_TRUE(table.WaitUntilNoLeases(payload, deadline))
      << "WaitUntilNoLeases must wait for the invalidated lease to be explicitly removed";

  holder.join();
}

TEST(LeaseTable, SecondaryIndexCleanupOnInsertAndRemoveAll) {
  LeaseTable table;
  const auto payload_a = MakePayloadID("payload-a");
  const auto payload_b = MakePayloadID("payload-b");

  // Use DISTINCT lease IDs so we actually test two independent leases.
  table.Insert(MakeLease("lease-for-a", payload_a, std::chrono::system_clock::now() + std::chrono::seconds(30)));
  table.Insert(MakeLease("lease-for-b", payload_b, std::chrono::system_clock::now() + std::chrono::seconds(30)));

  EXPECT_TRUE(table.HasActive(payload_a));
  EXPECT_TRUE(table.HasActive(payload_b));

  // RemoveAll on payload_b must not touch payload_a.
  table.RemoveAll(payload_b);
  EXPECT_TRUE(table.HasActive(payload_a));
  EXPECT_FALSE(table.HasActive(payload_b));
}

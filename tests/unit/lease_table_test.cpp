#include "internal/lease/lease_table.hpp"

#include <gtest/gtest.h>

#include <chrono>

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

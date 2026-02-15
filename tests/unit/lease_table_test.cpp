#include "internal/lease/lease_table.hpp"

#include <cassert>
#include <chrono>
#include <iostream>

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

Lease MakeLease(const std::string& lease_id, const PayloadID& payload_id, std::chrono::system_clock::time_point expires_at) {
  Lease lease;
  lease.lease_id           = lease_id;
  lease.payload_id         = payload_id;
  lease.payload_descriptor = PayloadDescriptor{};
  lease.expires_at         = expires_at;
  return lease;
}

void TestExpiredLeaseIsInactive() {
  LeaseTable table;
  const auto payload = MakePayloadID("payload-expired");

  table.Insert(MakeLease("lease-expired", payload, std::chrono::system_clock::now() - std::chrono::seconds(1)));

  assert(!table.HasActive(payload));
}

void TestMixedExpiredAndActiveLeases() {
  LeaseTable table;
  const auto payload = MakePayloadID("payload-mixed");

  table.Insert(MakeLease("lease-expired", payload, std::chrono::system_clock::now() - std::chrono::seconds(1)));
  table.Insert(MakeLease("lease-active", payload, std::chrono::system_clock::now() + std::chrono::seconds(30)));

  assert(table.HasActive(payload));

  table.Remove("lease-active");
  assert(!table.HasActive(payload));
}

void TestSecondaryIndexCleanupOnInsertAndRemoveAll() {
  LeaseTable table;
  const auto payload_a = MakePayloadID("payload-a");
  const auto payload_b = MakePayloadID("payload-b");

  table.Insert(MakeLease("lease-shared", payload_a, std::chrono::system_clock::now() + std::chrono::seconds(30)));
  table.Insert(MakeLease("lease-shared", payload_b, std::chrono::system_clock::now() + std::chrono::seconds(30)));

  assert(!table.HasActive(payload_a));
  assert(table.HasActive(payload_b));

  table.RemoveAll(payload_b);
  assert(!table.HasActive(payload_b));
}

} // namespace

int main() {
  TestExpiredLeaseIsInactive();
  TestMixedExpiredAndActiveLeases();
  TestSecondaryIndexCleanupOnInsertAndRemoveAll();

  std::cout << "payload_manager_unit_lease_table: pass\n";
  return 0;
}

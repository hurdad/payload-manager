/*
  Tests confirming that LeaseTable operations are correctly scoped to the
  target payload and do not inadvertently affect other payloads' leases.

  Before the fix, Insert() and HasActive() iterated every entry in the
  by_payload_ multimap (O(total_leases)) and the loop skipped non-matching
  keys with a continue.  That was correct but O(n); more critically, a bug
  in RemoveAll() used the same pattern which is now replaced with
  equal_range so only the target payload's bucket is touched.

  Covered:
    - Insert expiry sweep is scoped to the target payload: inserting a new
      lease for payload_a causes its expired leases to be reaped, but
      payload_b's active leases are left untouched.
    - HasActive only reports activity for the queried payload; a completely
      separate payload's leases do not influence the result.
    - RemoveAll on payload_a does not remove payload_b's leases.
    - RemoveAll leaves the table empty for the removed payload but the
      remaining payload still reports active.
*/

#include <cassert>
#include <chrono>
#include <iostream>

#include "internal/lease/lease_table.hpp"

namespace {

using payload::lease::Lease;
using payload::lease::LeaseTable;
using payload::manager::v1::LeaseID;
using payload::manager::v1::PayloadDescriptor;
using payload::manager::v1::PayloadID;

PayloadID MakePayload(const std::string& v) {
  PayloadID id;
  id.set_value(v);
  return id;
}

LeaseID MakeLID(const std::string& v) {
  LeaseID id;
  id.set_value(v);
  return id;
}

Lease ActiveLease(const std::string& lid, const PayloadID& pid) {
  Lease l;
  l.lease_id   = MakeLID(lid);
  l.payload_id = pid;
  l.expires_at = std::chrono::system_clock::now() + std::chrono::seconds(60);
  return l;
}

Lease ExpiredLease(const std::string& lid, const PayloadID& pid) {
  Lease l;
  l.lease_id   = MakeLID(lid);
  l.payload_id = pid;
  l.expires_at = std::chrono::system_clock::now() - std::chrono::seconds(1);
  return l;
}

// ---------------------------------------------------------------------------
// Test: Insert's expiry sweep only reaps leases for the target payload.
//       payload_b's active lease must survive even after payload_a's
//       expired leases are swept during a new Insert for payload_a.
// ---------------------------------------------------------------------------
void TestInsertSweepScopedToTargetPayload() {
  LeaseTable table;
  const auto pa = MakePayload("payload-a");
  const auto pb = MakePayload("payload-b");

  // Give payload_b an active lease BEFORE we do anything with payload_a.
  table.Insert(ActiveLease("b-lease", pb));
  assert(table.HasActive(pb) && "payload_b must be active before sweep");

  // Insert an expired lease for payload_a then a new active one, which
  // triggers the expiry sweep for payload_a's bucket.
  table.Insert(ExpiredLease("a-expired", pa));
  table.Insert(ActiveLease("a-active", pa));

  // payload_b must be unaffected.
  assert(table.HasActive(pb) && "payload_b lease must survive sweep for payload_a");
  assert(table.HasActive(pa) && "payload_a must still have its fresh active lease");
}

// ---------------------------------------------------------------------------
// Test: HasActive on payload_a does not disturb payload_b's entries.
//       Querying HasActive(pa) — which lazily expires stale entries —
//       must leave payload_b's lease intact.
// ---------------------------------------------------------------------------
void TestHasActiveDoesNotAffectOtherPayloads() {
  LeaseTable table;
  const auto pa = MakePayload("payload-sweep-a");
  const auto pb = MakePayload("payload-sweep-b");

  table.Insert(ExpiredLease("a-exp", pa));
  table.Insert(ActiveLease("b-act", pb));

  // Querying pa triggers lazy cleanup of pa's expired lease.
  assert(!table.HasActive(pa) && "expired lease must make payload_a inactive");
  // pb must remain active.
  assert(table.HasActive(pb) && "payload_b active lease must be unaffected by HasActive(pa)");
}

// ---------------------------------------------------------------------------
// Test: RemoveAll on payload_a must not remove payload_b's leases.
// ---------------------------------------------------------------------------
void TestRemoveAllScopedToTargetPayload() {
  LeaseTable table;
  const auto pa = MakePayload("payload-rm-a");
  const auto pb = MakePayload("payload-rm-b");

  table.Insert(ActiveLease("a-1", pa));
  table.Insert(ActiveLease("a-2", pa));
  table.Insert(ActiveLease("b-1", pb));

  assert(table.HasActive(pa));
  assert(table.HasActive(pb));

  table.RemoveAll(pa);

  assert(!table.HasActive(pa) && "payload_a must have no leases after RemoveAll");
  assert(table.HasActive(pb) && "payload_b lease must survive RemoveAll(pa)");
}

// ---------------------------------------------------------------------------
// Test: RemoveAll on payload with many leases leaves the table clean for
//       that payload while other payloads are unaffected.
// ---------------------------------------------------------------------------
void TestRemoveAllClearsAllLeasesForPayload() {
  LeaseTable table;
  const auto pa = MakePayload("payload-many");
  const auto pb = MakePayload("payload-bystander");

  for (int i = 0; i < 10; ++i) {
    table.Insert(ActiveLease("lease-" + std::to_string(i), pa));
  }
  table.Insert(ActiveLease("b-sole", pb));

  table.RemoveAll(pa);

  assert(!table.HasActive(pa) && "all leases for payload_a must be gone");
  assert(table.HasActive(pb) && "payload_b bystander lease must survive");
}

} // namespace

int main() {
  TestInsertSweepScopedToTargetPayload();
  TestHasActiveDoesNotAffectOtherPayloads();
  TestRemoveAllScopedToTargetPayload();
  TestRemoveAllClearsAllLeasesForPayload();

  std::cout << "lease_table_multi_payload_test: pass\n";
  return 0;
}

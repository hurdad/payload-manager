/*
  Tests for LeaseManager duration clamping and default-duration logic.

  The clamping rules (lease_manager.cpp):
    - min_duration_ms == 0  → use default_lease_ms
    - min_duration_ms >  0  → use min_duration_ms, then clamp to max_lease_ms
    - max_lease_ms    == 0  → no cap (any requested duration is accepted as-is)
*/

#include "internal/lease/lease_manager.hpp"

#include <gtest/gtest.h>

#include <chrono>

#include "payload/manager/v1.hpp"

namespace {

using payload::lease::LeaseManager;
using payload::manager::v1::PayloadDescriptor;
using payload::manager::v1::PayloadID;

PayloadID MakeID(const std::string& v) {
  PayloadID id;
  id.set_value(v);
  return id;
}

// Returns how many milliseconds from now the lease expires (rounded to nearest ms).
int64_t LeaseExpiryMs(const payload::lease::Lease& lease) {
  const auto now  = std::chrono::system_clock::now();
  const auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(lease.expires_at - now);
  return diff.count();
}

} // namespace

// ---------------------------------------------------------------------------
// min_duration_ms == 0 uses the configured default
// ---------------------------------------------------------------------------
TEST(LeaseManager, ZeroDurationUsesDefault) {
  constexpr uint64_t kDefault = 30'000;
  constexpr uint64_t kMax     = 120'000;
  LeaseManager       mgr(kDefault, kMax);

  const auto lease = mgr.Acquire(MakeID("p1"), PayloadDescriptor{}, /*min_duration_ms=*/0);

  // Expiry must be approximately now + kDefault.  Allow ±500 ms for test jitter.
  const auto expiry_ms = LeaseExpiryMs(lease);
  EXPECT_GE(expiry_ms, static_cast<int64_t>(kDefault) - 500) << "lease expiry must be at least default_lease_ms from now";
  EXPECT_LE(expiry_ms, static_cast<int64_t>(kDefault) + 500) << "lease expiry must not exceed default_lease_ms + jitter";
}

// ---------------------------------------------------------------------------
// min_duration_ms > max_lease_ms is clamped to max_lease_ms
// ---------------------------------------------------------------------------
TEST(LeaseManager, OverMaxIsClamped) {
  constexpr uint64_t kDefault = 20'000;
  constexpr uint64_t kMax     = 60'000;
  LeaseManager       mgr(kDefault, kMax);

  const auto lease = mgr.Acquire(MakeID("p2"), PayloadDescriptor{}, /*min_duration_ms=*/300'000);

  const auto expiry_ms = LeaseExpiryMs(lease);
  EXPECT_GE(expiry_ms, static_cast<int64_t>(kMax) - 500) << "clamped expiry must be at least max_lease_ms from now";
  EXPECT_LE(expiry_ms, static_cast<int64_t>(kMax) + 500) << "clamped expiry must not exceed max_lease_ms + jitter";
}

// ---------------------------------------------------------------------------
// min_duration_ms == max_lease_ms passes through unchanged
// ---------------------------------------------------------------------------
TEST(LeaseManager, ExactMaxPassesThrough) {
  constexpr uint64_t kDefault = 20'000;
  constexpr uint64_t kMax     = 60'000;
  LeaseManager       mgr(kDefault, kMax);

  const auto lease = mgr.Acquire(MakeID("p3"), PayloadDescriptor{}, /*min_duration_ms=*/kMax);

  const auto expiry_ms = LeaseExpiryMs(lease);
  EXPECT_GE(expiry_ms, static_cast<int64_t>(kMax) - 500);
  EXPECT_LE(expiry_ms, static_cast<int64_t>(kMax) + 500);
}

// ---------------------------------------------------------------------------
// max_lease_ms == 0 disables the cap — a large request is honoured
// ---------------------------------------------------------------------------
TEST(LeaseManager, ZeroMaxDisablesCap) {
  constexpr uint64_t kDefault   = 20'000;
  constexpr uint64_t kNoCap     = 0;
  constexpr uint64_t kRequested = 600'000; // 10 minutes
  LeaseManager       mgr(kDefault, kNoCap);

  const auto lease = mgr.Acquire(MakeID("p4"), PayloadDescriptor{}, /*min_duration_ms=*/kRequested);

  const auto expiry_ms = LeaseExpiryMs(lease);
  EXPECT_GE(expiry_ms, static_cast<int64_t>(kRequested) - 500) << "when max_lease_ms==0, large requests must not be clamped";
}

// ---------------------------------------------------------------------------
// A non-zero min_duration_ms below the max passes through unchanged
// ---------------------------------------------------------------------------
TEST(LeaseManager, BelowMaxPassesThroughUnchanged) {
  constexpr uint64_t kDefault   = 20'000;
  constexpr uint64_t kMax       = 120'000;
  constexpr uint64_t kRequested = 45'000;
  LeaseManager       mgr(kDefault, kMax);

  const auto lease = mgr.Acquire(MakeID("p5"), PayloadDescriptor{}, /*min_duration_ms=*/kRequested);

  const auto expiry_ms = LeaseExpiryMs(lease);
  EXPECT_GE(expiry_ms, static_cast<int64_t>(kRequested) - 500);
  EXPECT_LE(expiry_ms, static_cast<int64_t>(kRequested) + 500);
}

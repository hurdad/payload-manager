#include "internal/ring/ring_lease_table.hpp"

#include <cstring>
#include <random>
#include <stdexcept>

namespace payload::ring {

size_t LeaseIdHash::operator()(const LeaseId& id) const noexcept {
  // 16 bytes folded into a size_t via xor of two 64-bit halves —
  // cheap, good enough mixing for random UUIDs (which already have
  // ~uniform distribution).
  uint64_t a, b;
  std::memcpy(&a, id.data(), sizeof(a));
  std::memcpy(&b, id.data() + 8, sizeof(b));
  return static_cast<size_t>(a ^ b);
}

RingLeaseTable::RingLeaseTable() = default;

LeaseId RingLeaseTable::NewLeaseId_() {
  // Per-call thread_local PRNG: avoids contending on a global RNG
  // mutex without paying re-seeding cost on every call. Seeded
  // once per thread from random_device.
  thread_local std::mt19937_64 rng{std::random_device{}()};
  LeaseId                      id{};
  uint64_t                     a = rng();
  uint64_t                     b = rng();
  std::memcpy(id.data(), &a, sizeof(a));
  std::memcpy(id.data() + 8, &b, sizeof(b));
  return id;
}

LeaseId RingLeaseTable::Insert(std::string ring_id, uint32_t slot_idx, uint64_t generation) {
  std::lock_guard<std::mutex> lk(mu_);
  // 2^128 RNG space — collision is astronomically unlikely, but
  // re-roll defensively. Bounds the loop at a few attempts.
  for (int attempt = 0; attempt < 8; ++attempt) {
    LeaseId id          = NewLeaseId_();
    auto [it, inserted] = table_.emplace(
        id, LeaseRecord{.ring_id = ring_id, .slot_idx = slot_idx, .generation = generation, .granted_at = std::chrono::steady_clock::now()});
    if (inserted) return id;
  }
  // 8 collisions in a row means the RNG is degenerate (or someone is
  // attacking us); fail loud rather than silently overwrite a live
  // lease.
  throw std::runtime_error("RingLeaseTable: 8 consecutive lease_id collisions; RNG degenerate?");
}

std::optional<LeaseRecord> RingLeaseTable::Remove(const LeaseId& id) {
  std::lock_guard<std::mutex> lk(mu_);
  auto                        it = table_.find(id);
  if (it == table_.end()) return std::nullopt;
  LeaseRecord r = it->second;
  table_.erase(it);
  return r;
}

std::vector<LeaseRecord> RingLeaseTable::RemoveExpiredForRing(const std::string& ring_id, std::chrono::steady_clock::time_point cutoff) {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<LeaseRecord>    expired;
  for (auto it = table_.begin(); it != table_.end();) {
    if (it->second.ring_id == ring_id && it->second.granted_at <= cutoff) {
      expired.push_back(it->second);
      it = table_.erase(it);
    } else {
      ++it;
    }
  }
  return expired;
}

std::optional<LeaseId> RingLeaseTable::FromBytes(const std::string& bytes) {
  if (bytes.size() != 16) return std::nullopt;
  LeaseId id{};
  std::memcpy(id.data(), bytes.data(), 16);
  return id;
}

std::string RingLeaseTable::ToBytes(const LeaseId& id) {
  return std::string(reinterpret_cast<const char*>(id.data()), id.size());
}

size_t RingLeaseTable::Size() const {
  std::lock_guard<std::mutex> lk(mu_);
  return table_.size();
}

} // namespace payload::ring

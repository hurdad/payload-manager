#include "lease_table.hpp"

#include "payload/manager/v1.hpp"

namespace payload::lease {

std::string LeaseTable::Key(const payload::manager::v1::PayloadID& id) {
  return id.value();
}

std::string LeaseTable::Key(const payload::manager::v1::LeaseID& id) {
  return id.value();
}

bool LeaseTable::IsExpired(const Lease& lease, Clock::time_point now) {
  return lease.expires_at <= now;
}

Lease LeaseTable::Insert(const Lease& lease) {
  std::lock_guard lock(mutex_);

  const auto lease_key = Key(lease.lease_id);
  if (auto existing = leases_.find(lease_key); existing != leases_.end()) {
    auto old_range = by_payload_.equal_range(Key(existing->second.payload_id));
    for (auto it = old_range.first; it != old_range.second; ++it) {
      if (it->second == lease_key) {
        by_payload_.erase(it);
        break;
      }
    }
  }

  // Expire stale leases for this payload only, using equal_range to avoid an
  // O(total_leases) scan over all payloads.
  const auto now         = Clock::now();
  const auto payload_key = Key(lease.payload_id);
  auto       range       = by_payload_.equal_range(payload_key);
  for (auto it = range.first; it != range.second;) {
    auto lease_it = leases_.find(it->second);
    if (lease_it == leases_.end() || IsExpired(lease_it->second, now) || Key(lease_it->second.payload_id) != payload_key) {
      if (lease_it != leases_.end()) leases_.erase(lease_it);
      it = by_payload_.erase(it);
      continue;
    }
    ++it;
  }

  leases_[lease_key] = lease;
  by_payload_.emplace(payload_key, lease_key);
  return lease;
}

void LeaseTable::Remove(const payload::manager::v1::LeaseID& lease_id) {
  {
    std::lock_guard lock(mutex_);

    const auto lease_key = Key(lease_id);
    invalidated_leases_.erase(lease_key);

    auto it = leases_.find(lease_key);
    if (it == leases_.end()) return;

    auto range = by_payload_.equal_range(Key(it->second.payload_id));
    for (auto i = range.first; i != range.second; ++i) {
      if (i->second == lease_key) {
        by_payload_.erase(i);
        break;
      }
    }

    leases_.erase(it);
  }
  release_cv_.notify_all();
}

bool LeaseTable::HasActive(const payload::manager::v1::PayloadID& id) {
  std::lock_guard lock(mutex_);
  return HasActiveLocked(Key(id), Clock::now());
}

uint32_t LeaseTable::CountActive(const payload::manager::v1::PayloadID& id) {
  std::lock_guard lock(mutex_);

  const auto now         = Clock::now();
  const auto payload_key = Key(id);
  uint32_t   count       = 0;

  auto range = by_payload_.equal_range(payload_key);
  for (auto it = range.first; it != range.second;) {
    auto lease_it = leases_.find(it->second);
    if (lease_it == leases_.end() || IsExpired(lease_it->second, now) || Key(lease_it->second.payload_id) != payload_key) {
      if (lease_it != leases_.end()) leases_.erase(lease_it);
      it = by_payload_.erase(it);
      continue;
    }
    // Don't count invalidated leases as active.
    if (invalidated_leases_.count(it->second)) {
      ++it;
      continue;
    }
    ++count;
    ++it;
  }

  return count;
}

void LeaseTable::RemoveAll(const payload::manager::v1::PayloadID& id) {
  {
    std::lock_guard lock(mutex_);

    // Mark all active leases as invalidated rather than removing them
    // immediately.  This makes HasActive/CountActive return false (so
    // non-force operations can proceed) while keeping the lease entries alive
    // until each holder calls Remove().  WaitUntilNoLeases uses HasAnyLocked
    // to detect when all in-flight holders have finished.
    const auto payload_key = Key(id);
    auto       range       = by_payload_.equal_range(payload_key);
    for (auto it = range.first; it != range.second; ++it) {
      invalidated_leases_.insert(it->second);
    }
  }
  release_cv_.notify_all();
}

bool LeaseTable::HasActiveLocked(const std::string& payload_key, Clock::time_point now) {
  auto range = by_payload_.equal_range(payload_key);
  for (auto it = range.first; it != range.second;) {
    auto lease_it = leases_.find(it->second);
    if (lease_it == leases_.end() || IsExpired(lease_it->second, now) || Key(lease_it->second.payload_id) != payload_key) {
      if (lease_it != leases_.end()) leases_.erase(lease_it);
      it = by_payload_.erase(it);
      continue;
    }
    // Skip invalidated leases — they no longer count as "active" for the
    // purpose of blocking non-force operations, but they are still "held"
    // until the caller explicitly calls Remove().
    if (invalidated_leases_.count(it->second)) {
      ++it;
      continue;
    }
    return true;
  }
  return false;
}

bool LeaseTable::HasAnyLocked(const std::string& payload_key) const {
  return by_payload_.count(payload_key) > 0;
}

bool LeaseTable::WaitUntilNoLeases(const payload::manager::v1::PayloadID& id,
                                   std::chrono::steady_clock::time_point  deadline) {
  const auto payload_key = Key(id);
  std::unique_lock lock(mutex_);

  // In each iteration:
  //   1. HasActiveLocked sweeps naturally-expired leases out of by_payload_,
  //      so HasAnyLocked will reflect the true remaining count after the sweep.
  //   2. HasAnyLocked counts both non-expired active leases AND invalidated
  //      leases that are still "held" pending an explicit Remove() call.
  // We poll at ≤10 ms so that naturally-expired leases (which don't trigger a
  // cv notification) are detected promptly, while an explicit Remove() also
  // wakes us immediately via the cv.
  while (std::chrono::steady_clock::now() < deadline) {
    HasActiveLocked(payload_key, Clock::now()); // sweep expired entries
    if (!HasAnyLocked(payload_key)) {
      return true;
    }
    const auto poll_until = std::min(deadline, std::chrono::steady_clock::now() + std::chrono::milliseconds(10));
    release_cv_.wait_until(lock, poll_until);
  }
  HasActiveLocked(payload_key, Clock::now()); // final sweep
  return !HasAnyLocked(payload_key);
}

} // namespace payload::lease

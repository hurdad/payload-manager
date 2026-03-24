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
    auto       it        = leases_.find(lease_key);
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

  const auto now         = Clock::now();
  const auto payload_key = Key(id);
  bool       has_active  = false;

  auto range = by_payload_.equal_range(payload_key);
  for (auto it = range.first; it != range.second;) {
    auto lease_it = leases_.find(it->second);
    if (lease_it == leases_.end() || IsExpired(lease_it->second, now) || Key(lease_it->second.payload_id) != payload_key) {
      if (lease_it != leases_.end()) leases_.erase(lease_it);
      it = by_payload_.erase(it);
      continue;
    }
    has_active = true;
    ++it;
  }

  return has_active;
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
    ++count;
    ++it;
  }

  return count;
}

void LeaseTable::RemoveAll(const payload::manager::v1::PayloadID& id) {
  {
    std::lock_guard lock(mutex_);

    const auto payload_key = Key(id);
    auto       range       = by_payload_.equal_range(payload_key);
    for (auto it = range.first; it != range.second;) {
      leases_.erase(it->second);
      it = by_payload_.erase(it);
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
    return true;
  }
  return false;
}

bool LeaseTable::WaitUntilNoLeases(const payload::manager::v1::PayloadID& id,
                                   std::chrono::steady_clock::time_point  deadline) {
  const auto payload_key = Key(id);
  std::unique_lock lock(mutex_);
  return release_cv_.wait_until(lock, deadline, [&] {
    return !HasActiveLocked(payload_key, Clock::now());
  });
}

} // namespace payload::lease

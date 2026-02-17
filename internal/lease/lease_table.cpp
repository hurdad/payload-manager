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

  const auto now         = Clock::now();
  const auto payload_key = Key(lease.payload_id);
  for (auto it = by_payload_.begin(); it != by_payload_.end();) {
    if (it->first != payload_key) {
      ++it;
      continue;
    }

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

bool LeaseTable::HasActive(const payload::manager::v1::PayloadID& id) {
  std::lock_guard lock(mutex_);

  const auto now         = Clock::now();
  const auto payload_key = Key(id);
  bool       has_active  = false;

  for (auto it = by_payload_.begin(); it != by_payload_.end();) {
    if (it->first != payload_key) {
      ++it;
      continue;
    }

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

void LeaseTable::RemoveAll(const payload::manager::v1::PayloadID& id) {
  std::lock_guard lock(mutex_);

  const auto payload_key = Key(id);
  for (auto it = by_payload_.begin(); it != by_payload_.end();) {
    if (it->first != payload_key) {
      ++it;
      continue;
    }

    leases_.erase(it->second);
    it = by_payload_.erase(it);
  }
}

} // namespace payload::lease

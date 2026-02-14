#include "lease_table.hpp"
#include "payload/manager/v1.hpp"

namespace payload::lease {

std::string LeaseTable::Key(const payload::manager::v1::PayloadID& id) {
  return id.value();
}

Lease LeaseTable::Insert(const Lease& lease) {
  std::lock_guard lock(mutex_);

  leases_[lease.lease_id] = lease;
  by_payload_.emplace(Key(lease.payload_id), lease.lease_id);
  return lease;
}

void LeaseTable::Remove(const std::string& lease_id) {
  std::lock_guard lock(mutex_);

  auto it = leases_.find(lease_id);
  if (it == leases_.end())
    return;

  auto range = by_payload_.equal_range(Key(it->second.payload_id));
  for (auto i = range.first; i != range.second; ++i) {
    if (i->second == lease_id) {
      by_payload_.erase(i);
      break;
    }
  }

  leases_.erase(it);
}

bool LeaseTable::HasActive(const payload::manager::v1::PayloadID& id) {
  std::lock_guard lock(mutex_);
  return by_payload_.count(Key(id)) > 0;
}

void LeaseTable::RemoveAll(const payload::manager::v1::PayloadID& id) {
  std::lock_guard lock(mutex_);

  auto range = by_payload_.equal_range(Key(id));
  for (auto it = range.first; it != range.second; ++it)
    leases_.erase(it->second);

  by_payload_.erase(Key(id));
}

}

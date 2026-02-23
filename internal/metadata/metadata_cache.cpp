#include "metadata_cache.hpp"

#include "payload/manager/v1.hpp"

namespace payload::metadata {

using namespace payload::manager::v1;

std::string MetadataCache::Key(const PayloadID& id) {
  return id.value();
}

void MetadataCache::TouchLocked(const std::string& key) const {
  auto index_it = recency_index_.find(key);
  if (index_it != recency_index_.end()) {
    recency_.erase(index_it->second);
  }

  recency_.push_back(key);
  recency_index_[key] = std::prev(recency_.end());
}

// ------------------------------------------------------------
// Put
// ------------------------------------------------------------

void MetadataCache::Put(const PayloadID& id, const PayloadMetadata& metadata) {
  std::unique_lock lock(mutex_);

  const auto key = Key(id);
  cache_[key]    = metadata;
  TouchLocked(key);
}

// ------------------------------------------------------------
// Merge
// ------------------------------------------------------------

void MetadataCache::Merge(const PayloadID& id, const PayloadMetadata& update) {
  std::unique_lock lock(mutex_);

  const auto key = Key(id);
  auto&      dst = cache_[key];

  if (dst.id().value().empty()) {
    *dst.mutable_id() = id;
  }

  if (!update.data().empty()) {
    dst.set_data(update.data());
  }

  if (!update.schema().empty()) {
    dst.set_schema(update.schema());
  }

  TouchLocked(key);
}

// ------------------------------------------------------------
// Get
// ------------------------------------------------------------

std::optional<PayloadMetadata> MetadataCache::Get(const PayloadID& id) const {
  std::unique_lock lock(mutex_);

  const auto key = Key(id);
  auto       it  = cache_.find(key);
  if (it == cache_.end()) return std::nullopt;

  const auto metadata = it->second;
  TouchLocked(key);
  return metadata;
}

std::vector<PayloadID> MetadataCache::ListIds() const {
  std::shared_lock lock(mutex_);

  std::vector<PayloadID> ids;
  ids.reserve(cache_.size());

  for (const auto& [key, _] : cache_) {
    PayloadID id;
    id.set_value(key);
    ids.push_back(std::move(id));
  }

  return ids;
}

std::optional<PayloadID> MetadataCache::GetLeastRecentlyUsedId() const {
  std::shared_lock lock(mutex_);

  if (recency_.empty()) {
    return std::nullopt;
  }

  PayloadID id;
  id.set_value(recency_.front());
  return id;
}

// ------------------------------------------------------------
// Remove
// ------------------------------------------------------------

void MetadataCache::Remove(const PayloadID& id) {
  std::unique_lock lock(mutex_);

  const auto key = Key(id);
  cache_.erase(key);

  auto index_it = recency_index_.find(key);
  if (index_it != recency_index_.end()) {
    recency_.erase(index_it->second);
    recency_index_.erase(index_it);
  }
}

} // namespace payload::metadata

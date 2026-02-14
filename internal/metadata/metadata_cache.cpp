#include "metadata_cache.hpp"
#include "payload/manager/v1_compat.hpp"

namespace payload::metadata {

using namespace payload::manager::v1;

std::string MetadataCache::Key(const PayloadID& id) {
    return id.value();
}

// ------------------------------------------------------------
// Put
// ------------------------------------------------------------

void MetadataCache::Put(const PayloadID& id,
                        const PayloadMetadata& metadata) {
    std::unique_lock lock(mutex_);
    cache_[Key(id)] = metadata;
}

// ------------------------------------------------------------
// Merge
// ------------------------------------------------------------

void MetadataCache::Merge(const PayloadID& id,
                          const PayloadMetadata& update) {

    std::unique_lock lock(mutex_);

    auto& dst = cache_[Key(id)];

    if (dst.id().value().empty()) {
        *dst.mutable_id() = id;
    }

    if (!update.data().empty()) {
        dst.set_data(update.data());
    }

    if (!update.schema().empty()) {
        dst.set_schema(update.schema());
    }
}

// ------------------------------------------------------------
// Get
// ------------------------------------------------------------

std::optional<PayloadMetadata>
MetadataCache::Get(const PayloadID& id) const {
    std::shared_lock lock(mutex_);

    auto it = cache_.find(Key(id));
    if (it == cache_.end())
        return std::nullopt;

    return it->second;
}

// ------------------------------------------------------------
// Remove
// ------------------------------------------------------------

void MetadataCache::Remove(const PayloadID& id) {
    std::unique_lock lock(mutex_);
    cache_.erase(Key(id));
}

} // namespace payload::metadata

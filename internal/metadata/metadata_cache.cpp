#include "metadata_cache.hpp"

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

    // simple merge semantics:
    // overwrite set fields, preserve others

    if (!update.name().empty())
        dst.set_name(update.name());

    if (!update.group_id().empty())
        dst.set_group_id(update.group_id());

    if (update.size_bytes())
        dst.set_size_bytes(update.size_bytes());

    if (update.compressed_size_bytes())
        dst.set_compressed_size_bytes(update.compressed_size_bytes());

    if (update.format())
        dst.set_format(update.format());

    if (update.compression())
        dst.set_compression(update.compression());

    if (update.current_tier())
        dst.set_current_tier(update.current_tier());

    if (update.state())
        dst.set_state(update.state());

    if (update.access_count())
        dst.set_access_count(update.access_count());

    if (!update.checksum().empty())
        dst.set_checksum(update.checksum());

    // merge attributes map
    for (const auto& [k, v] : update.attributes())
        (*dst.mutable_attributes())[k] = v;
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

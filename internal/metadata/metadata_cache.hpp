#pragma once

#include <list>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "payload/manager/catalog/v1/catalog.pb.h"
#include "payload/manager/v1.hpp"

namespace payload::metadata {

class MetadataCache {
 public:
  void Put(const payload::manager::v1::PayloadID& id, const payload::manager::v1::PayloadMetadata& metadata);

  void Merge(const payload::manager::v1::PayloadID& id, const payload::manager::v1::PayloadMetadata& update);

  std::optional<payload::manager::v1::PayloadMetadata> Get(const payload::manager::v1::PayloadID& id) const;

  std::vector<payload::manager::v1::PayloadID> ListIds() const;

  std::optional<payload::manager::v1::PayloadID> GetLeastRecentlyUsedId() const;

  void Remove(const payload::manager::v1::PayloadID& id);

 private:
  static std::string Key(const payload::manager::v1::PayloadID& id);

  void TouchLocked(const std::string& key) const;

  mutable std::shared_mutex                                              mutex_;
  std::unordered_map<std::string, payload::manager::v1::PayloadMetadata> cache_;
  mutable std::list<std::string>                                          recency_;
  mutable std::unordered_map<std::string, std::list<std::string>::iterator> recency_index_;
};

} // namespace payload::metadata

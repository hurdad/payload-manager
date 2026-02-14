#pragma once

#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "payload/manager/catalog/v1/catalog.pb.h"
#include "payload/manager/v1.hpp"

namespace payload::metadata {

class MetadataCache {
 public:
  void Put(const payload::manager::v1::PayloadID& id, const payload::manager::v1::PayloadMetadata& metadata);

  void Merge(const payload::manager::v1::PayloadID& id, const payload::manager::v1::PayloadMetadata& update);

  std::optional<payload::manager::v1::PayloadMetadata> Get(const payload::manager::v1::PayloadID& id) const;

  void Remove(const payload::manager::v1::PayloadID& id);

 private:
  static std::string Key(const payload::manager::v1::PayloadID& id);

  mutable std::shared_mutex                                              mutex_;
  std::unordered_map<std::string, payload::manager::v1::PayloadMetadata> cache_;
};

} // namespace payload::metadata

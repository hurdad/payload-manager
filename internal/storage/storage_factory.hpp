#pragma once

#include <memory>
#include <unordered_map>

#include "config/config.pb.h"
#include "payload/manager/v1.hpp"
#include "storage_backend.hpp"

namespace payload::storage {

/*
  Builds all tier storage backends from configuration.

  Core uses this as:

      auto stores = StorageFactory::Build(config);
      stores[TIER_RAM]->Allocate(...)
*/

class StorageFactory {
 public:
  using TierMap = std::unordered_map<payload::manager::v1::Tier, StorageBackendPtr>;

  static TierMap Build(const payload::runtime::config::StorageConfig& cfg);
};

} // namespace payload::storage

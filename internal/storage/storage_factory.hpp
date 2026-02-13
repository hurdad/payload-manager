#pragma once

#include <memory>
#include <unordered_map>

#include "storage_backend.hpp"
#include "payload/manager/v1/config.pb.h"

namespace payload::storage {

/*
  Builds all tier storage backends from configuration.

  Core uses this as:

      auto stores = StorageFactory::Build(config);
      stores[TIER_RAM]->Allocate(...)
*/

class StorageFactory {
public:

  using TierMap = std::unordered_map<
      payload::manager::v1::Tier,
      StorageBackendPtr>;

  static TierMap Build(const payload::manager::v1::StorageConfig& cfg);

};

} // namespace payload::storage

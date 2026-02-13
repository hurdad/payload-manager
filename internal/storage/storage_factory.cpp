#include "storage_factory.hpp"

namespace payload::storage {

StorageFactory::TierMap BuildMap(const payload::runtime::config::StorageConfig&) {
  return {};
}

StorageFactory::TierMap StorageFactory::Build(const payload::runtime::config::StorageConfig& cfg) {
  (void)cfg;
  return {};
}

} // namespace payload::storage

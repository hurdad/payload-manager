#include "storage_factory.hpp"

#include <filesystem>

#include "common/arrow_utils.hpp"
#include "disk/disk_arrow_store.hpp"
#include "object/object_arrow_store.hpp"
#include "ram/ram_arrow_store.hpp"
#if PAYLOAD_MANAGER_ARROW_CUDA
#include "gpu/cuda_arrow_store.hpp"
#endif

namespace payload::storage {

StorageFactory::TierMap StorageFactory::Build(const payload::runtime::config::StorageConfig& cfg) {
  StorageFactory::TierMap stores;

  const std::string shm_prefix = cfg.ram().shm_prefix().empty() ? "pm" : cfg.ram().shm_prefix();
  stores.emplace(payload::manager::v1::TIER_RAM, std::make_shared<RamArrowStore>(shm_prefix));

  std::filesystem::path disk_root =
      cfg.disk_hot().root_path().empty() ? std::filesystem::path{"/tmp/payload-manager"} : std::filesystem::path{cfg.disk_hot().root_path()};
  stores.emplace(payload::manager::v1::TIER_DISK_HOT, std::make_shared<DiskArrowStore>(std::move(disk_root), payload::manager::v1::TIER_DISK_HOT));

  // The cold level is opt-in. An empty root_path means no TIER_DISK_COLD entry
  // in the map at all, which is what keeps the demotion chain at DISK → OBJECT
  // for every config written before this tier existed. Unlike the hot level
  // there is no default path: a cold tier that silently landed in /tmp would be
  // worse than no cold tier.
  if (!cfg.disk_cold().root_path().empty()) {
    stores.emplace(payload::manager::v1::TIER_DISK_COLD,
                   std::make_shared<DiskArrowStore>(std::filesystem::path{cfg.disk_cold().root_path()}, payload::manager::v1::TIER_DISK_COLD));
  }

  if (!cfg.object().root_path().empty()) {
    const bool is_s3 = cfg.object().filesystem() == pb::arrow::storage::FILE_SYSTEM_S3 || cfg.object().filesystem_options().has_s3();
    auto [object_fs, object_root] =
        payload::storage::common::Unwrap(payload::storage::common::ResolveFileSystem(cfg.object().root_path(), cfg.object()));
    stores.emplace(payload::manager::v1::TIER_OBJECT, std::make_shared<ObjectArrowStore>(std::move(object_fs), std::move(object_root), is_s3));
  }

#if PAYLOAD_MANAGER_ARROW_CUDA
  if (!cfg.gpu().devices().empty()) {
    stores.emplace(payload::manager::v1::TIER_GPU, std::make_shared<CudaArrowStore>(static_cast<int>(cfg.gpu().devices(0).device_id())));
  }
#endif

  return stores;
}

} // namespace payload::storage

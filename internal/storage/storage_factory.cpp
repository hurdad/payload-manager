#include "storage_factory.hpp"

#include <filesystem>

#include "disk/disk_arrow_store.hpp"
#include "ram/ram_arrow_store.hpp"
#if PAYLOAD_MANAGER_ARROW_CUDA
#include "gpu/cuda_arrow_store.hpp"
#endif

namespace payload::storage {

StorageFactory::TierMap StorageFactory::Build(const payload::runtime::config::StorageConfig& cfg) {
  StorageFactory::TierMap stores;

  stores.emplace(payload::manager::v1::TIER_RAM, std::make_shared<RamArrowStore>());

  std::filesystem::path disk_root =
      cfg.disk().root_path().empty() ? std::filesystem::path{"/tmp/payload-manager"} : std::filesystem::path{cfg.disk().root_path()};
  stores.emplace(payload::manager::v1::TIER_DISK, std::make_shared<DiskArrowStore>(std::move(disk_root)));

#if PAYLOAD_MANAGER_ARROW_CUDA
  if (!cfg.gpu().devices().empty()) {
    stores.emplace(payload::manager::v1::TIER_GPU, std::make_shared<CudaArrowStore>(static_cast<int>(cfg.gpu().devices(0).device_id())));
  }
#endif

  return stores;
}

} // namespace payload::storage

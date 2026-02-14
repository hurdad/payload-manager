#pragma once

#include <arrow/cuda/api.h>

#include <shared_mutex>
#include <unordered_map>

#include "cuda_context.hpp"
#include "internal/storage/storage_backend.hpp"
#include "payload/manager/v1.hpp"

namespace payload::storage {

/*
  GPU storage tier using Arrow CUDA buffers.

  Provides:
    - CUDA allocations
    - IPC handles for cross-process reads
    - spill to RAM/DISK via copy
*/

class CudaArrowStore final : public StorageBackend {
 public:
  explicit CudaArrowStore(int device_id = 0);

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size_bytes) override;

  std::shared_ptr<arrow::Buffer> Read(const payload::manager::v1::PayloadID& id) override;

  void Write(const payload::manager::v1::PayloadID& id, const std::shared_ptr<arrow::Buffer>& buffer, bool fsync) override;

  void Remove(const payload::manager::v1::PayloadID& id) override;

  payload::manager::v1::Tier TierType() const override {
    return payload::manager::v1::TIER_GPU;
  }

  // export CUDA IPC handle
  std::shared_ptr<arrow::cuda::CudaIpcMemHandle> ExportIPC(const payload::manager::v1::PayloadID& id);

 private:
  using UUID = std::string;

  static UUID Key(const payload::manager::v1::PayloadID& id);

  std::shared_ptr<arrow::cuda::CudaContext> ctx_;

  mutable std::shared_mutex                                          mutex_;
  std::unordered_map<UUID, std::shared_ptr<arrow::cuda::CudaBuffer>> buffers_;
};

} // namespace payload::storage

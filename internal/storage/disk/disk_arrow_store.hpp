#pragma once

#include <arrow/buffer.h>

#include <filesystem>

#include "internal/storage/storage_backend.hpp"
#include "payload/manager/v1.hpp"

namespace payload::storage {

/*
  Durable disk storage using Arrow IO.

  Properties:
    - atomic replace writes
    - optional fsync
    - mmap friendly reads later
*/

class DiskArrowStore final : public StorageBackend {
 public:
  // `tier` names which durable local level this instance backs. The cold level
  // (TIER_DISK_COLD) is the same on-disk format under a different root, so it
  // is the same class with a different tier identity rather than a subclass.
  explicit DiskArrowStore(std::filesystem::path root, payload::manager::v1::Tier tier = payload::manager::v1::TIER_DISK_HOT);

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size_bytes) override;

  std::shared_ptr<arrow::Buffer> Read(const payload::manager::v1::PayloadID& id) override;

  uint64_t Size(const payload::manager::v1::PayloadID& id) override;

  void Write(const payload::manager::v1::PayloadID& id, const std::shared_ptr<arrow::Buffer>& buffer, bool fsync) override;

  void Remove(const payload::manager::v1::PayloadID& id) override;

  void WriteSidecar(const payload::manager::v1::PayloadID& id, const payload::manager::catalog::v1::PayloadArchiveMetadata& meta) override;

  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }

 private:
  std::filesystem::path      root_;
  payload::manager::v1::Tier tier_;
};

} // namespace payload::storage

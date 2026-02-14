#pragma once

#include <unordered_map>
#include <shared_mutex>
#include <memory>

#include <arrow/buffer.h>

#include "internal/storage/storage_backend.hpp"
#include "payload/manager/v1.hpp"

namespace payload::storage {

/*
  RAM storage tier.

  Backed by Arrow buffers stored in-memory.
  Provides zero-copy reads to callers.

  Thread safety:
    - shared reads
    - exclusive writes
*/

class RamArrowStore final : public StorageBackend {
public:
  RamArrowStore() = default;
  ~RamArrowStore() override = default;

  // StorageBackend interface
  std::shared_ptr<arrow::Buffer>
  Allocate(const payload::manager::v1::PayloadID& id,
           uint64_t size_bytes) override;

  std::shared_ptr<arrow::Buffer>
  Read(const payload::manager::v1::PayloadID& id) override;

  void Write(const payload::manager::v1::PayloadID& id,
             const std::shared_ptr<arrow::Buffer>& buffer,
             bool fsync) override;

  void Remove(const payload::manager::v1::PayloadID& id) override;

  payload::manager::v1::Tier TierType() const override {
    return payload::manager::v1::TIER_RAM;
  }

private:
  using UUID = std::string;

  static UUID Key(const payload::manager::v1::PayloadID& id);

  mutable std::shared_mutex mutex_;
  std::unordered_map<UUID, std::shared_ptr<arrow::Buffer>> buffers_;
};

} // namespace payload::storage

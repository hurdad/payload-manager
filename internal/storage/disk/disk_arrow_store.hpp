#pragma once

#include <filesystem>
#include <arrow/buffer.h>

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
  explicit DiskArrowStore(std::filesystem::path root);

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
    return payload::manager::v1::TIER_DISK;
  }

private:
  std::filesystem::path root_;
};

}

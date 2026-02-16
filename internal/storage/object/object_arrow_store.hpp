#pragma once

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>

#include <memory>
#include <string>

#include "internal/storage/storage_backend.hpp"
#include "payload/manager/v1.hpp"

namespace payload::storage {

/*
  Object storage tier using Arrow filesystem.

  Characteristics:
    - immutable object writes
    - eventual durability
    - no allocation
    - no fsync semantics
*/

class ObjectArrowStore final : public StorageBackend {
 public:
  ObjectArrowStore(std::shared_ptr<arrow::fs::FileSystem> fs, std::string root_path);

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size_bytes) override;

  std::shared_ptr<arrow::Buffer> Read(const payload::manager::v1::PayloadID& id) override;

  void Write(const payload::manager::v1::PayloadID& id, const std::shared_ptr<arrow::Buffer>& buffer, bool fsync) override;

  void Remove(const payload::manager::v1::PayloadID& id) override;

  payload::manager::v1::Tier TierType() const override {
    return payload::manager::v1::TIER_OBJECT;
  }

 private:
  std::string ObjectPath(const payload::manager::v1::PayloadID& id) const;

  std::shared_ptr<arrow::fs::FileSystem> fs_;
  std::string                            root_path_;
};

} // namespace payload::storage

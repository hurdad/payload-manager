#pragma once

#include <memory>
#include <string>

#include <arrow/filesystem/s3fs.h>
#include <arrow/buffer.h>

#include "internal/storage/storage_backend.hpp"

namespace payload::storage {

/*
  Object storage tier (S3 / MinIO) using Arrow filesystem.

  Characteristics:
    - immutable object writes
    - eventual durability
    - no allocation
    - no fsync semantics
*/

class ObjectArrowStore final : public StorageBackend {
public:
  ObjectArrowStore(std::shared_ptr<arrow::fs::S3FileSystem> fs,
                   std::string bucket,
                   std::string prefix);

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
    return payload::manager::v1::TIER_OBJECT;
  }

private:
  std::string ObjectPath(const payload::manager::v1::PayloadID& id) const;

  std::shared_ptr<arrow::fs::S3FileSystem> fs_;
  std::string bucket_;
  std::string prefix_;
};

}

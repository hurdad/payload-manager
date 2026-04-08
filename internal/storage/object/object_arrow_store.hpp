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
  ObjectArrowStore(std::shared_ptr<arrow::fs::FileSystem> fs, std::string root_path, bool is_s3 = false);
  ~ObjectArrowStore() override;

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size_bytes) override;

  std::shared_ptr<arrow::Buffer> Read(const payload::manager::v1::PayloadID& id) override;

  uint64_t Size(const payload::manager::v1::PayloadID& id) override;

  void Write(const payload::manager::v1::PayloadID& id, const std::shared_ptr<arrow::Buffer>& buffer, bool fsync) override;

  void Remove(const payload::manager::v1::PayloadID& id) override;

  void WriteSidecar(const payload::manager::v1::PayloadID& id, const payload::manager::catalog::v1::PayloadArchiveMetadata& meta) override;

  payload::manager::v1::Tier TierType() const override {
    return payload::manager::v1::TIER_OBJECT;
  }

  // Returns a URI clients can pass to arrow::fs::FileSystemFromUri to upload
  // payload bytes directly. For S3: "s3://<root_path>/<uuid>.bin".
  // For non-S3 backends the raw filesystem path is returned unchanged.
  std::string GetUploadUri(const payload::manager::v1::PayloadID& id) const;

 private:
  std::string ObjectPath(const payload::manager::v1::PayloadID& id) const;
  std::string SidecarObjectPath(const payload::manager::v1::PayloadID& id) const;

  std::shared_ptr<arrow::fs::FileSystem> fs_;
  std::string                            root_path_;
  bool                                   is_s3_;
};

} // namespace payload::storage

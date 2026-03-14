#pragma once

#include <arrow/buffer.h>

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "internal/storage/storage_backend.hpp"
#include "payload/manager/v1.hpp"

namespace payload::storage {

/*
  RAM storage tier.

  Backed by POSIX shared memory (shm_open / mmap) so that both the server
  and local C++ clients can map the same pages without copying.

  Each payload gets its own shm segment named "/<prefix>-<uuid-hex>".

  Thread safety:
    - shared reads
    - exclusive writes
*/

class RamArrowStore final : public StorageBackend {
 public:
  explicit RamArrowStore(std::string shm_prefix = "pm") : shm_prefix_(std::move(shm_prefix)) {
  }
  ~RamArrowStore() override = default;

  // StorageBackend interface
  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size_bytes) override;

  std::shared_ptr<arrow::Buffer> Read(const payload::manager::v1::PayloadID& id) override;

  void Write(const payload::manager::v1::PayloadID& id, const std::shared_ptr<arrow::Buffer>& buffer, bool fsync) override;

  void Remove(const payload::manager::v1::PayloadID& id) override;

  payload::manager::v1::Tier TierType() const override {
    return payload::manager::v1::TIER_RAM;
  }

  // Returns the POSIX shm segment name for a payload ID (starts with '/').
  // Format: /<prefix>-<uuid>.
  std::string ShmName(const payload::manager::v1::PayloadID& id) const;

  // Static overload for use when only a prefix string is available.
  static std::string ShmName(const payload::manager::v1::PayloadID& id, const std::string& prefix);

  const std::string& GetShmPrefix() const {
    return shm_prefix_;
  }

 private:
  using UUID = std::string;

  static UUID Key(const payload::manager::v1::PayloadID& id);

  // Open (or create) a shm segment, mmap it, and return an Arrow buffer.
  // writable=true → O_CREAT|O_RDWR + ftruncate; false → O_RDONLY.
  static std::shared_ptr<arrow::Buffer> OpenShm(const std::string& name, size_t size_bytes, bool writable);

  std::string shm_prefix_;

  mutable std::shared_mutex                                mutex_;
  std::unordered_map<UUID, std::shared_ptr<arrow::Buffer>> buffers_;
};

} // namespace payload::storage

#pragma once

#include <arrow/buffer.h>

#include <chrono>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

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

  /*
    Free bytes on the tmpfs backing /dev/shm, cached briefly.

    The configured RAM capacity only bounds what this process has handed out.
    Anything else sharing the tmpfs — another container on the same ipc
    namespace, a leaked mapping — consumes it invisibly, and the shortfall only
    shows up as SIGBUS in whichever process writes next. Consulting the real
    free space closes that gap.

    Cached for kCacheTtl so a burst of allocations does not syscall per request.
  */
  std::optional<uint64_t> AvailableBytes() const override;

  // Total size of the tmpfs backing /dev/shm, or nullopt if it cannot be read.
  static std::optional<uint64_t> ShmTotalBytes();

  /*
    Unlink shm segments belonging to this prefix that no longer correspond to a
    known payload, and return how many were removed.

    A crashed or killed container leaves its /dev/shm/<prefix>-* segments
    behind. Nothing in this process's accounting knows about them, so it keeps
    accepting allocations until the tmpfs is full — at which point shm_open,
    ftruncate and mmap all still succeed (they only touch metadata and address
    space) and the *producer* takes SIGBUS on its first write. Downstream saw
    7.7G/7.7G with 10,329 orphans.

    `known_uuid_hex` must contain every payload the repository knows about, not
    only TIER_RAM ones: a payload part-way through a spill still owns its
    segment. Passing an incomplete set deletes live data.

    Existing client mappings keep working until they munmap, per POSIX.

    Note Read() deliberately relies on segments surviving a restart, so this
    must run against the repository's set rather than unlinking blindly.
  */
  std::size_t PurgeOrphans(const std::unordered_set<std::string>& known_uuid_hex);

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

  // statvfs is cheap but not free, and Allocate can be called in tight bursts.
  static constexpr std::chrono::milliseconds    kCacheTtl{100};
  mutable std::mutex                            avail_guard_;
  mutable std::chrono::steady_clock::time_point avail_checked_at_{};
  mutable std::optional<uint64_t>               avail_cached_{};
};

} // namespace payload::storage

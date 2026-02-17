#pragma once

#include <arrow/buffer.h>

#include <memory>
#include <string>

#include "payload/manager/core/v1/id.pb.h"
#include "payload/manager/core/v1/types.pb.h"
#include "payload/manager/v1.hpp"

namespace payload::storage {

/*
  Tier storage abstraction.

  Every payload is represented as an Arrow Buffer.
  The manager never manipulates raw pointers — only buffers.

  Implementations:
    RAM      → in-memory Arrow buffers
    DISK     → Arrow file IO
    OBJECT   → Arrow S3 / MinIO
    GPU      → (future) Arrow CUDA buffers
*/

class StorageBackend {
 public:
  virtual ~StorageBackend() = default;

  // ------------------------------------------------------------------
  // Allocate
  // ------------------------------------------------------------------
  /*
    Allocate writable storage in this tier.

    Returns a mutable buffer the caller writes into.
    Only valid for writable tiers (RAM/GPU).
  */
  virtual std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size_bytes) = 0;

  // ------------------------------------------------------------------
  // Read
  // ------------------------------------------------------------------
  /*
    Read entire payload into an Arrow buffer.

    Implementations may mmap / zero-copy when possible.
  */
  virtual std::shared_ptr<arrow::Buffer> Read(const payload::manager::v1::PayloadID& id) = 0;

  // ------------------------------------------------------------------
  // Size
  // ------------------------------------------------------------------
  /*
    Return payload size in bytes.

    Backends with cheap metadata lookups (disk/object) should override this.
    The default implementation falls back to Read() and inspects buffer size.
  */
  virtual uint64_t Size(const payload::manager::v1::PayloadID& id) {
    return static_cast<uint64_t>(Read(id)->size());
  }

  // ------------------------------------------------------------------
  // Write
  // ------------------------------------------------------------------
  /*
    Persist a buffer into this tier.

    Used for:
      spill RAM → DISK
      promote DISK → RAM
      replicate DISK → OBJECT
  */
  virtual void Write(const payload::manager::v1::PayloadID& id, const std::shared_ptr<arrow::Buffer>& buffer, bool fsync) = 0;

  // ------------------------------------------------------------------
  // Delete
  // ------------------------------------------------------------------
  /*
    Remove bytes from this tier.

    Called after eviction or delete.
  */
  virtual void Remove(const payload::manager::v1::PayloadID& id) = 0;

  // ------------------------------------------------------------------
  // Tier type
  // ------------------------------------------------------------------
  virtual payload::manager::v1::Tier TierType() const = 0;
};

using StorageBackendPtr = std::shared_ptr<StorageBackend>;

} // namespace payload::storage

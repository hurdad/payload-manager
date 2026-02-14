#include "ram_arrow_store.hpp"

#include <arrow/buffer.h>
#include <arrow/memory_pool.h>

#include <stdexcept>

#include "payload/manager/v1.hpp"

namespace payload::storage {

using namespace payload::manager::v1;

std::string RamArrowStore::Key(const PayloadID& id) {
  return id.value();
}

/*
  Allocate a writable buffer for producer.
  This is the SDR ingest path typically.
*/
std::shared_ptr<arrow::Buffer> RamArrowStore::Allocate(const PayloadID& id, uint64_t size_bytes) {
  auto result = arrow::AllocateResizableBuffer(size_bytes);
  if (!result.ok()) throw std::runtime_error(result.status().ToString());

  std::shared_ptr<arrow::Buffer> buf = *result;

  {
    std::unique_lock lock(mutex_);
    buffers_[Key(id)] = buf;
  }

  return buf;
}

/*
  Zero-copy read.
*/
std::shared_ptr<arrow::Buffer> RamArrowStore::Read(const PayloadID& id) {
  std::shared_lock lock(mutex_);

  auto it = buffers_.find(Key(id));
  if (it == buffers_.end()) throw std::runtime_error("RAM payload not found");

  return it->second;
}

/*
  Used during promotion (DISK -> RAM) or replication.
*/
void RamArrowStore::Write(const PayloadID& id, const std::shared_ptr<arrow::Buffer>& buffer, bool /*fsync unused*/) {
  std::unique_lock lock(mutex_);
  buffers_[Key(id)] = buffer;
}

/*
  Eviction or delete.
*/
void RamArrowStore::Remove(const PayloadID& id) {
  std::unique_lock lock(mutex_);
  buffers_.erase(Key(id));
}

} // namespace payload::storage

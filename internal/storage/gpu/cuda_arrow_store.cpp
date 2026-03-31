#include "cuda_arrow_store.hpp"

#include <arrow/buffer.h>
#include <arrow/result.h>

#include <stdexcept>

#include "payload/manager/v1.hpp"

namespace payload::storage {

using namespace payload::manager::v1;

std::string CudaArrowStore::Key(const PayloadID& id) {
  return id.value();
}

CudaArrowStore::CudaArrowStore(int device_id) : ctx_(CudaContextManager::Get(device_id)) {
}

/*
  Allocate GPU memory
*/
std::shared_ptr<arrow::Buffer> CudaArrowStore::Allocate(const PayloadID& id, uint64_t size_bytes) {
  auto maybe = ctx_->Allocate(size_bytes);
  if (!maybe.ok()) throw std::runtime_error(maybe.status().ToString());

  std::shared_ptr<arrow::cuda::CudaBuffer> buf = std::move(*maybe);

  {
    std::unique_lock lock(mutex_);
    buffers_[Key(id)] = buf;
  }

  return buf;
}

/*
  Copy GPU → host and return a host-accessible buffer.
  Callers (spill, etc.) expect a CPU-readable buffer.
*/
std::shared_ptr<arrow::Buffer> CudaArrowStore::Read(const PayloadID& id) {
  std::shared_lock lock(mutex_);

  auto it = buffers_.find(Key(id));
  if (it == buffers_.end()) throw std::runtime_error("GPU payload not found");

  auto maybe_host = arrow::AllocateBuffer(it->second->size());
  if (!maybe_host.ok()) throw std::runtime_error("Host allocate failed: " + maybe_host.status().ToString());

  auto copy_status = it->second->CopyToHost(/*position=*/0, it->second->size(), (*maybe_host)->mutable_data());
  if (!copy_status.ok()) throw std::runtime_error("GPU device-to-host copy failed: " + copy_status.ToString());

  return std::move(*maybe_host);
}

/*
  Copy CPU → GPU
*/
void CudaArrowStore::Write(const PayloadID& id, const std::shared_ptr<arrow::Buffer>& buffer, bool /*fsync*/) {
  auto maybe_buf = ctx_->Allocate(buffer->size());
  if (!maybe_buf.ok()) throw std::runtime_error("GPU allocate failed: " + maybe_buf.status().ToString());
  std::shared_ptr<arrow::cuda::CudaBuffer> gpu_buf = std::move(*maybe_buf);

  auto copy_status = gpu_buf->CopyFromHost(/*position=*/0, buffer->data(), buffer->size());
  if (!copy_status.ok()) throw std::runtime_error("GPU host-to-device copy failed: " + copy_status.ToString());

  std::unique_lock lock(mutex_);
  buffers_[Key(id)] = gpu_buf;
}

/*
  Free GPU memory
*/
void CudaArrowStore::Remove(const PayloadID& id) {
  std::unique_lock lock(mutex_);
  buffers_.erase(Key(id));
}

/*
  Export IPC handle for cross-process consumers
*/
std::shared_ptr<arrow::cuda::CudaIpcMemHandle> CudaArrowStore::ExportIPC(const PayloadID& id) {
  std::shared_lock lock(mutex_);

  auto it = buffers_.find(Key(id));
  if (it == buffers_.end()) throw std::runtime_error("GPU payload not found");

  auto maybe = it->second->ExportForIpc();
  if (!maybe.ok()) throw std::runtime_error(maybe.status().ToString());

  return *maybe;
}

} // namespace payload::storage

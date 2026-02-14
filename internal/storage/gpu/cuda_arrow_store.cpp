#include "cuda_arrow_store.hpp"

#include <arrow/result.h>
#include <stdexcept>
#include "payload/manager/v1_compat.hpp"

namespace payload::storage {

using namespace payload::manager::v1;

std::string CudaArrowStore::Key(const PayloadID& id) {
    return id.value();
}

CudaArrowStore::CudaArrowStore(int device_id)
    : ctx_(CudaContextManager::Get(device_id)) {}

/*
  Allocate GPU memory
*/
std::shared_ptr<arrow::Buffer>
CudaArrowStore::Allocate(const PayloadID& id, uint64_t size_bytes) {

    auto maybe = ctx_->Allocate(size_bytes);
    if (!maybe.ok())
        throw std::runtime_error(maybe.status().ToString());

    auto buf = *maybe;

    {
        std::unique_lock lock(mutex_);
        buffers_[Key(id)] = buf;
    }

    return buf;
}

/*
  Returns GPU buffer handle (caller must know tier)
*/
std::shared_ptr<arrow::Buffer>
CudaArrowStore::Read(const PayloadID& id) {

    std::shared_lock lock(mutex_);

    auto it = buffers_.find(Key(id));
    if (it == buffers_.end())
        throw std::runtime_error("GPU payload not found");

    return it->second;
}

/*
  Copy CPU → GPU
*/
void CudaArrowStore::Write(const PayloadID& id,
                           const std::shared_ptr<arrow::Buffer>& buffer,
                           bool /*fsync*/) {

    auto gpu_buf = ctx_->Allocate(buffer->size()).ValueOrDie();
    ctx_->CopyHostToDevice(buffer->data(), gpu_buf->mutable_data(), buffer->size()).ValueOrDie();

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
std::shared_ptr<arrow::cuda::CudaIpcMemHandle>
CudaArrowStore::ExportIPC(const PayloadID& id) {

    std::shared_lock lock(mutex_);

    auto it = buffers_.find(Key(id));
    if (it == buffers_.end())
        throw std::runtime_error("GPU payload not found");

    auto maybe = it->second->ExportForIpc();
    if (!maybe.ok())
        throw std::runtime_error(maybe.status().ToString());

    return *maybe;
}

}

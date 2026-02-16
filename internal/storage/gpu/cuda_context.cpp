#include "cuda_context.hpp"

#include <arrow/result.h>

#include <stdexcept>

namespace payload::storage {

std::mutex                                CudaContextManager::mutex_;
std::shared_ptr<arrow::cuda::CudaContext> CudaContextManager::ctx_;
int                                       CudaContextManager::device_id_{-1};

std::shared_ptr<arrow::cuda::CudaContext> CudaContextManager::Get(int device_id) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (ctx_) {
    if (device_id_ != device_id) {
      throw std::runtime_error("CudaContextManager already initialized for device " + std::to_string(device_id_) +
                               ", cannot reinitialize for device " + std::to_string(device_id));
    }
    return ctx_;
  }

  auto maybe_manager = arrow::cuda::CudaDeviceManager::Instance();
  if (!maybe_manager.ok()) throw std::runtime_error(maybe_manager.status().ToString());

  auto maybe = (*maybe_manager)->GetContext(device_id);
  if (!maybe.ok()) throw std::runtime_error(maybe.status().ToString());

  ctx_       = *maybe;
  device_id_ = device_id;
  return ctx_;
}

} // namespace payload::storage

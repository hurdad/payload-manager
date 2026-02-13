#include "cuda_context.hpp"
#include <arrow/result.h>
#include <stdexcept>

namespace payload::storage {

std::shared_ptr<arrow::cuda::CudaContext> CudaContextManager::ctx_;

std::shared_ptr<arrow::cuda::CudaContext>
CudaContextManager::Get(int device_id) {

  if (ctx_)
    return ctx_;

  auto maybe = arrow::cuda::CudaDeviceManager::Instance()->GetContext(device_id);
  if (!maybe.ok())
    throw std::runtime_error(maybe.status().ToString());

  ctx_ = *maybe;
  return ctx_;
}

}

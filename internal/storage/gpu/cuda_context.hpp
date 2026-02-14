#pragma once

#include <arrow/cuda/api.h>

#include <memory>

namespace payload::storage {

/*
  Singleton CUDA context wrapper.

  We keep one primary context per process.
  Arrow requires context lifetime >= all buffers.
*/

class CudaContextManager {
 public:
  static std::shared_ptr<arrow::cuda::CudaContext> Get(int device_id = 0);

 private:
  static std::shared_ptr<arrow::cuda::CudaContext> ctx_;
};

} // namespace payload::storage

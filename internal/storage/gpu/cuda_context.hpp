#pragma once

#include <arrow/gpu/cuda_api.h>

#include <memory>
#include <mutex>

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
  static std::mutex                                mutex_;
  static std::shared_ptr<arrow::cuda::CudaContext> ctx_;
  static int                                       device_id_;
};

} // namespace payload::storage
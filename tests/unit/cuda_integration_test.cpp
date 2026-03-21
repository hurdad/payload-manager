#include <arrow/buffer.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

#include "internal/storage/gpu/cuda_arrow_store.hpp"
#include "internal/storage/gpu/cuda_context.hpp"

namespace {

using payload::manager::v1::PayloadID;
using payload::storage::CudaArrowStore;
using payload::storage::CudaContextManager;

PayloadID MakePayloadID(const std::string& value) {
  PayloadID id;
  id.set_value(value);
  return id;
}

bool HasCudaDevice() {
  auto maybe_manager = arrow::cuda::CudaDeviceManager::Instance();
  if (!maybe_manager.ok()) {
    return false;
  }

  auto maybe_ctx = (*maybe_manager)->GetContext(0);
  return maybe_ctx.ok();
}

} // namespace

TEST(CudaIntegration, ContextManagerReturnsSingletonContext) {
  if (!HasCudaDevice()) {
    GTEST_SKIP() << "CUDA context unavailable";
  }

  const auto first  = CudaContextManager::Get(0);
  const auto second = CudaContextManager::Get(0);

  EXPECT_TRUE(first != nullptr);
  EXPECT_TRUE(second != nullptr);
  EXPECT_EQ(first.get(), second.get());
}

TEST(CudaIntegration, AllocateReadExportAndRemoveLifecycle) {
  if (!HasCudaDevice()) {
    GTEST_SKIP() << "CUDA context unavailable";
  }

  CudaArrowStore store(0);
  const auto     id = MakePayloadID("cuda-lifecycle");

  constexpr int64_t size_bytes = 32;
  const auto        allocated  = store.Allocate(id, size_bytes);
  ASSERT_TRUE(allocated != nullptr);
  EXPECT_EQ(allocated->size(), size_bytes);

  const auto read = store.Read(id);
  ASSERT_TRUE(read != nullptr);
  EXPECT_EQ(read.get(), allocated.get());

  const auto ipc = store.ExportIPC(id);
  EXPECT_TRUE(ipc != nullptr);

  store.Remove(id);

  EXPECT_THROW((void)store.Read(id), std::runtime_error);
}

TEST(CudaIntegration, WriteCopiesHostBufferToDeviceBuffer) {
  if (!HasCudaDevice()) {
    GTEST_SKIP() << "CUDA context unavailable";
  }

  CudaArrowStore store(0);
  const auto     id = MakePayloadID("cuda-write");

  auto maybe_host_buffer = arrow::AllocateBuffer(4);
  ASSERT_TRUE(maybe_host_buffer.ok());
  std::shared_ptr<arrow::Buffer> host_buffer(std::move(*maybe_host_buffer));
  host_buffer->mutable_data()[0] = static_cast<uint8_t>(1);
  host_buffer->mutable_data()[1] = static_cast<uint8_t>(2);
  host_buffer->mutable_data()[2] = static_cast<uint8_t>(3);
  host_buffer->mutable_data()[3] = static_cast<uint8_t>(4);

  store.Write(id, host_buffer, false);
  const auto device_buffer = store.Read(id);
  ASSERT_TRUE(device_buffer != nullptr);
  EXPECT_EQ(device_buffer->size(), host_buffer->size());

  std::vector<uint8_t> round_trip(static_cast<size_t>(host_buffer->size()), 0);
  const auto           cuda_buffer = std::dynamic_pointer_cast<arrow::cuda::CudaBuffer>(device_buffer);
  ASSERT_TRUE(cuda_buffer != nullptr);
  const auto copy_status = cuda_buffer->CopyToHost(/*position=*/0, cuda_buffer->size(), round_trip.data());
  ASSERT_TRUE(copy_status.ok());

  EXPECT_EQ(round_trip[0], 1u);
  EXPECT_EQ(round_trip[1], 2u);
  EXPECT_EQ(round_trip[2], 3u);
  EXPECT_EQ(round_trip[3], 4u);
}

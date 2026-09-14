#include <arrow/buffer.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <stdexcept>

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

  // Read copies device to host and hands back a fresh CPU-readable buffer —
  // see the contract on CudaArrowStore::Read, which spill depends on. So it is
  // deliberately not the buffer Allocate returned, and deliberately not a
  // CudaBuffer. This used to assert pointer equality, which the implementation
  // could never satisfy.
  const auto read = store.Read(id);
  ASSERT_TRUE(read != nullptr);
  EXPECT_NE(read.get(), allocated.get());
  EXPECT_EQ(read->size(), size_bytes);
  EXPECT_EQ(std::dynamic_pointer_cast<arrow::cuda::CudaBuffer>(read), nullptr);

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

  // Read has already done the device-to-host copy, so the bytes are readable
  // directly. The previous version cast this to CudaBuffer and called
  // CopyToHost on it, which cannot work: the cast yields null because the
  // buffer is host memory by the time Read returns.
  const auto round_trip = store.Read(id);
  ASSERT_TRUE(round_trip != nullptr);
  EXPECT_EQ(round_trip->size(), host_buffer->size());
  EXPECT_EQ(std::dynamic_pointer_cast<arrow::cuda::CudaBuffer>(round_trip), nullptr);

  ASSERT_EQ(round_trip->size(), 4);
  EXPECT_EQ(round_trip->data()[0], 1u);
  EXPECT_EQ(round_trip->data()[1], 2u);
  EXPECT_EQ(round_trip->data()[2], 3u);
  EXPECT_EQ(round_trip->data()[3], 4u);

  // The stored payload is still device memory — only Read's return value is
  // on the host. Exporting an IPC handle proves the device side survived.
  EXPECT_TRUE(store.ExportIPC(id) != nullptr);
}

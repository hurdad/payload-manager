#include <arrow/buffer.h>

#include <cassert>
#include <cstdint>
#include <iostream>
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

void TestContextManagerReturnsSingletonContext() {
  const auto first  = CudaContextManager::Get(0);
  const auto second = CudaContextManager::Get(0);

  assert(first);
  assert(second);
  assert(first.get() == second.get());
}

void TestAllocateReadExportAndRemoveLifecycle() {
  CudaArrowStore store(0);
  const auto     id = MakePayloadID("cuda-lifecycle");

  constexpr int64_t size_bytes = 32;
  const auto        allocated  = store.Allocate(id, size_bytes);
  assert(allocated);
  assert(allocated->size() == size_bytes);

  const auto read = store.Read(id);
  assert(read);
  assert(read.get() == allocated.get());

  const auto ipc = store.ExportIPC(id);
  assert(ipc);

  store.Remove(id);

  bool threw = false;
  try {
    (void)store.Read(id);
  } catch (const std::runtime_error&) {
    threw = true;
  }

  assert(threw);
}

void TestWriteCopiesHostBufferToDeviceBuffer() {
  CudaArrowStore store(0);
  const auto     id = MakePayloadID("cuda-write");

  auto maybe_host_buffer = arrow::AllocateBuffer(4);
  assert(maybe_host_buffer.ok());
  std::shared_ptr<arrow::Buffer> host_buffer(std::move(*maybe_host_buffer));
  host_buffer->mutable_data()[0] = static_cast<uint8_t>(1);
  host_buffer->mutable_data()[1] = static_cast<uint8_t>(2);
  host_buffer->mutable_data()[2] = static_cast<uint8_t>(3);
  host_buffer->mutable_data()[3] = static_cast<uint8_t>(4);

  store.Write(id, host_buffer, false);
  const auto device_buffer = store.Read(id);
  assert(device_buffer);
  assert(device_buffer->size() == host_buffer->size());

  std::vector<uint8_t> round_trip(static_cast<size_t>(host_buffer->size()), 0);
  const auto           cuda_buffer = std::dynamic_pointer_cast<arrow::cuda::CudaBuffer>(device_buffer);
  assert(cuda_buffer);
  const auto copy_status = cuda_buffer->CopyToHost(/*position=*/0, cuda_buffer->size(), round_trip.data());
  assert(copy_status.ok());

  assert(round_trip[0] == 1);
  assert(round_trip[1] == 2);
  assert(round_trip[2] == 3);
  assert(round_trip[3] == 4);
}

} // namespace

int main() {
  if (!HasCudaDevice()) {
    std::cout << "payload_manager_unit_cuda_integration: skipped (CUDA context unavailable)\n";
    return 0;
  }

  TestContextManagerReturnsSingletonContext();
  TestAllocateReadExportAndRemoveLifecycle();
  TestWriteCopiesHostBufferToDeviceBuffer();

  std::cout << "payload_manager_unit_cuda_integration: pass\n";
  return 0;
}

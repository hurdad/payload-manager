#include <cuda.h>   // CUDA driver API — same context layer Arrow CUDA uses internally

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "client/cpp/payload_manager_client.h"
#include "example_util.hpp"
#include "otel_tracer.hpp"
#include "payload/manager/v1.hpp"
#include "traced_channel.hpp"

namespace {

bool CuCheck(CUresult result, const char* op) {
  if (result == CUDA_SUCCESS) return true;
  const char* err_str = "unknown";
  cuGetErrorString(result, &err_str);
  std::cerr << op << " failed: " << err_str << '\n';
  return false;
}

} // namespace

int main(int argc, char** argv) {
  // argv[1]: server endpoint    (default localhost:50051)
  // argv[2]: OTLP gRPC endpoint (default localhost:4317, empty to disable)
  const std::string target  = argc > 1 ? argv[1] : "localhost:50051";
  const std::string otlp_ep = argc > 2 ? argv[2] : "localhost:4317";

  OtelInit(otlp_ep, "cpp-examples");
  auto                                    channel = StartSpanAndMakeChannel(target, "gpu_example");
  payload::manager::client::PayloadClient client(channel);

  constexpr uint64_t kSize = 64;

  // --- Initialise the CUDA primary context and make it current ---
  // Arrow CUDA uses cuDevicePrimaryCtxRetain internally; we retain the same
  // context here so that cuMemcpy* calls below share the correct context.
  // This must happen before AllocateWritableBuffer() so the context is active
  // for the full lifetime of the opened IPC buffers.
  if (!CuCheck(cuInit(0), "cuInit")) { OtelShutdown(); return 1; }
  CUdevice cu_device{};
  if (!CuCheck(cuDeviceGet(&cu_device, 0), "cuDeviceGet")) { OtelShutdown(); return 1; }
  CUcontext cu_ctx{};
  if (!CuCheck(cuDevicePrimaryCtxRetain(&cu_ctx, cu_device), "cuDevicePrimaryCtxRetain")) { OtelShutdown(); return 1; }
  if (!CuCheck(cuCtxSetCurrent(cu_ctx), "cuCtxSetCurrent")) { cuDevicePrimaryCtxRelease(cu_device); OtelShutdown(); return 1; }

  // --- Allocate a payload on the GPU tier ---
  // The client opens the CUDA IPC handle via Arrow's driver-API context and
  // returns a MutableBuffer whose mutable_data() is a CUdeviceptr.
  auto writable = client.AllocateWritableBuffer(kSize, payload::manager::v1::TIER_GPU);
  if (!writable.ok()) {
    std::cerr << "AllocateWritableBuffer(GPU) failed: " << writable.status().ToString() << '\n';
    cuDevicePrimaryCtxRelease(cu_device);
    OtelShutdown();
    return 1;
  }
  auto& wp = writable.ValueOrDie();

  // --- Write an incrementing pattern from host → GPU device memory ---
  // Use the CUDA driver API (cuMemcpyHtoD) to stay in the same driver context
  // that Arrow CUDA uses internally; the runtime API cudaMemcpy operates in a
  // separate primary context and cannot address the IPC-opened device pointer.
  std::vector<uint8_t> host_src(kSize);
  for (uint64_t i = 0; i < kSize; ++i) {
    host_src[i] = static_cast<uint8_t>(i & 0xFFu);
  }

  const auto dev_write = reinterpret_cast<CUdeviceptr>(wp.buffer->mutable_data());
  if (!CuCheck(cuMemcpyHtoD(dev_write, host_src.data(), kSize), "cuMemcpyHtoD")) {
    cuDevicePrimaryCtxRelease(cu_device);
    OtelShutdown();
    return 1;
  }

  // --- Commit so the payload is visible to readers ---
  const auto& payload_id    = wp.descriptor.payload_id();
  const auto  uuid_text     = payload::examples::UuidToHex(payload_id.value());
  auto        commit_status = client.CommitPayload(payload_id);
  if (!commit_status.ok()) {
    std::cerr << "CommitPayload failed: " << commit_status.ToString() << '\n';
    cuDevicePrimaryCtxRelease(cu_device);
    OtelShutdown();
    return 1;
  }

  // --- Acquire a read lease (stays on GPU tier) ---
  auto readable = client.AcquireReadableBuffer(payload_id);
  if (!readable.ok()) {
    std::cerr << "AcquireReadableBuffer failed: " << readable.status().ToString() << '\n';
    cuDevicePrimaryCtxRelease(cu_device);
    OtelShutdown();
    return 1;
  }
  auto& rp = readable.ValueOrDie();

  // --- Copy GPU device memory → host for verification ---
  std::vector<uint8_t> host_dst(kSize);
  // buffer->data() returns nullptr for GPU buffers (is_cpu_=false); use address() instead.
  const auto           dev_read = static_cast<CUdeviceptr>(rp.buffer->address());
  if (!CuCheck(cuMemcpyDtoH(host_dst.data(), dev_read, kSize), "cuMemcpyDtoH")) {
    client.Release(rp.lease_id);
    cuDevicePrimaryCtxRelease(cu_device);
    OtelShutdown();
    return 1;
  }

  std::cout << "GPU payload UUID=" << uuid_text << ", size=" << rp.buffer->size() << " bytes\n";

  int mismatches = 0;
  for (uint64_t i = 0; i < kSize; ++i) {
    const uint8_t expected = static_cast<uint8_t>(i & 0xFFu);
    if (host_dst[i] != expected) {
      std::cerr << "mismatch at byte " << i << ": expected " << static_cast<int>(expected)
                << " got " << static_cast<int>(host_dst[i]) << '\n';
      ++mismatches;
    }
  }

  if (mismatches == 0) {
    std::cout << "verify: OK (" << kSize << " bytes match incrementing sequence)\n";
  } else {
    std::cout << "verify: FAIL (" << mismatches << " mismatches)\n";
  }

  // --- Release the lease ---
  auto release_status = client.Release(rp.lease_id);
  if (!release_status.ok()) {
    std::cerr << "Release failed: " << release_status.ToString() << '\n';
    cuDevicePrimaryCtxRelease(cu_device);
    OtelShutdown();
    return 1;
  }

  OtelEndSpan();
  cuDevicePrimaryCtxRelease(cu_device);
  OtelShutdown();
  return mismatches == 0 ? 0 : 1;
}

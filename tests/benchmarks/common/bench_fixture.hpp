#pragma once

#include <arrow/buffer.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/storage/ram/ram_arrow_store.hpp"
#include "internal/storage/storage_backend.hpp"
#include "payload/manager/v1.hpp"

namespace payload::bench {

// ---------------------------------------------------------------------------
// CopyingMemoryBackend
//
// Fake "disk" tier that performs a full memcpy on Write (mirroring the data
// movement cost of a real spill) but stays in memory. This is used by
// BenchFixture so benchmarks can drive PayloadManager::ExecuteSpill without
// hitting the filesystem — which has a binary PayloadID encoding issue
// (raw UUID bytes can contain \0, which DiskArrowStore rejects).
//
// Use this to measure: DB tx overhead + mutex contention + copy cost.
// Use spill_bench's BenchRawDiskWrite to measure raw filesystem throughput.
// ---------------------------------------------------------------------------
class CopyingMemoryBackend final : public payload::storage::StorageBackend {
 public:
  explicit CopyingMemoryBackend(payload::manager::v1::Tier tier) : tier_(tier) {
  }

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size_bytes) override {
    auto result = arrow::AllocateResizableBuffer(size_bytes);
    if (!result.ok()) throw std::runtime_error(result.status().ToString());
    std::shared_ptr<arrow::Buffer> buf(std::move(*result));
    std::unique_lock               lock(mutex_);
    buffers_[id.value()] = buf;
    return buf;
  }

  std::shared_ptr<arrow::Buffer> Read(const payload::manager::v1::PayloadID& id) override {
    std::shared_lock lock(mutex_);
    auto             it = buffers_.find(id.value());
    if (it == buffers_.end()) throw std::runtime_error("CopyingMemoryBackend: payload not found");
    return it->second;
  }

  void Write(const payload::manager::v1::PayloadID& id, const std::shared_ptr<arrow::Buffer>& src, bool /*fsync*/) override {
    // Full copy — simulates the data movement cost of a disk/object write.
    auto result = arrow::AllocateResizableBuffer(src->size());
    if (!result.ok()) throw std::runtime_error(result.status().ToString());
    auto copy = std::move(*result);
    if (src->size() > 0) std::memcpy(copy->mutable_data(), src->data(), static_cast<size_t>(src->size()));
    std::unique_lock lock(mutex_);
    buffers_[id.value()] = std::shared_ptr<arrow::Buffer>(std::move(copy));
  }

  void Remove(const payload::manager::v1::PayloadID& id) override {
    std::unique_lock lock(mutex_);
    buffers_.erase(id.value());
  }

  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }

 private:
  payload::manager::v1::Tier                                      tier_;
  mutable std::shared_mutex                                       mutex_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> buffers_;
};

// ---------------------------------------------------------------------------
// BenchFixture
//
// Wires a PayloadManager with a real RamArrowStore (source) and a
// CopyingMemoryBackend (destination) to isolate spill pipeline overhead
// from filesystem latency.
// ---------------------------------------------------------------------------
struct BenchFixture {
  std::shared_ptr<payload::core::PayloadManager> manager;

  explicit BenchFixture() {
    payload::storage::StorageFactory::TierMap storage;
    storage[payload::manager::v1::TIER_RAM]  = std::make_shared<payload::storage::RamArrowStore>();
    storage[payload::manager::v1::TIER_DISK] = std::make_shared<CopyingMemoryBackend>(payload::manager::v1::TIER_DISK);

    auto repo  = std::make_shared<payload::db::memory::MemoryRepository>();
    auto lease = std::make_shared<payload::lease::LeaseManager>();

    manager = std::make_shared<payload::core::PayloadManager>(std::move(storage), lease, repo);
  }

  // Allocate a payload in RAM and commit it.
  payload::manager::v1::PayloadDescriptor MakeRamPayload(uint64_t size_bytes) {
    return manager->Commit(manager->Allocate(size_bytes, payload::manager::v1::TIER_RAM).payload_id());
  }
};

// ---------------------------------------------------------------------------
// Timing helpers
// ---------------------------------------------------------------------------

struct BenchResult {
  std::string name;
  size_t      payload_bytes;
  int         iterations;
  double      total_ns;

  double per_op_us() const {
    return total_ns / iterations / 1e3;
  }
  double throughput_mbs() const {
    if (payload_bytes == 0) return 0.0;
    double total_bytes = static_cast<double>(payload_bytes) * iterations;
    double total_s     = total_ns / 1e9;
    return total_bytes / total_s / (1024.0 * 1024.0);
  }
};

inline void PrintHeader() {
  std::cout << std::left << std::setw(44) << "benchmark" << std::setw(10) << "size" << std::setw(8) << "iters" << std::setw(14) << "per-op (µs)"
            << std::setw(16) << "throughput MB/s" << "\n"
            << std::string(92, '-') << "\n";
}

inline void PrintResult(const BenchResult& r) {
  auto size_str = [](size_t b) -> std::string {
    if (b >= 1024 * 1024) return std::to_string(b / (1024 * 1024)) + " MB";
    if (b >= 1024) return std::to_string(b / 1024) + " KB";
    if (b == 0) return "-";
    return std::to_string(b) + " B";
  };

  std::string throughput = r.payload_bytes > 0 ? (std::ostringstream{} << std::fixed << std::setprecision(1) << r.throughput_mbs()).str() : "-";

  std::cout << std::left << std::setw(44) << r.name << std::setw(10) << size_str(r.payload_bytes) << std::setw(8) << r.iterations << std::setw(14)
            << std::fixed << std::setprecision(2) << r.per_op_us() << std::setw(16) << throughput << "\n";
}

// Run fn() for `iterations` calls, preceded by `warmup` uncounted calls.
inline BenchResult TimedRun(const std::string& name, size_t payload_bytes, int iterations, const std::function<void()>& fn, int warmup = 3) {
  for (int i = 0; i < std::min(warmup, iterations); ++i) fn();

  auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < iterations; ++i) fn();
  auto t1 = std::chrono::steady_clock::now();

  BenchResult r;
  r.name          = name;
  r.payload_bytes = payload_bytes;
  r.iterations    = iterations;
  r.total_ns      = static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
  return r;
}

// Choose an iteration count targeting ~target_bytes total data movement.
// max_iters caps the count so benchmarks don't run too long.
inline int IterationsFor(size_t payload_bytes, size_t target_bytes = 64ULL * 1024 * 1024, int max_iters = 2000) {
  if (payload_bytes == 0) return max_iters;
  return std::clamp(static_cast<int>(target_bytes / payload_bytes), 8, max_iters);
}

} // namespace payload::bench

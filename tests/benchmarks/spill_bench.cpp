/*
  spill_bench.cpp

  Two complementary measurements:

  1. ExecuteSpill pipeline (PayloadManager + CopyingMemoryBackend)
     Measures: DB tx round-trip + mutex acquire + buffer copy + snapshot cache update.
     The "disk" tier is an in-memory copy backend, so this isolates application
     overhead from filesystem latency.

  2. Raw DiskArrowStore write/read (no PayloadManager)
     Measures: actual filesystem throughput via Arrow FileOutputStream.
     Uses a fixed hex ID to avoid the binary PayloadID encoding issue.
     Run this to characterize the storage device independently of the manager.
*/

#include <filesystem>
#include <iostream>
#include <vector>

#include "common/bench_fixture.hpp"
#include "internal/storage/disk/disk_arrow_store.hpp"
#include "payload/manager/v1.hpp"

using namespace payload::bench;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_RAM;

// ---------------------------------------------------------------------------
// 1. Spill pipeline: ExecuteSpill through PayloadManager
// ---------------------------------------------------------------------------
static void BenchSpillPipeline(size_t payload_bytes, bool fsync) {
  // Pre-allocate iterations + warmup so the warmup calls don't overlap the
  // timed pool.
  const int warmup     = 3;
  const int iterations = IterationsFor(payload_bytes, 64ULL * 1024 * 1024, 1000);
  const int total      = iterations + warmup;

  BenchFixture fix;

  std::vector<payload::manager::v1::PayloadID> ids;
  ids.reserve(total);
  for (int i = 0; i < total; ++i)
    ids.push_back(fix.MakeRamPayload(payload_bytes).payload_id());

  int idx = 0;
  auto result = TimedRun(
      std::string("ExecuteSpill (pipeline) fsync=") + (fsync ? "y" : "n"),
      payload_bytes,
      iterations,
      [&] { fix.manager->ExecuteSpill(ids[idx++], TIER_DISK, fsync); },
      warmup);

  PrintResult(result);
}

// ---------------------------------------------------------------------------
// 2. Raw DiskArrowStore write — isolates filesystem throughput
// ---------------------------------------------------------------------------
static void BenchRawDiskWrite(size_t payload_bytes, bool fsync) {
  const std::filesystem::path disk_root = std::filesystem::temp_directory_path() / "payload_bench_raw";
  std::filesystem::create_directories(disk_root);

  payload::storage::DiskArrowStore store(disk_root);

  // Allocate a source buffer once and reuse it across iterations.
  auto maybe_buf = arrow::AllocateResizableBuffer(payload_bytes);
  if (!maybe_buf.ok()) throw std::runtime_error("buffer alloc failed");
  std::shared_ptr<arrow::Buffer> src(std::move(*maybe_buf));
  if (payload_bytes > 0) std::memset(src->mutable_data(), 0xAB, payload_bytes);

  const int iterations = IterationsFor(payload_bytes, 128ULL * 1024 * 1024, 500);

  // Use a simple fixed printable ID so DiskArrowStore filepath is valid.
  // We overwrite the same file each iteration — measures steady-state write perf.
  payload::manager::v1::PayloadID id;
  id.set_value("bench0000000000000000");

  auto result = TimedRun(
      std::string("DiskArrowStore::Write fsync=") + (fsync ? "y" : "n"),
      payload_bytes,
      iterations,
      [&] { store.Write(id, src, fsync); });

  PrintResult(result);

  std::filesystem::remove_all(disk_root);
}

// ---------------------------------------------------------------------------
// 3. Raw DiskArrowStore read — isolates filesystem read throughput
// ---------------------------------------------------------------------------
static void BenchRawDiskRead(size_t payload_bytes) {
  const std::filesystem::path disk_root = std::filesystem::temp_directory_path() / "payload_bench_rawread";
  std::filesystem::create_directories(disk_root);

  payload::storage::DiskArrowStore store(disk_root);

  auto maybe_buf = arrow::AllocateResizableBuffer(payload_bytes);
  if (!maybe_buf.ok()) throw std::runtime_error("buffer alloc failed");
  std::shared_ptr<arrow::Buffer> src(std::move(*maybe_buf));
  if (payload_bytes > 0) std::memset(src->mutable_data(), 0xCD, payload_bytes);

  payload::manager::v1::PayloadID id;
  id.set_value("bench0000000000000000");

  store.Write(id, src, false);

  const int iterations = IterationsFor(payload_bytes, 128ULL * 1024 * 1024, 500);

  auto result = TimedRun(
      "DiskArrowStore::Read",
      payload_bytes,
      iterations,
      [&] { auto buf = store.Read(id); (void)buf; });

  PrintResult(result);

  std::filesystem::remove_all(disk_root);
}

// ---------------------------------------------------------------------------
// 4. Promote pipeline: DISK -> RAM through PayloadManager
// ---------------------------------------------------------------------------
static void BenchPromotePipeline(size_t payload_bytes) {
  const int warmup     = 3;
  const int iterations = IterationsFor(payload_bytes, 64ULL * 1024 * 1024, 1000);
  const int total      = iterations + warmup;

  BenchFixture fix;

  // Pre-allocate in RAM, spill to "disk" (CopyingMemoryBackend), then promote.
  std::vector<payload::manager::v1::PayloadID> ids;
  ids.reserve(total);
  for (int i = 0; i < total; ++i) {
    auto id = fix.MakeRamPayload(payload_bytes).payload_id();
    fix.manager->ExecuteSpill(id, TIER_DISK, false);
    ids.push_back(id);
  }

  int idx = 0;
  auto result = TimedRun(
      "Promote DISK->RAM (pipeline)",
      payload_bytes,
      iterations,
      [&] { fix.manager->Promote(ids[idx++], TIER_RAM); },
      warmup);

  PrintResult(result);
}

int main() {
  PrintHeader();

  std::cout << "\n-- Spill pipeline (application overhead, in-memory copy backend)\n";
  for (size_t size : {4096UL, 65536UL, 1048576UL, 16777216UL})
    BenchSpillPipeline(size, false);

  std::cout << "\n-- Promote pipeline (application overhead, in-memory copy backend)\n";
  for (size_t size : {4096UL, 65536UL, 1048576UL})
    BenchPromotePipeline(size);

  std::cout << "\n-- Raw DiskArrowStore::Write fsync=n (filesystem throughput)\n";
  for (size_t size : {4096UL, 65536UL, 1048576UL, 16777216UL})
    BenchRawDiskWrite(size, false);

  std::cout << "\n-- Raw DiskArrowStore::Write fsync=y (durable write throughput)\n";
  for (size_t size : {4096UL, 65536UL, 1048576UL})
    BenchRawDiskWrite(size, true);

  std::cout << "\n-- Raw DiskArrowStore::Read (filesystem read throughput)\n";
  for (size_t size : {4096UL, 65536UL, 1048576UL, 16777216UL})
    BenchRawDiskRead(size);

  return 0;
}

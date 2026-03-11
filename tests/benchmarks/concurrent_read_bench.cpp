/*
  concurrent_read_bench.cpp

  Measures read throughput under concurrent load at varying thread counts.

  Two paths are benchmarked:

  1. ResolveSnapshot (snapshot cache hit)
     - Acquires shared lock on PayloadManager::snapshot_cache_mutex_
     - No storage backend involved
     - Models: multiple consumers reading descriptor metadata

  2. RamArrowStore::Read (direct storage read)
     - Acquires shared lock on RamArrowStore::mutex_
     - Returns the Arrow buffer pointer (zero-copy)
     - Models: concurrent GPU/CPU consumers reading the same payload buffer
*/

#include <atomic>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include <arrow/buffer.h>

#include "common/bench_fixture.hpp"
#include "internal/storage/ram/ram_arrow_store.hpp"
#include "payload/manager/v1.hpp"

using namespace payload::bench;
using payload::manager::v1::TIER_RAM;

// ---------------------------------------------------------------------------
// Bench: concurrent ResolveSnapshot (snapshot cache path)
// ---------------------------------------------------------------------------
static void BenchConcurrentSnapshotResolve(size_t payload_bytes, int n_threads) {
  const int    reads_per_thread = std::max(500, IterationsFor(payload_bytes, 64ULL * 1024 * 1024, 10000) / n_threads);
  BenchFixture fix{};

  // Single payload — all threads read the same descriptor.
  auto desc = fix.MakeRamPayload(payload_bytes);
  auto id   = desc.payload_id();

  // Warm the cache with one resolve.
  fix.manager->ResolveSnapshot(id);

  auto t0 = std::chrono::steady_clock::now();

  std::vector<std::thread> threads;
  threads.reserve(n_threads);
  for (int t = 0; t < n_threads; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < reads_per_thread; ++i)
        fix.manager->ResolveSnapshot(id);
    });
  }
  for (auto& th : threads) th.join();

  auto t1 = std::chrono::steady_clock::now();

  int    total_ops = n_threads * reads_per_thread;
  double total_ns  = static_cast<double>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

  BenchResult r;
  r.name          = "ResolveSnapshot threads=" + std::to_string(n_threads);
  r.payload_bytes = payload_bytes;
  r.iterations    = total_ops;
  r.total_ns      = total_ns;
  PrintResult(r);
}

// ---------------------------------------------------------------------------
// Bench: concurrent RamArrowStore::Read (direct storage backend)
// ---------------------------------------------------------------------------
static void BenchConcurrentRamRead(size_t payload_bytes, int n_threads) {
  const int reads_per_thread = std::max(500, IterationsFor(payload_bytes, 64ULL * 1024 * 1024, 10000) / n_threads);

  // Use the store directly — bypass PayloadManager overhead so we isolate
  // the shared_mutex in RamArrowStore.
  auto store = std::make_shared<payload::storage::RamArrowStore>();

  payload::manager::v1::PayloadID id;
  id.set_value("bench-payload");
  store->Allocate(id, payload_bytes);

  auto t0 = std::chrono::steady_clock::now();

  std::vector<std::thread> threads;
  threads.reserve(n_threads);
  for (int t = 0; t < n_threads; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < reads_per_thread; ++i) {
        auto buf = store->Read(id);
        (void)buf;
      }
    });
  }
  for (auto& th : threads) th.join();

  auto t1 = std::chrono::steady_clock::now();

  int    total_ops = n_threads * reads_per_thread;
  double total_ns  = static_cast<double>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

  BenchResult r;
  r.name          = "RamStore::Read threads=" + std::to_string(n_threads);
  r.payload_bytes = payload_bytes;
  r.iterations    = total_ops;
  r.total_ns      = total_ns;
  PrintResult(r);
}

int main() {
  PrintHeader();

  const size_t payload_bytes = 1048576; // 1 MB — large enough that lock overhead is visible

  std::cout << "-- ResolveSnapshot (snapshot_cache_mutex shared lock)\n";
  for (int threads : {1, 2, 4, 8, 16})
    BenchConcurrentSnapshotResolve(payload_bytes, threads);

  std::cout << "\n-- RamArrowStore::Read (ram store shared_mutex)\n";
  for (int threads : {1, 2, 4, 8, 16})
    BenchConcurrentRamRead(payload_bytes, threads);

  return 0;
}

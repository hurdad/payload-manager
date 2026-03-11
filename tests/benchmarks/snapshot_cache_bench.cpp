/*
  snapshot_cache_bench.cpp

  Measures ResolveSnapshot latency for cache hits vs cache misses.

  Cache hit:   snapshot_cache_ populated on Commit — served from an
               unordered_map under a shared_mutex. Should be very fast.

  Cache miss:  HydrateCaches() clears and rebuilds the cache from the
               repository. After clearing, the first ResolveSnapshot per
               payload goes to the MemoryRepository. Models the warm-up
               cost after a restart or a forced cache invalidation.

  Also measures how cache lookup time degrades as the number of live
  payloads grows (hash map overhead at scale).
*/

#include <iostream>
#include <vector>

#include "common/bench_fixture.hpp"
#include "payload/manager/v1.hpp"

using namespace payload::bench;
using payload::manager::v1::TIER_RAM;

// ---------------------------------------------------------------------------
// Bench: ResolveSnapshot on a pre-warmed cache
// ---------------------------------------------------------------------------
static void BenchSnapshotCacheHit(int n_payloads) {
  BenchFixture fix{};

  // Populate the manager with n_payloads; each Commit warms the cache entry.
  std::vector<payload::manager::v1::PayloadID> ids;
  ids.reserve(n_payloads);
  for (int i = 0; i < n_payloads; ++i) ids.push_back(fix.MakeRamPayload(4096).payload_id());

  // Resolve the last one repeatedly — ensures we're not just hitting index 0.
  auto& target_id = ids.back();

  const int iterations = 50000;
  auto      result =
      TimedRun("ResolveSnapshot cache hit n=" + std::to_string(n_payloads), 0, iterations, [&] { fix.manager->ResolveSnapshot(target_id); });

  // payload_bytes=0 since we're not moving data; suppress throughput column.
  result.payload_bytes = 0;
  std::cout << std::left << std::setw(42) << result.name << std::setw(10) << "-" << std::setw(8) << result.iterations << std::setw(14) << std::fixed
            << std::setprecision(3) << result.per_op_us() << "\n";
}

// ---------------------------------------------------------------------------
// Bench: ResolveSnapshot after HydrateCaches (cold cache, DB fallback)
// ---------------------------------------------------------------------------
static void BenchSnapshotCacheMiss(int n_payloads) {
  BenchFixture fix{};

  std::vector<payload::manager::v1::PayloadID> ids;
  ids.reserve(n_payloads);
  for (int i = 0; i < n_payloads; ++i) ids.push_back(fix.MakeRamPayload(4096).payload_id());

  // HydrateCaches rebuilds snapshot_cache_ from the repository, then
  // subsequent resolves serve from the fresh cache. Measure the rebuild cost.
  const int iterations = 200;
  auto      result     = TimedRun("HydrateCaches n=" + std::to_string(n_payloads), 0, iterations, [&] { fix.manager->HydrateCaches(); });

  result.payload_bytes = 0;
  std::cout << std::left << std::setw(42) << result.name << std::setw(10) << "-" << std::setw(8) << result.iterations << std::setw(14) << std::fixed
            << std::setprecision(3) << result.per_op_us() << "\n";
}

int main() {
  std::cout << std::left << std::setw(42) << "benchmark" << std::setw(10) << "size" << std::setw(8) << "iters" << std::setw(14) << "per-op (µs)"
            << "\n"
            << std::string(74, '-') << "\n";

  std::cout << "\n-- Cache hit: cost vs number of live payloads\n";
  for (int n : {10, 100, 1000, 10000}) BenchSnapshotCacheHit(n);

  std::cout << "\n-- HydrateCaches (full rebuild from repository)\n";
  for (int n : {10, 100, 1000, 5000}) BenchSnapshotCacheMiss(n);

  return 0;
}

/*
  allocate_commit_bench.cpp

  Measures the Allocate -> Commit pipeline throughput at varying payload sizes.

  What this exercises:
    - RamArrowStore::Allocate (arrow buffer alloc + map insert under exclusive lock)
    - MemoryRepository InsertPayload tx
    - PayloadManager::Commit (state transition + snapshot cache write)
    - PayloadMutex map growth under repeated allocation
*/

#include <iostream>

#include "common/bench_fixture.hpp"
#include "payload/manager/v1.hpp"

using namespace payload::bench;
using payload::manager::v1::TIER_RAM;

// ---------------------------------------------------------------------------
// Bench: Allocate + Commit back-to-back (typical producer ingest path)
// ---------------------------------------------------------------------------
static void BenchAllocateCommit(size_t payload_bytes) {
  const int    iterations = IterationsFor(payload_bytes, 64ULL * 1024 * 1024, 5000);
  BenchFixture fix{};

  auto result = TimedRun(
      "allocate+commit RAM",
      payload_bytes,
      iterations,
      [&] {
        auto desc = fix.manager->Allocate(payload_bytes, TIER_RAM);
        fix.manager->Commit(desc.payload_id());
      });

  PrintResult(result);
}

// ---------------------------------------------------------------------------
// Bench: Allocate only — isolates buffer allocation cost from DB overhead
// ---------------------------------------------------------------------------
static void BenchAllocateOnly(size_t payload_bytes) {
  const int    iterations = IterationsFor(payload_bytes, 64ULL * 1024 * 1024, 5000);
  BenchFixture fix{};

  auto result = TimedRun(
      "allocate only RAM (no commit)",
      payload_bytes,
      iterations,
      [&] { fix.manager->Allocate(payload_bytes, TIER_RAM); });

  PrintResult(result);
}

// ---------------------------------------------------------------------------
// Bench: Allocate + Commit + Delete cycle
//
// Models ephemeral payloads — allocate, use, discard.
// Exercises PayloadMutex map pruning on delete.
// ---------------------------------------------------------------------------
static void BenchAllocateCommitDelete(size_t payload_bytes) {
  const int    iterations = IterationsFor(payload_bytes, 64ULL * 1024 * 1024, 5000);
  BenchFixture fix{};

  auto result = TimedRun(
      "allocate+commit+delete RAM",
      payload_bytes,
      iterations,
      [&] {
        auto desc = fix.manager->Allocate(payload_bytes, TIER_RAM);
        auto committed = fix.manager->Commit(desc.payload_id());
        fix.manager->Delete(committed.payload_id(), /*force=*/true);
      });

  PrintResult(result);
}

int main() {
  PrintHeader();

  for (size_t size : {256UL, 4096UL, 65536UL, 1048576UL, 16777216UL})
    BenchAllocateCommit(size);

  std::cout << "\n";

  for (size_t size : {256UL, 4096UL, 65536UL, 1048576UL})
    BenchAllocateOnly(size);

  std::cout << "\n";

  for (size_t size : {256UL, 4096UL, 65536UL})
    BenchAllocateCommitDelete(size);

  return 0;
}

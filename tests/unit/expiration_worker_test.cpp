/*
  Unit tests for ExpirationWorker:

  - Worker starts and stops cleanly without crashing
  - Worker triggers ExpireStale: TTL-expired payload is removed
*/

#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>
#include <unordered_map>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/expiration/expiration_worker.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/storage/storage_backend.hpp"
#include "payload/manager/v1.hpp"

namespace {

using payload::core::PayloadManager;
using payload::expiration::ExpirationWorker;
using payload::lease::LeaseManager;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_RAM;

class SimpleStorageBackend final : public payload::storage::StorageBackend {
 public:
  explicit SimpleStorageBackend(payload::manager::v1::Tier tier) : tier_(tier) {
  }

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size_bytes) override {
    auto result = arrow::AllocateBuffer(size_bytes);
    if (!result.ok()) throw std::runtime_error(result.status().ToString());
    std::shared_ptr<arrow::Buffer> buf(std::move(*result));
    if (size_bytes > 0) std::memset(buf->mutable_data(), 0, static_cast<size_t>(size_bytes));
    buffers_[id.value()] = buf;
    return buf;
  }

  std::shared_ptr<arrow::Buffer> Read(const payload::manager::v1::PayloadID& id) override {
    return buffers_.at(id.value());
  }

  void Write(const payload::manager::v1::PayloadID& id, const std::shared_ptr<arrow::Buffer>& buf, bool) override {
    buffers_[id.value()] = buf;
  }

  void Remove(const payload::manager::v1::PayloadID& id) override {
    buffers_.erase(id.value());
  }

  bool Has(const payload::manager::v1::PayloadID& id) const {
    return buffers_.count(id.value()) > 0;
  }

  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }

 private:
  payload::manager::v1::Tier                                      tier_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> buffers_;
};

struct Fixture {
  std::shared_ptr<LeaseManager>                          lease_mgr = std::make_shared<LeaseManager>();
  std::shared_ptr<SimpleStorageBackend>                  ram       = std::make_shared<SimpleStorageBackend>(TIER_RAM);
  std::shared_ptr<payload::db::memory::MemoryRepository> repo      = std::make_shared<payload::db::memory::MemoryRepository>();
  std::shared_ptr<PayloadManager>                        manager{[&] {
    payload::storage::StorageFactory::TierMap storage;
    storage[TIER_RAM] = ram;
    return std::make_shared<PayloadManager>(storage, lease_mgr, repo);
  }()};
};

// Worker starts and stops without crashing (no payloads).
void TestWorkerStartStop() {
  Fixture f;

  ExpirationWorker worker(f.manager, std::chrono::seconds{60});
  worker.Start();
  worker.Stop();
  // If we reach here without hanging or crashing, the test passes.
}

// Worker fires ExpireStale within its interval: TTL-expired payload is removed.
void TestWorkerExpiresPayload() {
  Fixture f;

  // Commit a payload with 1 ms TTL — it expires almost instantly.
  const auto desc = f.manager->Commit(f.manager->Allocate(64, TIER_RAM, /*ttl_ms=*/1).payload_id());
  assert(f.ram->Has(desc.payload_id()));

  // Sleep past the TTL.
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Run worker with a 50 ms interval — it should fire quickly.
  ExpirationWorker worker(f.manager, std::chrono::milliseconds{50});
  worker.Start();

  // Wait long enough for at least one sweep.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  worker.Stop();

  assert(!f.ram->Has(desc.payload_id()) && "expired payload must be removed after worker sweep");
}

} // namespace

int main() {
  TestWorkerStartStop();
  TestWorkerExpiresPayload();

  std::cout << "expiration_worker_test: pass\n";
  return 0;
}

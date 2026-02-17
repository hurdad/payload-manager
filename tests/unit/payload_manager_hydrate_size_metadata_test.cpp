#include <cassert>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/db/model/payload_record.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/storage/storage_backend.hpp"

namespace {

using payload::core::PayloadManager;
using payload::db::memory::MemoryRepository;
using payload::db::model::PayloadRecord;
using payload::manager::v1::PAYLOAD_STATE_ACTIVE;
using payload::manager::v1::PayloadID;
using payload::manager::v1::TIER_DISK;
using payload::storage::StorageBackend;

class SizeOnlyStorageBackend final : public StorageBackend {
 public:
  std::shared_ptr<arrow::Buffer> Allocate(const PayloadID&, uint64_t) override {
    throw std::runtime_error("allocate unsupported");
  }

  std::shared_ptr<arrow::Buffer> Read(const PayloadID&) override {
    throw std::runtime_error("read should not be called for hydrate metadata");
  }

  uint64_t Size(const PayloadID& id) override {
    return sizes_.at(id.value());
  }

  void Write(const PayloadID&, const std::shared_ptr<arrow::Buffer>&, bool) override {
    throw std::runtime_error("write unsupported");
  }

  void Remove(const PayloadID&) override {
  }

  payload::manager::v1::Tier TierType() const override {
    return TIER_DISK;
  }

  void SetSize(const std::string& id, uint64_t size) {
    sizes_[id] = size;
  }

 private:
  std::unordered_map<std::string, uint64_t> sizes_;
};

void TestHydrateCachesUsesSizeMetadataWithoutRead() {
  auto repository = std::make_shared<MemoryRepository>();
  auto backend    = std::make_shared<SizeOnlyStorageBackend>();

  PayloadRecord seed;
  seed.id         = "payload-disk-1";
  seed.tier       = TIER_DISK;
  seed.state      = PAYLOAD_STATE_ACTIVE;
  seed.size_bytes = 0;
  seed.version    = 1;

  {
    auto tx = repository->Begin();
    auto ok = repository->InsertPayload(*tx, seed);
    assert(ok);
    tx->Commit();
  }

  backend->SetSize(seed.id, 4096);

  payload::storage::StorageFactory::TierMap storage;
  storage[TIER_DISK] = backend;

  PayloadManager manager(std::move(storage), std::make_shared<payload::lease::LeaseManager>(), /*metadata=*/nullptr,
                         /*lineage=*/nullptr, repository);

  manager.HydrateCaches();

  PayloadID id;
  id.set_value(seed.id);

  const auto descriptor = manager.ResolveSnapshot(id);
  assert(descriptor.has_disk());
  assert(descriptor.disk().length_bytes() == 4096);
}

} // namespace

int main() {
  TestHydrateCachesUsesSizeMetadataWithoutRead();

  std::cout << "payload_manager_unit_hydrate_size_metadata: pass\n";
  return 0;
}

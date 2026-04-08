/*
  Unit tests for the object-tier client-upload path:
    PayloadManager::Allocate(TIER_OBJECT) — no storage allocation
    PayloadManager::GetObjectUploadPath    — returns URI or empty
    PayloadManager::Import                 — ALLOCATED → DURABLE, writes sidecar

  All tests use an in-memory repository and an ObjectArrowStore backed by the
  Arrow LocalFileSystem so sidecar writes can be verified without S3.

  Covered:
    1. Allocate(TIER_OBJECT) succeeds and does NOT call storage Allocate()
    2. Allocate(TIER_OBJECT) returns a descriptor with no location set
    3. Allocate(TIER_OBJECT) stores a PAYLOAD_STATE_ALLOCATED record in the DB
    4. GetObjectUploadPath returns a non-empty path when object store is configured
    5. GetObjectUploadPath returns empty string when no object store is configured
    6. Import transitions ALLOCATED → DURABLE
    7. Import writes a .meta.json sidecar to the local object store
    8. Import with size_bytes > 0 updates the stored size
    9. Import on a non-ALLOCATED payload throws InvalidState
   10. Import on a TIER_RAM payload throws InvalidState
   11. Import on an unknown payload throws NotFound
   12. CatalogService::Allocate populates object_upload_path for TIER_OBJECT
   13. CatalogService::Import delegates to PayloadManager::Import
*/

#include <arrow/buffer.h>
#include <arrow/filesystem/localfs.h>
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/service/catalog_service.hpp"
#include "internal/service/service_context.hpp"
#include "internal/storage/object/object_arrow_store.hpp"
#include "internal/storage/ram/ram_arrow_store.hpp"
#include "internal/storage/storage_backend.hpp"
#include "internal/util/errors.hpp"
#include "payload/manager/catalog/v1/archive_metadata.pb.h"
#include "payload/manager/v1.hpp"

using namespace payload::manager::v1;
using namespace payload::manager::catalog::v1;
using payload::core::PayloadManager;
using payload::lease::LeaseManager;

namespace {

// ---------------------------------------------------------------------------
// RAII temp directory
// ---------------------------------------------------------------------------

struct TempDir {
  std::filesystem::path path;

  TempDir() {
    path = std::filesystem::temp_directory_path() /
           ("import_test_" + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())));
    std::filesystem::create_directories(path);
  }

  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
  }
};

// ---------------------------------------------------------------------------
// Stub storage backend that records Allocate calls
// ---------------------------------------------------------------------------

class TrackingStorageBackend final : public payload::storage::StorageBackend {
 public:
  explicit TrackingStorageBackend(payload::manager::v1::Tier tier) : tier_(tier) {
  }

  std::shared_ptr<arrow::Buffer> Allocate(const PayloadID& id, uint64_t size_bytes) override {
    allocate_calls_++;
    auto buf = *arrow::AllocateBuffer(size_bytes);
    buffers_[id.value()] = std::shared_ptr<arrow::Buffer>(std::move(buf));
    return buffers_[id.value()];
  }

  std::shared_ptr<arrow::Buffer> Read(const PayloadID& id) override {
    return buffers_.at(id.value());
  }

  void Write(const PayloadID& id, const std::shared_ptr<arrow::Buffer>& buf, bool) override {
    buffers_[id.value()] = buf;
  }

  void Remove(const PayloadID& id) override {
    buffers_.erase(id.value());
  }

  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }

  int allocate_calls() const {
    return allocate_calls_;
  }

 private:
  payload::manager::v1::Tier                                      tier_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> buffers_;
  int                                                              allocate_calls_{0};
};

// ---------------------------------------------------------------------------
// Test fixture — RAM + local-filesystem ObjectArrowStore
// ---------------------------------------------------------------------------

struct ImportFixture {
  TempDir                                                tmp;
  std::shared_ptr<LeaseManager>                          lease_mgr = std::make_shared<LeaseManager>();
  std::shared_ptr<TrackingStorageBackend>                ram       = std::make_shared<TrackingStorageBackend>(TIER_RAM);
  std::shared_ptr<payload::storage::ObjectArrowStore>    obj_store{[&] {
    auto local_fs = std::make_shared<arrow::fs::LocalFileSystem>();
    return std::make_shared<payload::storage::ObjectArrowStore>(local_fs, tmp.path.string(), false);
  }()};
  std::shared_ptr<payload::db::memory::MemoryRepository> repo = std::make_shared<payload::db::memory::MemoryRepository>();
  std::shared_ptr<PayloadManager>                        manager{[&] {
    payload::storage::StorageFactory::TierMap storage;
    storage[TIER_RAM]    = ram;
    storage[TIER_OBJECT] = obj_store;
    return std::make_shared<PayloadManager>(storage, lease_mgr, repo);
  }()};
  payload::service::ServiceContext ctx{[&] {
    payload::service::ServiceContext c;
    c.manager    = manager;
    c.repository = repo;
    return c;
  }()};
  payload::service::CatalogService catalog{ctx};
};

// Same but without an object store configured.
struct ImportFixtureNoObjectStore {
  std::shared_ptr<LeaseManager>                          lease_mgr = std::make_shared<LeaseManager>();
  std::shared_ptr<TrackingStorageBackend>                ram       = std::make_shared<TrackingStorageBackend>(TIER_RAM);
  std::shared_ptr<payload::db::memory::MemoryRepository> repo = std::make_shared<payload::db::memory::MemoryRepository>();
  std::shared_ptr<PayloadManager>                        manager{[&] {
    payload::storage::StorageFactory::TierMap storage;
    storage[TIER_RAM] = ram;
    return std::make_shared<PayloadManager>(storage, lease_mgr, repo);
  }()};
};

// ---------------------------------------------------------------------------
// Helper: write a real bytes file so ObjectArrowStore::Size() succeeds.
// ---------------------------------------------------------------------------

void WriteObjectBytes(const TempDir& tmp, const PayloadID& id, uint64_t size_bytes) {
  // Derive uuid hex from 16-byte binary id.
  static constexpr char kHex[] = "0123456789abcdef";
  std::string           hex;
  hex.reserve(32);
  for (unsigned char c : id.value()) {
    hex.push_back(kHex[c >> 4]);
    hex.push_back(kHex[c & 0x0fu]);
  }
  const auto path = tmp.path / (hex + ".bin");
  std::ofstream out(path, std::ios::binary);
  std::string   data(size_bytes, '\xBE');
  out.write(data.data(), static_cast<std::streamsize>(data.size()));
}

// ---------------------------------------------------------------------------
// 1. Allocate(TIER_OBJECT) does not call storage Allocate()
// ---------------------------------------------------------------------------

TEST(PayloadManagerImport, AllocateObjectTierSkipsStorageAllocate) {
  ImportFixture f;

  const auto desc = f.manager->Allocate(256, TIER_OBJECT);

  EXPECT_EQ(f.ram->allocate_calls(), 0);
  EXPECT_EQ(desc.tier(), TIER_OBJECT);
}

// ---------------------------------------------------------------------------
// 2. Allocate(TIER_OBJECT) returns descriptor with no location set
// ---------------------------------------------------------------------------

TEST(PayloadManagerImport, AllocateObjectTierDescriptorHasNoLocation) {
  ImportFixture f;

  const auto desc = f.manager->Allocate(128, TIER_OBJECT);

  EXPECT_FALSE(desc.has_ram());
  EXPECT_FALSE(desc.has_disk());
  EXPECT_FALSE(desc.has_gpu());
}

// ---------------------------------------------------------------------------
// 3. Allocate(TIER_OBJECT) stores PAYLOAD_STATE_ALLOCATED in the DB
// ---------------------------------------------------------------------------

TEST(PayloadManagerImport, AllocateObjectTierPersistsAllocatedRecord) {
  ImportFixture f;

  const auto desc = f.manager->Allocate(64, TIER_OBJECT);
  const auto uuid = payload::util::FromProto(desc.payload_id());

  auto tx     = f.repo->Begin();
  const auto record = f.repo->GetPayload(*tx, uuid);
  tx->Commit();

  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(record->state, PAYLOAD_STATE_ALLOCATED);
  EXPECT_EQ(record->tier, TIER_OBJECT);
  EXPECT_EQ(record->size_bytes, 64u);
}

// ---------------------------------------------------------------------------
// 4. GetObjectUploadPath returns a non-empty URI when object store is configured
// ---------------------------------------------------------------------------

TEST(PayloadManagerImport, GetObjectUploadPathNonEmptyWhenConfigured) {
  ImportFixture f;

  const auto desc = f.manager->Allocate(64, TIER_OBJECT);
  const auto uri  = f.manager->GetObjectUploadPath(desc.payload_id());

  EXPECT_FALSE(uri.empty());
  // Local filesystem path should contain the uuid hex.
  const auto uuid = payload::util::ToString(payload::util::FromProto(desc.payload_id()));
  EXPECT_NE(uri.find(uuid), std::string::npos);
}

// ---------------------------------------------------------------------------
// 5. GetObjectUploadPath returns empty when no object store is configured
// ---------------------------------------------------------------------------

TEST(PayloadManagerImport, GetObjectUploadPathEmptyWithoutObjectStore) {
  ImportFixtureNoObjectStore f;

  const auto desc = f.manager->Allocate(64, TIER_RAM);
  const auto uri  = f.manager->GetObjectUploadPath(desc.payload_id());

  EXPECT_TRUE(uri.empty());
}

// ---------------------------------------------------------------------------
// 6. Import transitions ALLOCATED → DURABLE
// ---------------------------------------------------------------------------

TEST(PayloadManagerImport, ImportTransitionsToDurable) {
  ImportFixture f;

  const auto desc = f.manager->Allocate(64, TIER_OBJECT);
  const auto id   = desc.payload_id();

  WriteObjectBytes(f.tmp, id, 64);
  f.manager->Import(id, 64);

  const auto uuid  = payload::util::FromProto(id);
  auto       tx    = f.repo->Begin();
  const auto record = f.repo->GetPayload(*tx, uuid);
  tx->Commit();

  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(record->state, PAYLOAD_STATE_DURABLE);
  EXPECT_EQ(record->tier, TIER_OBJECT);
}

// ---------------------------------------------------------------------------
// 7. Import writes a .meta.json sidecar
// ---------------------------------------------------------------------------

TEST(PayloadManagerImport, ImportWritesSidecarJson) {
  ImportFixture f;

  const auto desc = f.manager->Allocate(32, TIER_OBJECT);
  const auto id   = desc.payload_id();

  WriteObjectBytes(f.tmp, id, 32);
  f.manager->Import(id, 32);

  const auto uuid    = payload::util::ToString(payload::util::FromProto(id));
  const auto sidecar = f.tmp.path / (uuid + ".meta.json");
  ASSERT_TRUE(std::filesystem::exists(sidecar)) << "sidecar should exist at " << sidecar;

  std::ifstream      in(sidecar);
  const std::string  json_str((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  EXPECT_FALSE(json_str.empty());

  PayloadArchiveMetadata meta;
  const auto             status = google::protobuf::util::JsonStringToMessage(json_str, &meta);
  EXPECT_TRUE(status.ok()) << "sidecar must be valid proto JSON: " << status.ToString();
  EXPECT_EQ(meta.metadata_version(), 1u);
  EXPECT_EQ(meta.payload_descriptor().tier(), TIER_OBJECT);
}

// ---------------------------------------------------------------------------
// 8. Import with new size_bytes updates the stored record
// ---------------------------------------------------------------------------

TEST(PayloadManagerImport, ImportUpdatesStoredSizeWhenDifferent) {
  ImportFixture f;

  // Allocate for 128 bytes; actually only 64 bytes were written.
  const auto desc = f.manager->Allocate(128, TIER_OBJECT);
  const auto id   = desc.payload_id();

  WriteObjectBytes(f.tmp, id, 64);
  f.manager->Import(id, 64);

  const auto uuid   = payload::util::FromProto(id);
  auto       tx     = f.repo->Begin();
  const auto record = f.repo->GetPayload(*tx, uuid);
  tx->Commit();

  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(record->size_bytes, 64u);
}

// ---------------------------------------------------------------------------
// 9. Import on a non-ALLOCATED payload throws InvalidState
// ---------------------------------------------------------------------------

TEST(PayloadManagerImport, ImportOnCommittedPayloadThrows) {
  ImportFixture f;

  const auto desc = f.manager->Allocate(32, TIER_OBJECT);
  const auto id   = desc.payload_id();
  WriteObjectBytes(f.tmp, id, 32);
  f.manager->Import(id, 32);

  // Second Import call — record is now DURABLE, not ALLOCATED.
  EXPECT_THROW(f.manager->Import(id, 32), payload::util::InvalidState);
}

// ---------------------------------------------------------------------------
// 10. Import on a TIER_RAM payload throws InvalidState
// ---------------------------------------------------------------------------

TEST(PayloadManagerImport, ImportOnRamPayloadThrows) {
  ImportFixture f;

  const auto desc = f.manager->Allocate(32, TIER_RAM);
  const auto id   = desc.payload_id();

  EXPECT_THROW(f.manager->Import(id, 32), payload::util::InvalidState);
}

// ---------------------------------------------------------------------------
// 11. Import on an unknown payload throws NotFound
// ---------------------------------------------------------------------------

TEST(PayloadManagerImport, ImportUnknownPayloadThrows) {
  ImportFixture f;

  PayloadID unknown;
  unknown.set_value(std::string(16, '\xFF'));

  EXPECT_THROW(f.manager->Import(unknown, 64), payload::util::NotFound);
}

// ---------------------------------------------------------------------------
// 12. CatalogService::Allocate populates object_upload_path for TIER_OBJECT
// ---------------------------------------------------------------------------

TEST(PayloadManagerImport, CatalogAllocatePopulatesObjectUploadPath) {
  ImportFixture f;

  AllocatePayloadRequest req;
  req.set_size_bytes(256);
  req.set_preferred_tier(TIER_OBJECT);

  const auto resp = f.catalog.Allocate(req);

  EXPECT_FALSE(resp.object_upload_path().empty())
      << "object_upload_path should be set for TIER_OBJECT allocations";
  // Descriptor should have no location set.
  EXPECT_FALSE(resp.payload_descriptor().has_ram());
  EXPECT_FALSE(resp.payload_descriptor().has_disk());
  EXPECT_FALSE(resp.payload_descriptor().has_gpu());
}

// ---------------------------------------------------------------------------
// 13. CatalogService::Allocate does not set object_upload_path for TIER_RAM
// ---------------------------------------------------------------------------

TEST(PayloadManagerImport, CatalogAllocateDoesNotSetUploadPathForRam) {
  ImportFixture f;

  AllocatePayloadRequest req;
  req.set_size_bytes(64);
  req.set_preferred_tier(TIER_RAM);

  const auto resp = f.catalog.Allocate(req);

  EXPECT_TRUE(resp.object_upload_path().empty());
  EXPECT_TRUE(resp.payload_descriptor().has_ram());
}

// ---------------------------------------------------------------------------
// 14. CatalogService::Import calls PayloadManager::Import end-to-end
// ---------------------------------------------------------------------------

TEST(PayloadManagerImport, CatalogImportEndToEnd) {
  ImportFixture f;

  AllocatePayloadRequest alloc_req;
  alloc_req.set_size_bytes(64);
  alloc_req.set_preferred_tier(TIER_OBJECT);
  const auto alloc_resp = f.catalog.Allocate(alloc_req);
  const auto id         = alloc_resp.payload_descriptor().payload_id();

  WriteObjectBytes(f.tmp, id, 64);

  ImportPayloadRequest import_req;
  *import_req.mutable_id() = id;
  import_req.set_size_bytes(64);
  EXPECT_NO_THROW(f.catalog.Import(import_req));

  // Payload should now be readable via ResolveSnapshot.
  const auto uuid   = payload::util::FromProto(id);
  auto       tx     = f.repo->Begin();
  const auto record = f.repo->GetPayload(*tx, uuid);
  tx->Commit();
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(record->state, PAYLOAD_STATE_DURABLE);
}

} // namespace

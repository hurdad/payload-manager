/*
  Unit tests for .meta.json sidecar writes on disk and object storage tiers.

  Covered:
    1. DiskArrowStore::WriteSidecar writes <uuid>.meta.json with valid proto JSON
    2. DiskArrowStore::Remove deletes both .bin and .meta.json
    3. ObjectArrowStore::WriteSidecar via Arrow LocalFileSystem
    4. ObjectArrowStore::Remove deletes both .bin and .meta.json
    5. PayloadManager::Commit on a disk-allocated payload triggers sidecar
    6. PayloadManager::ExecuteSpill (RAM → disk) triggers sidecar on destination
    7. Sidecar contains lineage edges registered before commit
*/

#include <arrow/buffer.h>
#include <arrow/filesystem/localfs.h>
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/service/catalog_service.hpp"
#include "internal/service/service_context.hpp"
#include "internal/storage/disk/disk_arrow_store.hpp"
#include "internal/storage/object/object_arrow_store.hpp"
#include "internal/storage/ram/ram_arrow_store.hpp"
#include "internal/storage/storage_factory.hpp"
#include "internal/util/uuid.hpp"
#include "payload/manager/catalog/v1/archive_metadata.pb.h"
#include "payload/manager/v1.hpp"

using namespace payload::manager::v1;
using namespace payload::manager::catalog::v1;
using payload::core::PayloadManager;
using payload::lease::LeaseManager;

namespace {

// RAII temp directory — removed on destruction.
struct TempDir {
  std::filesystem::path path;

  TempDir() {
    path = std::filesystem::temp_directory_path() / ("sidecar_test_" + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())));
    std::filesystem::create_directories(path);
  }

  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
  }
};

PayloadArchiveMetadata ParseSidecar(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  EXPECT_TRUE(in) << "sidecar file must exist";
  std::ostringstream ss;
  ss << in.rdbuf();
  const std::string json = ss.str();
  EXPECT_FALSE(json.empty()) << "sidecar must not be empty";

  PayloadArchiveMetadata meta;
  auto                   status = google::protobuf::util::JsonStringToMessage(json, &meta);
  EXPECT_TRUE(status.ok()) << "sidecar must parse as valid PayloadArchiveMetadata JSON";
  return meta;
}

PayloadID MakePayloadId(const std::string& hex_uuid) {
  PayloadID id;
  id.set_value(payload::util::ToString(payload::util::FromString(hex_uuid)));
  return id;
}

// Minimal RAM-backed storage backend for use in PayloadManager fixture.
class RamBackend final : public payload::storage::StorageBackend {
 public:
  explicit RamBackend(Tier tier) : tier_(tier) {
  }

  std::shared_ptr<arrow::Buffer> Allocate(const PayloadID& id, uint64_t size) override {
    auto r = arrow::AllocateBuffer(size);
    EXPECT_TRUE(r.ok());
    auto buf = std::shared_ptr<arrow::Buffer>(std::move(*r));
    if (size > 0) std::memset(buf->mutable_data(), 0, size);
    store_[id.value()] = buf;
    return buf;
  }

  std::shared_ptr<arrow::Buffer> Read(const PayloadID& id) override {
    return store_.at(id.value());
  }

  void Write(const PayloadID& id, const std::shared_ptr<arrow::Buffer>& buf, bool) override {
    store_[id.value()] = buf;
  }

  void Remove(const PayloadID& id) override {
    store_.erase(id.value());
  }

  Tier TierType() const override {
    return tier_;
  }

 private:
  Tier                                                            tier_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> store_;
};

struct DiskFixture {
  TempDir                                                tmp;
  std::shared_ptr<LeaseManager>                          lease_mgr = std::make_shared<LeaseManager>();
  std::shared_ptr<payload::storage::RamArrowStore>       ram_store = std::make_shared<payload::storage::RamArrowStore>("sidecar_test");
  std::shared_ptr<payload::storage::DiskArrowStore>      disk_store{std::make_shared<payload::storage::DiskArrowStore>(tmp.path)};
  std::shared_ptr<payload::db::memory::MemoryRepository> repo = std::make_shared<payload::db::memory::MemoryRepository>();
  std::shared_ptr<PayloadManager>                        manager{[&] {
    payload::storage::StorageFactory::TierMap storage;
    storage[TIER_RAM]  = ram_store;
    storage[TIER_DISK] = disk_store;
    return std::make_shared<PayloadManager>(std::move(storage), lease_mgr, repo);
  }()};
  payload::service::ServiceContext                       ctx{[&] {
    payload::service::ServiceContext c;
    c.manager    = manager;
    c.repository = repo;
    return c;
  }()};
  payload::service::CatalogService                       catalog{ctx};

  std::filesystem::path SidecarPath(const PayloadDescriptor& desc) const {
    const auto&         id_bytes = desc.payload_id().value();
    payload::util::UUID uuid{};
    std::memcpy(uuid.data(), id_bytes.data(), 16);
    return tmp.path / (payload::util::ToString(uuid) + ".meta.json");
  }
};

} // namespace

TEST(StorageSidecar, DiskWriteSidecar) {
  TempDir tmp;

  payload::storage::DiskArrowStore store(tmp.path);

  const auto uuid     = payload::util::GenerateUUID();
  const auto uuid_str = payload::util::ToString(uuid);
  PayloadID  id       = payload::util::ToProto(uuid);

  PayloadArchiveMetadata meta;
  *meta.mutable_uuid() = id;
  meta.set_metadata_version(1);
  meta.mutable_archived_at()->set_seconds(1700000000);
  meta.mutable_compression()->set_type(COMPRESSION_NONE);

  store.WriteSidecar(id, meta);

  const auto sidecar = tmp.path / (uuid_str + ".meta.json");
  EXPECT_TRUE(std::filesystem::exists(sidecar)) << "sidecar file must exist after WriteSidecar";

  auto parsed = ParseSidecar(sidecar);
  EXPECT_EQ(parsed.metadata_version(), 1) << "metadata_version must round-trip";
  EXPECT_EQ(parsed.archived_at().seconds(), 1700000000) << "archived_at must round-trip";
  EXPECT_EQ(parsed.compression().type(), COMPRESSION_NONE) << "compression must round-trip";
  EXPECT_FALSE(parsed.uuid().value().empty()) << "uuid must be set";
}

TEST(StorageSidecar, DiskRemoveDeletesSidecar) {
  TempDir tmp;

  payload::storage::DiskArrowStore store(tmp.path);

  const auto uuid = payload::util::GenerateUUID();
  PayloadID  id   = payload::util::ToProto(uuid);

  // Allocate creates .bin; WriteSidecar creates .meta.json.
  store.Allocate(id, 64);

  PayloadArchiveMetadata meta;
  *meta.mutable_uuid() = id;
  meta.set_metadata_version(1);
  store.WriteSidecar(id, meta);

  const auto uuid_str = payload::util::ToString(uuid);
  EXPECT_TRUE(std::filesystem::exists(tmp.path / (uuid_str + ".bin"))) << ".bin must exist before Remove";
  EXPECT_TRUE(std::filesystem::exists(tmp.path / (uuid_str + ".meta.json"))) << ".meta.json must exist before Remove";

  store.Remove(id);

  EXPECT_FALSE(std::filesystem::exists(tmp.path / (uuid_str + ".bin"))) << ".bin must be gone after Remove";
  EXPECT_FALSE(std::filesystem::exists(tmp.path / (uuid_str + ".meta.json"))) << ".meta.json must be gone after Remove";
}

TEST(StorageSidecar, ObjectWriteSidecar) {
  TempDir tmp;

  auto fs    = std::make_shared<arrow::fs::LocalFileSystem>();
  auto store = payload::storage::ObjectArrowStore(fs, tmp.path.string());

  const auto uuid     = payload::util::GenerateUUID();
  const auto uuid_str = payload::util::ToString(uuid);
  PayloadID  id       = payload::util::ToProto(uuid);

  // ObjectArrowStore uses id.value() directly as the key prefix.
  // For this test, set a simple ASCII value so the path is file-system safe.
  PayloadID ascii_id;
  ascii_id.set_value(uuid_str);

  PayloadArchiveMetadata meta;
  *meta.mutable_uuid() = ascii_id;
  meta.set_metadata_version(2);
  meta.mutable_archived_at()->set_seconds(1800000000);

  store.WriteSidecar(ascii_id, meta);

  const auto sidecar = tmp.path / (uuid_str + ".meta.json");
  EXPECT_TRUE(std::filesystem::exists(sidecar)) << "object sidecar must exist";

  auto parsed = ParseSidecar(sidecar);
  EXPECT_EQ(parsed.metadata_version(), 2) << "metadata_version must round-trip";
  EXPECT_EQ(parsed.archived_at().seconds(), 1800000000) << "archived_at must round-trip";
}

TEST(StorageSidecar, ObjectRemoveDeletesSidecar) {
  TempDir tmp;

  auto fs    = std::make_shared<arrow::fs::LocalFileSystem>();
  auto store = payload::storage::ObjectArrowStore(fs, tmp.path.string());

  const auto uuid     = payload::util::GenerateUUID();
  const auto uuid_str = payload::util::ToString(uuid);

  // Use an ASCII id (see ObjectWriteSidecar note).
  PayloadID ascii_id;
  ascii_id.set_value(uuid_str);

  // Write dummy data and sidecar.
  auto data_buf = arrow::AllocateBuffer(4);
  ASSERT_TRUE(data_buf.ok());
  store.Write(ascii_id, std::shared_ptr<arrow::Buffer>(std::move(*data_buf)), false);

  PayloadArchiveMetadata meta;
  *meta.mutable_uuid() = ascii_id;
  store.WriteSidecar(ascii_id, meta);

  EXPECT_TRUE(std::filesystem::exists(tmp.path / (uuid_str + ".bin"))) << ".bin must exist before Remove";
  EXPECT_TRUE(std::filesystem::exists(tmp.path / (uuid_str + ".meta.json"))) << ".meta.json must exist before Remove";

  store.Remove(ascii_id);

  EXPECT_FALSE(std::filesystem::exists(tmp.path / (uuid_str + ".bin"))) << ".bin must be gone";
  EXPECT_FALSE(std::filesystem::exists(tmp.path / (uuid_str + ".meta.json"))) << ".meta.json must be gone";
}

TEST(StorageSidecar, CommitDiskWritesSidecar) {
  DiskFixture f;

  AllocatePayloadRequest alloc_req;
  alloc_req.set_size_bytes(64);
  alloc_req.set_preferred_tier(TIER_DISK);
  const auto alloc_resp = f.catalog.Allocate(alloc_req);
  const auto desc       = alloc_resp.payload_descriptor();

  EXPECT_EQ(desc.tier(), TIER_DISK) << "must be allocated on disk";
  EXPECT_FALSE(std::filesystem::exists(f.SidecarPath(desc))) << "sidecar must not exist before commit";

  CommitPayloadRequest commit_req;
  *commit_req.mutable_id() = desc.payload_id();
  f.catalog.Commit(commit_req);

  EXPECT_TRUE(std::filesystem::exists(f.SidecarPath(desc))) << "sidecar must exist after commit";

  auto meta = ParseSidecar(f.SidecarPath(desc));
  EXPECT_EQ(meta.metadata_version(), 1) << "metadata_version must be 1";
  EXPECT_FALSE(meta.uuid().value().empty()) << "uuid must be populated";
  EXPECT_TRUE(meta.has_archived_at()) << "archived_at must be set";
  EXPECT_TRUE(meta.has_payload_descriptor()) << "payload_descriptor must be set";
  EXPECT_EQ(meta.payload_descriptor().tier(), TIER_DISK) << "descriptor tier must be TIER_DISK";
}

TEST(StorageSidecar, SpillRamToDiskWritesSidecar) {
  DiskFixture f;

  AllocatePayloadRequest alloc_req;
  alloc_req.set_size_bytes(64);
  alloc_req.set_preferred_tier(TIER_RAM);
  const auto alloc_resp = f.catalog.Allocate(alloc_req);
  const auto desc       = alloc_resp.payload_descriptor();
  const auto id         = desc.payload_id();

  CommitPayloadRequest commit_req;
  *commit_req.mutable_id() = id;
  f.catalog.Commit(commit_req);

  // No sidecar yet — payload is on RAM tier.
  EXPECT_FALSE(std::filesystem::exists(f.SidecarPath(desc))) << "sidecar must not exist before spill";

  SpillRequest spill_req;
  *spill_req.add_ids() = id;
  spill_req.set_fsync(false);
  const auto spill_resp = f.catalog.Spill(spill_req);
  EXPECT_EQ(spill_resp.results_size(), 1);
  EXPECT_TRUE(spill_resp.results(0).ok()) << "spill must succeed";

  EXPECT_TRUE(std::filesystem::exists(f.SidecarPath(desc))) << "sidecar must exist after spill";

  auto meta = ParseSidecar(f.SidecarPath(desc));
  EXPECT_EQ(meta.metadata_version(), 1);
  EXPECT_TRUE(meta.has_archived_at());
  EXPECT_EQ(meta.payload_descriptor().tier(), TIER_DISK) << "descriptor in sidecar must show disk tier";
}

TEST(StorageSidecar, SidecarContainsLineage) {
  DiskFixture f;

  // Allocate a parent (RAM) and a child (disk).
  AllocatePayloadRequest parent_req;
  parent_req.set_size_bytes(32);
  parent_req.set_preferred_tier(TIER_RAM);
  const auto           parent_desc = f.catalog.Allocate(parent_req).payload_descriptor();
  CommitPayloadRequest commit_parent;
  *commit_parent.mutable_id() = parent_desc.payload_id();
  f.catalog.Commit(commit_parent);

  AllocatePayloadRequest child_req;
  child_req.set_size_bytes(32);
  child_req.set_preferred_tier(TIER_DISK);
  const auto child_desc = f.catalog.Allocate(child_req).payload_descriptor();

  // Add lineage edge before committing the child.
  AddLineageRequest lineage_req;
  *lineage_req.mutable_child() = child_desc.payload_id();
  auto* edge                   = lineage_req.add_parents();
  *edge->mutable_parent()      = parent_desc.payload_id();
  edge->set_operation("transform");
  edge->set_role("source");
  f.catalog.AddLineage(lineage_req);

  CommitPayloadRequest commit_child;
  *commit_child.mutable_id() = child_desc.payload_id();
  f.catalog.Commit(commit_child);

  EXPECT_TRUE(std::filesystem::exists(f.SidecarPath(child_desc))) << "sidecar must exist after commit";

  auto meta = ParseSidecar(f.SidecarPath(child_desc));
  EXPECT_EQ(meta.lineage_size(), 1) << "sidecar must contain one lineage edge";
  EXPECT_EQ(meta.lineage(0).operation(), "transform") << "lineage operation must match";
  EXPECT_EQ(meta.lineage(0).role(), "source") << "lineage role must match";
}

/*
  Object-storage spill integration test.

  Exercises the full spill-to-object-storage path against a live
  payload-manager connected to MinIO, then verifies both the payload binary
  (.bin) and its sidecar metadata (.meta.json) land correctly in the bucket
  using the Arrow S3 filesystem directly.

  Required environment variables:
    PAYLOAD_MANAGER_ENDPOINT  gRPC address, e.g. "payload-manager:50051"
    MINIO_ENDPOINT            S3 endpoint, e.g. "minio:9000"
    MINIO_BUCKET              Bucket payload-manager writes to, e.g. "payloads"

  Credentials are read from the standard AWS environment variables:
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION

  Covered:
    1. Allocate a RAM payload with eviction_policy.spill_target = TIER_OBJECT
    2. Write a known fill pattern into the shm segment and commit
    3. Spill (BLOCKING policy) — payload moves from RAM to TIER_OBJECT
    4. Verify SpillResponse.result.ok and descriptor.tier == TIER_OBJECT
    5. Verify ListPayloads(TIER_OBJECT) contains the payload
    6. Via Arrow S3: confirm <uuid>.bin exists with the correct byte size
    7. Via Arrow S3: download <uuid>.meta.json and parse as PayloadArchiveMetadata
         - uuid field matches payload_id
         - archived_at is populated
         - payload_descriptor.tier == TIER_OBJECT
         - metadata_version == 1
    8. Promote back to RAM (BLOCKING) and re-read bytes — fill pattern survives
    9. Delete; confirm payload is gone from ListPayloads
*/

#include <arrow/filesystem/s3fs.h>
#include <arrow/result.h>
#include <fcntl.h>
#include <google/protobuf/util/json_util.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>

#include "client/cpp/client.h"
#include "payload/manager/catalog/v1/archive_metadata.pb.h"
#include "payload/manager/services/v1/payload_catalog_service.grpc.pb.h"
#include "payload/manager/v1.hpp"

using namespace payload::manager::v1;
using namespace payload::manager::catalog::v1;
using payload::manager::client::PayloadClient;

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

#define ASSERT_OK(expr)                                                                                 \
  do {                                                                                                  \
    auto _s = (expr);                                                                                   \
    if (!_s.ok()) {                                                                                     \
      std::cerr << "FAIL [" #expr "]: " << _s.ToString() << "\n  at " __FILE__ ":" << __LINE__ << '\n'; \
      std::exit(1);                                                                                     \
    }                                                                                                   \
  } while (0)

#define ASSERT_TRUE(cond)                                                               \
  do {                                                                                  \
    if (!(cond)) {                                                                      \
      std::cerr << "FAIL assertion: " #cond "\n  at " __FILE__ ":" << __LINE__ << '\n'; \
      std::exit(1);                                                                     \
    }                                                                                   \
  } while (0)

#define ASSERT_EQ(a, b)                                                                                            \
  do {                                                                                                             \
    auto _a = (a);                                                                                                 \
    auto _b = (b);                                                                                                 \
    if (_a != _b) {                                                                                                \
      std::cerr << "FAIL: " #a " (" << _a << ") != " #b " (" << _b << ")\n  at " __FILE__ ":" << __LINE__ << '\n'; \
      std::exit(1);                                                                                                \
    }                                                                                                              \
  } while (0)

template <typename T>
T DieOnErr(arrow::Result<T> result, const char* what) {
  if (!result.ok()) {
    std::cerr << "FAIL [" << what << "]: " << result.status().ToString() << '\n';
    std::exit(1);
  }
  return result.MoveValueUnsafe();
}

// ---------------------------------------------------------------------------
// S3 helpers
// ---------------------------------------------------------------------------

namespace {

// Build an Arrow S3FileSystem pointing at the MinIO endpoint.
// Credentials are picked up from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.
std::shared_ptr<arrow::fs::S3FileSystem> MakeMinioFs(const std::string& minio_endpoint) {
  ASSERT_OK(arrow::fs::EnsureS3Initialized());

  arrow::fs::S3Options opts     = arrow::fs::S3Options::Defaults();
  opts.endpoint_override        = minio_endpoint;
  opts.scheme                   = "http";
  opts.force_virtual_addressing = false; // use path-style URLs for MinIO

  return DieOnErr(arrow::fs::S3FileSystem::Make(opts), "S3FileSystem::Make");
}

// Read an S3 object into a std::string.
std::string ReadS3Object(const std::shared_ptr<arrow::fs::FileSystem>& fs, const std::string& path) {
  auto input = DieOnErr(fs->OpenInputFile(path), ("OpenInputFile: " + path).c_str());
  auto size  = DieOnErr(input->GetSize(), "GetSize");
  auto buf   = DieOnErr(input->Read(size), "Read");
  return std::string(reinterpret_cast<const char*>(buf->data()), static_cast<size_t>(buf->size()));
}

// Convert a PayloadID to its hex-string key.
// PayloadID.value() may be a 16-byte binary UUID; convert to hex just as the server does.
std::string PayloadIdKey(const PayloadID& id) {
  const auto& v = id.value();
  if (v.size() == 16) {
    // Binary UUID → xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    static constexpr char kHex[] = "0123456789abcdef";
    const auto*           b      = reinterpret_cast<const uint8_t*>(v.data());
    std::string           s;
    s.reserve(36);
    for (int i = 0; i < 16; ++i) {
      if (i == 4 || i == 6 || i == 8 || i == 10) s += '-';
      s += kHex[b[i] >> 4];
      s += kHex[b[i] & 0xf];
    }
    return s;
  }
  return v;
}

// The ObjectArrowStore path layout is:  <root_path>/<uuid>.bin
// root_path is the resolved URI path of "s3://bucket", which becomes "bucket".
// So the Arrow S3 path for a payload is:  "<bucket>/<uuid>.bin"
std::string BinPath(const std::string& bucket, const PayloadID& id) {
  return bucket + "/" + PayloadIdKey(id) + ".bin";
}

std::string SidecarPath(const std::string& bucket, const PayloadID& id) {
  return bucket + "/" + PayloadIdKey(id) + ".meta.json";
}

// ---------------------------------------------------------------------------
// Allocate on RAM with spill_target=TIER_OBJECT, write fill pattern, commit.
// Returns the descriptor from AllocatePayload.
// ---------------------------------------------------------------------------

PayloadDescriptor AllocateAndFillAndCommit(PayloadCatalogService::Stub& catalog, const PayloadClient& client, uint64_t size_bytes, uint8_t fill) {
  // Allocate on RAM, requesting TIER_OBJECT as the spill destination.
  AllocatePayloadRequest alloc_req;
  alloc_req.set_size_bytes(size_bytes);
  alloc_req.set_preferred_tier(TIER_RAM);
  alloc_req.mutable_eviction_policy()->set_spill_target(TIER_OBJECT);

  AllocatePayloadResponse alloc_resp;
  grpc::ClientContext     alloc_ctx;
  auto                    alloc_status = catalog.AllocatePayload(&alloc_ctx, alloc_req, &alloc_resp);
  ASSERT_TRUE(alloc_status.ok());

  const auto& desc = alloc_resp.payload_descriptor();
  ASSERT_EQ(static_cast<int>(desc.tier()), static_cast<int>(TIER_RAM));
  ASSERT_TRUE(desc.has_ram());

  const auto& ram    = desc.ram();
  const auto& shm    = ram.shm_name();
  uint64_t    length = ram.length_bytes();
  ASSERT_EQ(length, size_bytes);

  // Open the shm segment the server created and write the fill pattern.
  // The client always mmaps at offset 0 (block_index is an internal
  // accounting field, not a byte offset into the shm file).
  int fd = shm_open(shm.c_str(), O_RDWR, S_IRUSR | S_IWUSR);
  ASSERT_TRUE(fd >= 0);
  if (ftruncate(fd, static_cast<off_t>(length)) != 0) {
    close(fd);
    std::cerr << "FAIL: ftruncate on shm " << shm << " failed: " << std::strerror(errno) << '\n';
    std::exit(1);
  }

  void* base = mmap(nullptr, static_cast<size_t>(length), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  ASSERT_TRUE(base != MAP_FAILED);

  std::memset(base, fill, static_cast<size_t>(length));
  munmap(base, static_cast<size_t>(length));
  close(fd);

  // Commit makes the payload visible and immutable.
  ASSERT_OK(client.CommitPayload(desc.payload_id()));

  return desc;
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

void TestSpillToObject(const std::string& endpoint, const std::string& minio_endpoint, const std::string& bucket) {
  std::cout << "  TestSpillToObject: RAM → TIER_OBJECT spill, verify .bin + .meta.json\n";

  constexpr uint64_t kSize = 512;
  constexpr uint8_t  kFill = 0x5A;

  auto                                         channel = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
  PayloadClient                                client(channel);
  std::unique_ptr<PayloadCatalogService::Stub> catalog = PayloadCatalogService::NewStub(channel);

  // --- 1 & 2. Allocate + fill + commit ---
  const auto  desc = AllocateAndFillAndCommit(*catalog, client, kSize, kFill);
  const auto& id   = desc.payload_id();

  // --- 3. Spill to TIER_OBJECT (blocking) ---
  SpillRequest spill_req;
  *spill_req.add_ids() = id;
  spill_req.set_policy(SPILL_POLICY_BLOCKING);
  spill_req.set_fsync(false);

  auto spill_resp = client.Spill(spill_req);
  ASSERT_OK(spill_resp.status());
  ASSERT_EQ(spill_resp->results_size(), 1);

  const auto& result = spill_resp->results(0);

  // --- 4. Verify SpillResponse ---
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(static_cast<int>(result.payload_descriptor().tier()), static_cast<int>(TIER_OBJECT));

  // --- 5. Verify ListPayloads(TIER_OBJECT) ---
  ListPayloadsRequest list_req;
  list_req.set_tier_filter(TIER_OBJECT);
  auto list_resp = client.ListPayloads(list_req);
  ASSERT_OK(list_resp.status());

  bool found_in_object_list = false;
  for (const auto& s : list_resp->payloads()) {
    if (s.id().value() == id.value()) {
      found_in_object_list = true;
      ASSERT_EQ(static_cast<int>(s.tier()), static_cast<int>(TIER_OBJECT));
    }
  }
  ASSERT_TRUE(found_in_object_list);

  // --- 6 & 7. Verify .bin and .meta.json in MinIO via Arrow S3 ---
  auto s3fs = MakeMinioFs(minio_endpoint);

  const std::string bin_path  = BinPath(bucket, id);
  const std::string meta_path = SidecarPath(bucket, id);

  // .bin must exist with the correct size.
  auto bin_info = DieOnErr(s3fs->GetFileInfo(bin_path), ("GetFileInfo bin: " + bin_path).c_str());
  ASSERT_TRUE(bin_info.type() == arrow::fs::FileType::File);
  ASSERT_EQ(static_cast<uint64_t>(bin_info.size()), kSize);

  // .meta.json must exist and parse as PayloadArchiveMetadata.
  auto meta_info = DieOnErr(s3fs->GetFileInfo(meta_path), ("GetFileInfo meta: " + meta_path).c_str());
  ASSERT_TRUE(meta_info.type() == arrow::fs::FileType::File);
  ASSERT_TRUE(meta_info.size() > 0);

  const std::string json = ReadS3Object(s3fs, meta_path);
  ASSERT_TRUE(!json.empty());

  PayloadArchiveMetadata meta;
  auto                   parse_status = google::protobuf::util::JsonStringToMessage(json, &meta);
  ASSERT_TRUE(parse_status.ok());

  ASSERT_EQ(meta.uuid().value(), id.value());
  ASSERT_TRUE(meta.has_archived_at());
  ASSERT_TRUE(meta.archived_at().seconds() > 0);
  ASSERT_EQ(meta.metadata_version(), 1u);
  ASSERT_TRUE(meta.has_payload_descriptor());
  ASSERT_EQ(static_cast<int>(meta.payload_descriptor().tier()), static_cast<int>(TIER_OBJECT));

  std::cout << "    .bin  size=" << bin_info.size() << " bytes  ✓\n";
  std::cout << "    .meta.json parsed, uuid matches, archived_at=" << meta.archived_at().seconds() << "  ✓\n";

  // --- 8. Promote back to RAM and verify data integrity ---
  PromoteRequest prom_req;
  *prom_req.mutable_id() = id;
  prom_req.set_target_tier(TIER_RAM);
  prom_req.set_policy(PROMOTION_POLICY_BLOCKING);
  auto prom_resp = client.Promote(prom_req);
  ASSERT_OK(prom_resp.status());
  ASSERT_EQ(static_cast<int>(prom_resp->payload_descriptor().tier()), static_cast<int>(TIER_RAM));

  auto rd = client.AcquireReadableBuffer(id);
  ASSERT_OK(rd.status());
  auto rp = rd.ValueOrDie();
  ASSERT_EQ(static_cast<uint64_t>(rp.buffer->size()), kSize);
  for (uint64_t i = 0; i < kSize; ++i) {
    ASSERT_EQ(rp.buffer->data()[i], kFill);
  }
  ASSERT_OK(client.Release(rp.lease_id));

  // --- 9. Delete and verify gone ---
  DeleteRequest del_req;
  *del_req.mutable_id() = id;
  del_req.set_force(false);
  ASSERT_OK(client.Delete(del_req));

  ListPayloadsRequest all_req;
  auto                after = client.ListPayloads(all_req);
  ASSERT_OK(after.status());
  for (const auto& s : after->payloads()) {
    ASSERT_TRUE(s.id().value() != id.value());
  }
}

} // namespace

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
  const char*       env_ep     = std::getenv("PAYLOAD_MANAGER_ENDPOINT");
  const char*       env_minio  = std::getenv("MINIO_ENDPOINT");
  const char*       env_bucket = std::getenv("MINIO_BUCKET");
  const std::string endpoint   = (argc > 1) ? argv[1] : (env_ep ? env_ep : "");

  if (endpoint.empty() || !env_minio || !env_bucket) {
    std::cout << "object_spill_integration_test: skip\n"
              << "  Set PAYLOAD_MANAGER_ENDPOINT, MINIO_ENDPOINT, and MINIO_BUCKET to run.\n";
    return 0;
  }

  const std::string minio_endpoint = env_minio;
  const std::string bucket         = env_bucket;

  std::cout << "object_spill_integration_test: connecting to " << endpoint << '\n';
  std::cout << "  MinIO: " << minio_endpoint << "  bucket: " << bucket << '\n';

  // Pre-create the bucket so payload-manager does not need allow_bucket_creation.
  // Arrow's S3FileSystem::CreateDir at the bucket level (top-level path) maps
  // to CreateBucket in the S3 API.  Failures are ignored in case the bucket
  // already exists.
  {
    ASSERT_OK(arrow::fs::EnsureS3Initialized());
    arrow::fs::S3Options opts     = arrow::fs::S3Options::Defaults();
    opts.endpoint_override        = minio_endpoint;
    opts.scheme                   = "http";
    opts.force_virtual_addressing = false;
    auto s3fs                     = DieOnErr(arrow::fs::S3FileSystem::Make(opts), "pre-create bucket: S3FileSystem::Make");
    (void)s3fs->CreateDir(bucket, /*recursive=*/false);
  }

  TestSpillToObject(endpoint, minio_endpoint, bucket);

  std::cout << "object_spill_integration_test: pass\n";

  ASSERT_OK(arrow::fs::EnsureS3Finalized());
  return 0;
}

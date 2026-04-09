/*
  Object-tier client-upload integration test.

  Exercises the full client-side upload path end-to-end:
    AllocatePayload(TIER_OBJECT)  →  write bytes into local buffer
    CommitPayload                 →  client uploads to S3, calls ImportPayload RPC
    ResolveSnapshot               →  verify payload is DURABLE on TIER_OBJECT
    ListPayloads(TIER_OBJECT)     →  payload appears in listing
    Arrow S3 direct check         →  .bin and .meta.json land in the bucket
    Delete                        →  cleanup

  Required environment variables (same as object_spill_integration_test):
    PAYLOAD_MANAGER_ENDPOINT  gRPC address, e.g. "payload-manager:50051"
    MINIO_ENDPOINT            S3 endpoint, e.g. "minio:9000"
    MINIO_BUCKET              Bucket name, e.g. "payloads"

  S3 credentials:
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION

  Build/run:
    This test is registered automatically when payload_manager::client is
    available (see tests/integration/CMakeLists.txt).

    ctest -R object_client_upload_integration --output-on-failure
*/

#include <arrow/buffer.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/result.h>
#include <google/protobuf/util/json_util.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>

#include "client/cpp/client.h"
#include "payload/manager/catalog/v1/archive_metadata.pb.h"
#include "payload/manager/v1.hpp"

using namespace payload::manager::v1;
using namespace payload::manager::catalog::v1;
using payload::manager::client::PayloadClient;

// ---------------------------------------------------------------------------
// Assertion helpers (same pattern as object_spill_integration_test.cpp)
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

#define ASSERT_EQ(a, b)                                                                                              \
  do {                                                                                                               \
    if ((a) != (b)) {                                                                                                \
      std::cerr << "FAIL: " #a " (" << (a) << ") != " #b " (" << (b) << ")\n  at " __FILE__ ":" << __LINE__ << '\n'; \
      std::exit(1);                                                                                                  \
    }                                                                                                                \
  } while (0)

static const char* RequireEnv(const char* name) {
  const char* val = std::getenv(name);
  if (!val || *val == '\0') {
    std::cerr << "Required environment variable not set: " << name << '\n';
    std::exit(1);
  }
  return val;
}

// ---------------------------------------------------------------------------
// Build an Arrow S3FileSystem pointed at MinIO for result verification.
// ---------------------------------------------------------------------------

std::pair<std::shared_ptr<arrow::fs::S3FileSystem>, std::string> MakeMinioS3Fs(const std::string& endpoint, const std::string& bucket) {
  ASSERT_OK(arrow::fs::EnsureS3Initialized());

  arrow::fs::S3Options opts;
  opts.ConfigureDefaultCredentials();
  opts.endpoint_override = endpoint;
  opts.scheme            = "http";

  auto result = arrow::fs::S3FileSystem::Make(opts);
  ASSERT_OK(result.status());
  return {*result, bucket};
}

// ---------------------------------------------------------------------------
// Helper: extract UUID hex string from a binary PayloadID
// ---------------------------------------------------------------------------

std::string IdToHex(const PayloadID& id) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string           hex;
  hex.reserve(32);
  for (unsigned char c : id.value()) {
    hex.push_back(kHex[c >> 4]);
    hex.push_back(kHex[c & 0x0fu]);
  }
  return hex;
}

// ---------------------------------------------------------------------------
// Main test
// ---------------------------------------------------------------------------

int main() {
  const std::string pm_endpoint    = RequireEnv("PAYLOAD_MANAGER_ENDPOINT");
  const std::string minio_endpoint = RequireEnv("MINIO_ENDPOINT");
  const std::string minio_bucket   = RequireEnv("MINIO_BUCKET");

  // Build the Arrow S3FileSystem the client will use for direct uploads.
  // This mirrors what a real caller would do by constructing S3Options from
  // a FileSystemOptions proto (arrow_fs_s3.proto) and calling S3FileSystem::Make.
  ASSERT_OK(arrow::fs::EnsureS3Initialized());
  arrow::fs::S3Options client_opts;
  client_opts.ConfigureDefaultCredentials();
  client_opts.endpoint_override = minio_endpoint;
  client_opts.scheme            = "http";
  auto make_result              = arrow::fs::S3FileSystem::Make(client_opts);
  ASSERT_OK(make_result.status());
  std::shared_ptr<arrow::fs::FileSystem> client_fs = *make_result;

  // Connect to the payload manager.
  auto          channel = grpc::CreateChannel(pm_endpoint, grpc::InsecureChannelCredentials());
  PayloadClient client(channel, client_fs);

  // -------------------------------------------------------------------------
  // Step 1 — allocate a TIER_OBJECT payload.
  // -------------------------------------------------------------------------
  std::cout << "[1] Allocating TIER_OBJECT payload...\n";
  constexpr uint64_t kPayloadSize = 1024;

  AllocatePayloadRequest alloc_req;
  alloc_req.set_size_bytes(kPayloadSize);
  alloc_req.set_preferred_tier(TIER_OBJECT);

  auto alloc_result = client.AllocateWritableBuffer(kPayloadSize, TIER_OBJECT);
  ASSERT_OK(alloc_result.status());

  const auto& writable   = *alloc_result;
  const auto  payload_id = writable.descriptor.payload_id();
  const auto  uuid_hex   = IdToHex(payload_id);

  ASSERT_TRUE(!payload_id.value().empty());
  ASSERT_TRUE(writable.buffer != nullptr);
  ASSERT_EQ(static_cast<uint64_t>(writable.buffer->size()), kPayloadSize);

  std::cout << "  payload_id=" << uuid_hex << '\n';
  std::cout << "  buffer size=" << writable.buffer->size() << '\n';

  // -------------------------------------------------------------------------
  // Step 2 — write a known fill pattern into the local buffer.
  // -------------------------------------------------------------------------
  std::cout << "[2] Writing fill pattern 0xBE into local buffer...\n";
  std::memset(writable.buffer->mutable_data(), 0xBE, static_cast<size_t>(kPayloadSize));

  // -------------------------------------------------------------------------
  // Step 3 — CommitPayload: uploads to S3 then calls ImportPayload RPC.
  // -------------------------------------------------------------------------
  std::cout << "[3] Committing (upload → ImportPayload RPC)...\n";
  ASSERT_OK(client.CommitPayload(payload_id));
  std::cout << "  CommitPayload OK\n";

  // -------------------------------------------------------------------------
  // Step 4 — ResolveSnapshot: verify DURABLE on TIER_OBJECT.
  // -------------------------------------------------------------------------
  std::cout << "[4] Resolving snapshot...\n";
  auto resolve_result = client.Resolve(payload_id);
  ASSERT_OK(resolve_result.status());
  const auto& descriptor = resolve_result->payload_descriptor();
  ASSERT_EQ(descriptor.tier(), TIER_OBJECT);
  ASSERT_EQ(descriptor.state(), PAYLOAD_STATE_DURABLE);
  std::cout << "  tier=TIER_OBJECT state=PAYLOAD_STATE_DURABLE  OK\n";

  // -------------------------------------------------------------------------
  // Step 5 — ListPayloads: payload must appear in TIER_OBJECT listing.
  // -------------------------------------------------------------------------
  std::cout << "[5] Listing TIER_OBJECT payloads...\n";
  ListPayloadsRequest list_req;
  list_req.set_tier_filter(TIER_OBJECT);
  auto list_result = client.ListPayloads(list_req);
  ASSERT_OK(list_result.status());
  bool found = false;
  for (const auto& summary : list_result->payloads()) {
    if (summary.id().value() == payload_id.value()) {
      found = true;
      break;
    }
  }
  ASSERT_TRUE(found) /* payload must appear in TIER_OBJECT listing */;
  std::cout << "  Found in listing  OK\n";

  // -------------------------------------------------------------------------
  // Step 6 — Arrow S3: verify .bin exists and has the correct size.
  // -------------------------------------------------------------------------
  std::cout << "[6] Verifying .bin via Arrow S3...\n";
  auto [verify_fs, bucket_root] = MakeMinioS3Fs(minio_endpoint, minio_bucket);
  const std::string bin_key     = minio_bucket + "/" + uuid_hex + ".bin";
  auto              info_result = verify_fs->GetFileInfo(bin_key);
  ASSERT_OK(info_result.status());
  ASSERT_EQ(static_cast<uint64_t>(info_result->size()), kPayloadSize);
  std::cout << "  " << bin_key << "  size=" << info_result->size() << "  OK\n";

  // -------------------------------------------------------------------------
  // Step 7 — Arrow S3: read back bytes and verify fill pattern.
  // -------------------------------------------------------------------------
  std::cout << "[7] Verifying fill pattern survives S3 round-trip...\n";
  auto in_result = verify_fs->OpenInputFile(bin_key);
  ASSERT_OK(in_result.status());
  auto read_result = (*in_result)->Read(kPayloadSize);
  ASSERT_OK(read_result.status());
  const auto& read_buf   = *read_result;
  bool        pattern_ok = true;
  for (int64_t i = 0; i < read_buf->size(); ++i) {
    if (static_cast<unsigned char>(read_buf->data()[i]) != 0xBE) {
      pattern_ok = false;
      break;
    }
  }
  ASSERT_TRUE(pattern_ok) /* fill pattern must match */;
  std::cout << "  Pattern 0xBE verified across " << read_buf->size() << " bytes  OK\n";

  // -------------------------------------------------------------------------
  // Step 8 — Arrow S3: verify .meta.json exists and parses correctly.
  // -------------------------------------------------------------------------
  std::cout << "[8] Verifying .meta.json sidecar...\n";
  const std::string meta_key  = minio_bucket + "/" + uuid_hex + ".meta.json";
  auto              meta_info = verify_fs->GetFileInfo(meta_key);
  ASSERT_OK(meta_info.status());
  ASSERT_TRUE(meta_info->size() > 0);

  auto meta_stream = verify_fs->OpenInputFile(meta_key);
  ASSERT_OK(meta_stream.status());
  auto meta_buf = (*meta_stream)->Read(meta_info->size());
  ASSERT_OK(meta_buf.status());

  PayloadArchiveMetadata meta;
  const std::string      json_str(reinterpret_cast<const char*>((*meta_buf)->data()), static_cast<size_t>((*meta_buf)->size()));
  auto                   parse_status = google::protobuf::util::JsonStringToMessage(json_str, &meta);
  ASSERT_TRUE(parse_status.ok());
  ASSERT_EQ(meta.payload_descriptor().tier(), TIER_OBJECT);
  ASSERT_EQ(meta.metadata_version(), 1u);
  ASSERT_EQ(meta.uuid().value(), payload_id.value());
  std::cout << "  .meta.json valid  metadata_version=1  OK\n";

  // -------------------------------------------------------------------------
  // Step 9 — Delete and confirm gone from listing.
  // -------------------------------------------------------------------------
  std::cout << "[9] Deleting payload...\n";
  DeleteRequest del_req;
  *del_req.mutable_id() = payload_id;
  del_req.set_force(false);
  ASSERT_OK(client.Delete(del_req));

  auto list_after = client.ListPayloads(list_req);
  ASSERT_OK(list_after.status());
  bool still_found = false;
  for (const auto& summary : list_after->payloads()) {
    if (summary.id().value() == payload_id.value()) {
      still_found = true;
      break;
    }
  }
  ASSERT_TRUE(!still_found) /* payload must be gone after delete */;
  std::cout << "  Deleted and removed from listing  OK\n";

  std::cout << "\nAll checks passed.\n";
  return 0;
}

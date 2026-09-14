// Filesystem and compression resolution (internal/storage/common/arrow_utils).
//
// These are the pure decisions every storage tier routes through — which
// filesystem backs a path, and which codec to write with — and they had no
// tests at all. The S3 and GCS branches need a live client and stay out of
// reach here; the local filesystem branch, the compression mapping, and the
// Unwrap helpers do not, and they are where a mistake is silent: resolving to
// the wrong codec writes a file nothing complains about until something reads
// it back.

#include "internal/storage/common/arrow_utils.hpp"

#include <arrow/buffer.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>

namespace payload::storage::common {
namespace {

namespace pbs = pb::arrow::storage;

pbs::ObjectStorageConfig LocalConfig() {
  pbs::ObjectStorageConfig cfg;
  cfg.set_filesystem(pbs::FILE_SYSTEM_LOCAL);
  return cfg;
}

} // namespace

// ---------------------------------------------------------------------------
// ResolveCompression
// ---------------------------------------------------------------------------

TEST(ResolveCompression, ExplicitCodecsMapStraightThrough) {
  // Each arm of the switch, so a reordering or a missing case shows up here
  // rather than as a file written with the wrong codec.
  const std::pair<pbs::Compression, arrow::Compression::type> cases[] = {
      {pbs::COMPRESSION_UNCOMPRESSED, arrow::Compression::UNCOMPRESSED},
      {pbs::COMPRESSION_SNAPPY, arrow::Compression::SNAPPY},
      {pbs::COMPRESSION_GZIP, arrow::Compression::GZIP},
      {pbs::COMPRESSION_BROTLI, arrow::Compression::BROTLI},
      {pbs::COMPRESSION_ZSTD, arrow::Compression::ZSTD},
      {pbs::COMPRESSION_LZ4, arrow::Compression::LZ4},
      {pbs::COMPRESSION_LZ4_FRAME, arrow::Compression::LZ4_FRAME},
      {pbs::COMPRESSION_LZO, arrow::Compression::LZO},
      {pbs::COMPRESSION_BZ2, arrow::Compression::BZ2},
  };

  for (const auto& [proto_value, expected] : cases) {
    // The path is irrelevant for an explicit codec — it must not override.
    auto result = ResolveCompression("payload.bin", proto_value);
    ASSERT_TRUE(result.ok()) << pbs::Compression_Name(proto_value);
    EXPECT_EQ(*result, expected) << pbs::Compression_Name(proto_value);
  }
}

TEST(ResolveCompression, ExplicitCodecBeatsTheExtension) {
  // A .gz path with an explicit UNCOMPRESSED setting stays uncompressed.
  auto result = ResolveCompression("payload.bin.gz", pbs::COMPRESSION_UNCOMPRESSED);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, arrow::Compression::UNCOMPRESSED);
}

TEST(ResolveCompression, AutoInfersFromTheExtension) {
  const std::pair<const char*, arrow::Compression::type> cases[] = {
      {"payload.bin.gz", arrow::Compression::GZIP},
      {"payload.bin.zst", arrow::Compression::ZSTD},
      {"payload.bin.zstd", arrow::Compression::ZSTD},
      {"payload.bin.snappy", arrow::Compression::SNAPPY},
      {"payload.bin.sz", arrow::Compression::SNAPPY},
      {"payload.bin.br", arrow::Compression::BROTLI},
      {"payload.bin.bz2", arrow::Compression::BZ2},
      {"payload.bin.lzo", arrow::Compression::LZO},
      // .lz4 on disk is the framed format, not raw blocks.
      {"payload.bin.lz4", arrow::Compression::LZ4_FRAME},
      // Case should not matter.
      {"PAYLOAD.BIN.GZ", arrow::Compression::GZIP},
  };

  for (const auto& [path, expected] : cases) {
    auto result = ResolveCompression(path, pbs::COMPRESSION_AUTO);
    ASSERT_TRUE(result.ok()) << path;
    EXPECT_EQ(*result, expected) << path;
  }
}

TEST(ResolveCompression, AutoFallsBackToUncompressed) {
  // No recognised suffix, and an empty path: AUTO must resolve to something
  // rather than fail, since every write goes through here.
  for (const char* path : {"payload.bin", "payload", "", "archive.tar.unknown"}) {
    auto result = ResolveCompression(path, pbs::COMPRESSION_AUTO);
    ASSERT_TRUE(result.ok()) << path;
    EXPECT_EQ(*result, arrow::Compression::UNCOMPRESSED) << path;
  }
}

// ---------------------------------------------------------------------------
// ResolveFileSystem
// ---------------------------------------------------------------------------

TEST(ResolveFileSystem, LocalReturnsALocalFilesystemAndKeepsThePath) {
  pbs::FileSystemOptions opts;
  auto                   result = ResolveFileSystem("/var/lib/payload-manager/payloads", pbs::FILE_SYSTEM_LOCAL, opts);
  ASSERT_TRUE(result.ok()) << result.status().ToString();

  const auto& [fs, path] = *result;
  ASSERT_NE(fs, nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<arrow::fs::LocalFileSystem>(fs), nullptr);
  // The local branch passes the path through untouched — no URI stripping.
  EXPECT_EQ(path, "/var/lib/payload-manager/payloads");
}

TEST(ResolveFileSystem, LocalIgnoresFilesystemOptions) {
  // Options belong to the object backends; setting them must not divert the
  // local branch, which is checked before the options are looked at.
  pbs::FileSystemOptions opts;
  opts.mutable_s3()->set_endpoint_override("minio:9000");

  auto result = ResolveFileSystem("/tmp/payloads", pbs::FILE_SYSTEM_LOCAL, opts);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_NE(std::dynamic_pointer_cast<arrow::fs::LocalFileSystem>(result->first), nullptr);
  EXPECT_EQ(result->second, "/tmp/payloads");
}

TEST(ResolveFileSystem, ConfigOverloadDelegatesToTheSameResolution) {
  auto result = ResolveFileSystem("/tmp/payloads", LocalConfig());
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_NE(std::dynamic_pointer_cast<arrow::fs::LocalFileSystem>(result->first), nullptr);
  EXPECT_EQ(result->second, "/tmp/payloads");
}

TEST(ResolveFileSystem, RelativeAndEmptyLocalPathsAreAccepted) {
  // The disk tier hands whatever is configured straight in; resolution should
  // not be the thing that rejects it.
  pbs::FileSystemOptions opts;
  for (const char* path : {"payloads", "./payloads", ""}) {
    auto result = ResolveFileSystem(path, pbs::FILE_SYSTEM_LOCAL, opts);
    ASSERT_TRUE(result.ok()) << path;
    EXPECT_EQ(result->second, path);
  }
}

// ---------------------------------------------------------------------------
// Unwrap / ReadAll
// ---------------------------------------------------------------------------

TEST(Unwrap, ReturnsTheValueWhenOk) {
  arrow::Result<int> ok(42);
  EXPECT_EQ(Unwrap(ok), 42);
}

TEST(Unwrap, ThrowsCarryingTheStatusText) {
  arrow::Result<int> bad(arrow::Status::IOError("disk on fire"));
  try {
    (void)Unwrap(bad);
    FAIL() << "expected Unwrap to throw";
  } catch (const std::runtime_error& e) {
    // The message is what surfaces to callers as the failure reason, so it has
    // to carry the status rather than a generic string.
    EXPECT_NE(std::string(e.what()).find("disk on fire"), std::string::npos) << e.what();
  }
}

TEST(Unwrap, StatusOverloadIsANoOpWhenOkAndThrowsOtherwise) {
  EXPECT_NO_THROW(Unwrap(arrow::Status::OK()));
  EXPECT_THROW(Unwrap(arrow::Status::Invalid("bad argument")), std::runtime_error);
}

} // namespace payload::storage::common

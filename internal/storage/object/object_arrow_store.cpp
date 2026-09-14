#include "object_arrow_store.hpp"

#include <arrow/filesystem/s3fs.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <google/protobuf/util/json_util.h>
#include <spdlog/spdlog.h>

#include "internal/storage/common/arrow_utils.hpp"
#include "internal/storage/common/path_utils.hpp"
#include "internal/util/uuid.hpp"
#include "payload/manager/v1.hpp"

namespace payload::storage {

using namespace payload::storage::common;
using namespace payload::manager::v1;

namespace {

// Convert a PayloadID to its hex-string key, handling binary (16-byte) UUIDs.
std::string Key(const PayloadID& id) {
  if (id.value().size() == 16) {
    payload::util::UUID uuid{};
    static_assert(sizeof(uuid) == 16, "UUID must be exactly 16 bytes");
    std::memcpy(uuid.data(), id.value().data(), sizeof(uuid));
    return payload::util::ToString(uuid);
  }
  return id.value();
}

} // namespace

ObjectArrowStore::ObjectArrowStore(std::shared_ptr<arrow::fs::FileSystem> fs, std::string root_path, bool is_s3)
    : fs_(std::move(fs)), root_path_(std::move(root_path)), is_s3_(is_s3) {
  // Ensure the root exists before anything tries to write into it.
  //
  // allow_bucket_creation is an S3Options flag that Arrow consults in
  // CreateDir and nowhere else — OpenOutputStream will not create a bucket
  // implicitly. Nothing called CreateDir, so with the flag set and the bucket
  // absent every spill failed at the first upload:
  //
  //   AWS Error NO_SUCH_BUCKET during CreateMultipartUpload operation
  //
  // CreateDir is a no-op when the root already exists, so this is safe on
  // every start. It is not fatal when it fails: the object tier may be
  // configured against a bucket an operator creates out of band, with
  // allow_bucket_creation deliberately off, and refusing to start would be
  // worse than the clear error a later write produces. Say so loudly instead.
  auto status = fs_->CreateDir(root_path_, /*recursive=*/true);
  if (status.ok()) {
    spdlog::debug("[obj] root ready path={}", root_path_);
  } else {
    spdlog::warn(
        "[obj] could not create root '{}': {} — writes will fail unless it already exists "
        "(for S3, set storage.object.filesystem_options.s3.allow_bucket_creation or create the bucket)",
        root_path_, status.ToString());
  }
}

ObjectArrowStore::~ObjectArrowStore() {
  fs_.reset();
  if (is_s3_) {
    (void)arrow::fs::EnsureS3Finalized();
  }
}

/*
  Object key layout:

      <root_path>/<uuid>.bin       — payload data
      <root_path>/<uuid>.meta.json — sidecar metadata
*/
std::string ObjectArrowStore::ObjectPath(const PayloadID& id) const {
  const auto key = Key(id);
  common::ValidatePayloadId(key);
  if (!root_path_.empty() && root_path_.back() == '/') {
    return root_path_ + key + ".bin";
  }
  return root_path_ + "/" + key + ".bin";
}

std::string ObjectArrowStore::SidecarObjectPath(const PayloadID& id) const {
  const auto key = Key(id);
  common::ValidatePayloadId(key);
  if (!root_path_.empty() && root_path_.back() == '/') {
    return root_path_ + key + ".meta.json";
  }
  return root_path_ + "/" + key + ".meta.json";
}

std::string ObjectArrowStore::GetUploadUri(const PayloadID& id) const {
  const auto key = Key(id);
  common::ValidatePayloadId(key);
  std::string path;
  if (!root_path_.empty() && root_path_.back() == '/') {
    path = root_path_ + key + ".bin";
  } else {
    path = root_path_ + "/" + key + ".bin";
  }
  return is_s3_ ? ("s3://" + path) : path;
}

/*
  Object store cannot allocate writable buffers.
*/
std::shared_ptr<arrow::Buffer> ObjectArrowStore::Allocate(const PayloadID&, uint64_t) {
  throw std::runtime_error("object tier does not support direct allocation");
}

/*
  Download full object
*/
std::shared_ptr<arrow::Buffer> ObjectArrowStore::Read(const PayloadID& id) {
  auto input = Unwrap(fs_->OpenInputFile(ObjectPath(id)));
  auto size  = Unwrap(input->GetSize());
  return Unwrap(input->Read(size));
}

uint64_t ObjectArrowStore::Size(const PayloadID& id) {
  auto info = Unwrap(fs_->GetFileInfo(ObjectPath(id)));
  return static_cast<uint64_t>(info.size());
}

/*
  Upload buffer as object.

  fsync flag ignored — object stores are atomic per PUT.
*/
void ObjectArrowStore::Write(const PayloadID& id, const std::shared_ptr<arrow::Buffer>& buffer, bool /*fsync*/) {
  const auto path = ObjectPath(id);
  spdlog::debug("[obj] Write begin path={} size={}", path, buffer ? buffer->size() : -1L);
  auto out = Unwrap(fs_->OpenOutputStream(path));
  Unwrap(out->Write(buffer->data(), buffer->size()));
  Unwrap(out->Close());
  spdlog::debug("[obj] Write closed OK path={}", path);
}

/*
  Delete object and sidecar (sidecar removal is best-effort).
*/
void ObjectArrowStore::Remove(const PayloadID& id) {
  Unwrap(fs_->DeleteFile(ObjectPath(id)));
  (void)fs_->DeleteFile(SidecarObjectPath(id));
}

/*
  Write <uuid>.meta.json to object storage.
*/
void ObjectArrowStore::WriteSidecar(const PayloadID& id, const payload::manager::catalog::v1::PayloadArchiveMetadata& meta) {
  google::protobuf::util::JsonPrintOptions opts;
  opts.add_whitespace = true;

  std::string json;
  auto        status = google::protobuf::util::MessageToJsonString(meta, &json, opts);
  if (!status.ok()) {
    throw std::runtime_error("WriteSidecar: serialization failed: " + status.ToString());
  }

  const auto path = SidecarObjectPath(id);
  auto       out  = Unwrap(fs_->OpenOutputStream(path));
  Unwrap(out->Write(json.data(), static_cast<int64_t>(json.size())));
  Unwrap(out->Close());
}

} // namespace payload::storage

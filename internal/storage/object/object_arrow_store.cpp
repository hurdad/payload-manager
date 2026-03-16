#include "object_arrow_store.hpp"

#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <google/protobuf/util/json_util.h>

#include "internal/storage/common/arrow_utils.hpp"
#include "internal/storage/common/path_utils.hpp"
#include "payload/manager/v1.hpp"

namespace payload::storage {

using namespace payload::storage::common;
using namespace payload::manager::v1;

ObjectArrowStore::ObjectArrowStore(std::shared_ptr<arrow::fs::FileSystem> fs, std::string root_path)
    : fs_(std::move(fs)), root_path_(std::move(root_path)) {
}

/*
  Object key layout:

      <root_path>/<uuid>.bin       — payload data
      <root_path>/<uuid>.meta.json — sidecar metadata
*/
std::string ObjectArrowStore::ObjectPath(const PayloadID& id) const {
  common::ValidatePayloadId(id.value());
  if (!root_path_.empty() && root_path_.back() == '/') {
    return root_path_ + id.value() + ".bin";
  }
  return root_path_ + "/" + id.value() + ".bin";
}

std::string ObjectArrowStore::SidecarObjectPath(const PayloadID& id) const {
  common::ValidatePayloadId(id.value());
  if (!root_path_.empty() && root_path_.back() == '/') {
    return root_path_ + id.value() + ".meta.json";
  }
  return root_path_ + "/" + id.value() + ".meta.json";
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
  auto out = Unwrap(fs_->OpenOutputStream(ObjectPath(id)));
  Unwrap(out->Write(buffer->data(), buffer->size()));
  Unwrap(out->Close());
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
  opts.add_whitespace                = true;
  opts.always_print_primitive_fields = false;

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

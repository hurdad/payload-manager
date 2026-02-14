#include "object_arrow_store.hpp"

#include <arrow/io/interfaces.h>
#include <arrow/result.h>

#include "internal/storage/common/arrow_utils.hpp"
#include "payload/manager/v1_compat.hpp"

namespace payload::storage {

using namespace payload::storage::common;
using namespace payload::manager::v1;

ObjectArrowStore::ObjectArrowStore(std::shared_ptr<arrow::fs::S3FileSystem> fs,
                                   std::string bucket,
                                   std::string prefix)
    : fs_(std::move(fs)),
      bucket_(std::move(bucket)),
      prefix_(std::move(prefix)) {}

/*
  Object key layout:

      s3://bucket/prefix/<uuid>.bin
*/
std::string ObjectArrowStore::ObjectPath(const PayloadID& id) const {
  return bucket_ + "/" + prefix_ + "/" + id.value() + ".bin";
}

/*
  Object store cannot allocate writable buffers.
*/
std::shared_ptr<arrow::Buffer>
ObjectArrowStore::Allocate(const PayloadID&, uint64_t) {
  throw std::runtime_error("object tier does not support direct allocation");
}

/*
  Download full object
*/
std::shared_ptr<arrow::Buffer>
ObjectArrowStore::Read(const PayloadID& id) {

  auto input = Unwrap(fs_->OpenInputFile(ObjectPath(id)));
  auto size = Unwrap(input->GetSize());
  return Unwrap(input->Read(size));
}

/*
  Upload buffer as object.

  fsync flag ignored — object stores are atomic per PUT.
*/
void ObjectArrowStore::Write(const PayloadID& id,
                             const std::shared_ptr<arrow::Buffer>& buffer,
                             bool /*fsync*/) {

  auto out = Unwrap(fs_->OpenOutputStream(ObjectPath(id)));
  Unwrap(out->Write(buffer->data(), buffer->size()));
  Unwrap(out->Close());
}

/*
  Delete object
*/
void ObjectArrowStore::Remove(const PayloadID& id) {
  Unwrap(fs_->DeleteFile(ObjectPath(id)));
}

}

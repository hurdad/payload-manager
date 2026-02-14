#include "disk_arrow_store.hpp"

#include <arrow/io/file.h>
#include <filesystem>

#include "internal/storage/common/path_utils.hpp"
#include "internal/storage/common/arrow_utils.hpp"
#include "payload/manager/v1_compat.hpp"

namespace payload::storage {

using namespace payload::storage::common;
using namespace payload::manager::v1;

DiskArrowStore::DiskArrowStore(std::filesystem::path root)
    : root_(std::move(root)) {

  std::filesystem::create_directories(root_);
}

/*
  Disk tier cannot allocate writable buffers.
  Only RAM/GPU tiers allocate.
*/
std::shared_ptr<arrow::Buffer>
DiskArrowStore::Allocate(const PayloadID&, uint64_t) {
  throw std::runtime_error("disk tier does not support direct allocation");
}

/*
  Read entire payload from disk.
*/
std::shared_ptr<arrow::Buffer>
DiskArrowStore::Read(const PayloadID& id) {

  auto path = PayloadPath(root_, id.value());

  auto file = Unwrap(arrow::io::ReadableFile::Open(path.string()));
  return ReadAll(file);
}

/*
  Atomic write:
      write tmp → flush → rename
*/
void DiskArrowStore::Write(const PayloadID& id,
                           const std::shared_ptr<arrow::Buffer>& buffer,
                           bool fsync) {

  auto final_path = PayloadPath(root_, id.value());
  auto tmp_path = final_path.string() + ".tmp";

  {
    auto out = Unwrap(arrow::io::FileOutputStream::Open(tmp_path));
    Unwrap(out->Write(buffer->data(), buffer->size()));

    if (fsync)
      Unwrap(out->Flush());

    Unwrap(out->Close());
  }

  std::filesystem::rename(tmp_path, final_path);
}

/*
  Remove payload from disk
*/
void DiskArrowStore::Remove(const PayloadID& id) {
  std::filesystem::remove(PayloadPath(root_, id.value()));
}

}


#include "disk_arrow_store.hpp"

#include <arrow/io/file.h>

#include <filesystem>

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
    std::memcpy(uuid.data(), id.value().data(), 16);
    return payload::util::ToString(uuid);
  }
  return id.value();
}

} // namespace

DiskArrowStore::DiskArrowStore(std::filesystem::path root) : root_(std::move(root)) {
  std::filesystem::create_directories(root_);
}

/*
  Disk tier cannot allocate writable buffers.
  Only RAM/GPU tiers allocate.
*/
std::shared_ptr<arrow::Buffer> DiskArrowStore::Allocate(const PayloadID&, uint64_t) {
  throw std::runtime_error("disk tier does not support direct allocation");
}

/*
  Read entire payload from disk.
*/
std::shared_ptr<arrow::Buffer> DiskArrowStore::Read(const PayloadID& id) {
  auto path = PayloadPath(root_, Key(id));

  auto file = Unwrap(arrow::io::ReadableFile::Open(path.string()));
  return ReadAll(file);
}

uint64_t DiskArrowStore::Size(const PayloadID& id) {
  return static_cast<uint64_t>(std::filesystem::file_size(PayloadPath(root_, Key(id))));
}

/*
  Atomic write:
      write tmp → flush → rename
*/
void DiskArrowStore::Write(const PayloadID& id, const std::shared_ptr<arrow::Buffer>& buffer, bool fsync) {
  auto final_path = PayloadPath(root_, Key(id));
  auto tmp_path   = final_path.string() + ".tmp";

  {
    auto out = Unwrap(arrow::io::FileOutputStream::Open(tmp_path));
    Unwrap(out->Write(buffer->data(), buffer->size()));

    if (fsync) Unwrap(out->Flush());

    Unwrap(out->Close());
  }

  try {
    std::filesystem::rename(tmp_path, final_path);
  } catch (...) {
    std::error_code ec;
    std::filesystem::remove(tmp_path, ec);
    throw;
  }
}

/*
  Remove payload from disk
*/
void DiskArrowStore::Remove(const PayloadID& id) {
  std::filesystem::remove(PayloadPath(root_, Key(id)));
}

} // namespace payload::storage

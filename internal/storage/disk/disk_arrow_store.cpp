#include "disk_arrow_store.hpp"

#include <arrow/io/file.h>
#include <fcntl.h>
#include <google/protobuf/util/json_util.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>

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

DiskArrowStore::DiskArrowStore(std::filesystem::path root) : root_(std::move(root)) {
  std::filesystem::create_directories(root_);
}

/*
  Pre-allocate the backing file so the client can mmap it writable.
  Uses open(O_CREAT|O_EXCL) + ftruncate to reserve space atomically.
  The return value is unused by PayloadManager::Allocate.
*/
std::shared_ptr<arrow::Buffer> DiskArrowStore::Allocate(const PayloadID& id, uint64_t size_bytes) {
  const auto path = PayloadPath(root_, Key(id));

  int fd = open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    throw std::runtime_error("disk allocate: open failed for " + path.string() + ": " + std::strerror(errno));
  }

  if (size_bytes > 0 && ftruncate(fd, static_cast<off_t>(size_bytes)) != 0) {
    const int saved = errno;
    close(fd);
    std::filesystem::remove(path);
    throw std::runtime_error("disk allocate: ftruncate failed for " + path.string() + ": " + std::strerror(saved));
  }

  close(fd);
  return nullptr;
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
  Remove payload from disk. Sidecar is cleaned up best-effort.
*/
void DiskArrowStore::Remove(const PayloadID& id) {
  std::filesystem::remove(PayloadPath(root_, Key(id)));
  std::error_code ec;
  std::filesystem::remove(SidecarPath(root_, Key(id)), ec);
}

/*
  Write <uuid>.meta.json alongside the data file.
  Uses write-to-tmp + atomic rename for crash safety.
*/
void DiskArrowStore::WriteSidecar(const PayloadID& id, const payload::manager::catalog::v1::PayloadArchiveMetadata& meta) {
  google::protobuf::util::JsonPrintOptions opts;
  opts.add_whitespace = true;

  std::string json;
  auto        status = google::protobuf::util::MessageToJsonString(meta, &json, opts);
  if (!status.ok()) {
    throw std::runtime_error("WriteSidecar: serialization failed for " + Key(id) + ": " + status.ToString());
  }

  const auto path = SidecarPath(root_, Key(id));
  const auto tmp  = path.string() + ".tmp";

  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out) {
      throw std::runtime_error("WriteSidecar: open failed for " + tmp + ": " + std::strerror(errno));
    }
    out.write(json.data(), static_cast<std::streamsize>(json.size()));
  }

  try {
    std::filesystem::rename(tmp, path);
  } catch (...) {
    std::error_code ec;
    std::filesystem::remove(tmp, ec);
    throw;
  }
}

} // namespace payload::storage

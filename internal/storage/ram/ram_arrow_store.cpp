#include "ram_arrow_store.hpp"

#include <arrow/buffer.h>
#include <arrow/memory_pool.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

#include "internal/util/uuid.hpp"
#include "payload/manager/v1.hpp"

namespace payload::storage {

using namespace payload::manager::v1;

// ---------------------------------------------------------------------------
// ShmBuffer — Arrow buffer backed by a POSIX shm mmap.
// Closes fd and munmaps on destruction; does NOT unlink (caller's job).
// ---------------------------------------------------------------------------
namespace {

class ShmBuffer final : public arrow::MutableBuffer {
 public:
  ShmBuffer(void* ptr, int64_t size, int fd) : arrow::MutableBuffer(static_cast<uint8_t*>(ptr), size), ptr_(ptr), size_(size), fd_(fd) {
  }

  ~ShmBuffer() override {
    if (ptr_ && ptr_ != MAP_FAILED) {
      munmap(ptr_, static_cast<size_t>(size_));
    }
    if (fd_ >= 0) {
      close(fd_);
    }
  }

 private:
  void*   ptr_;
  int64_t size_;
  int     fd_;
};

} // namespace

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/*static*/
std::string RamArrowStore::Key(const PayloadID& id) {
  if (id.value().size() == 16) {
    payload::util::UUID uuid{};
    std::memcpy(uuid.data(), id.value().data(), 16);
    return payload::util::ToString(uuid);
  }
  return id.value();
}

std::string RamArrowStore::ShmName(const PayloadID& id) const {
  return ShmName(id, shm_prefix_);
}

/*static*/
std::string RamArrowStore::ShmName(const PayloadID& id, const std::string& prefix) {
  return "/" + prefix + "-" + Key(id);
}

/*static*/
std::shared_ptr<arrow::Buffer> RamArrowStore::OpenShm(const std::string& name, size_t size_bytes, bool writable) {
  int fd = -1;

  if (writable) {
    fd = shm_open(name.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
      throw std::runtime_error("shm_open(create) failed for " + name + ": " + strerror(errno));
    }
    // fchmod overrides the process umask so cross-user access works.
    // Non-fatal if it fails (e.g. NFS-mounted tmpfs); access may be
    // restricted to the creating process only on those filesystems.
    if (fchmod(fd, 0666) != 0) {
      // Best-effort; segment remains usable by the owning process.
      (void)errno; // suppress unused-result warning on strict compilers
    }
    if (ftruncate(fd, static_cast<off_t>(size_bytes)) != 0) {
      const int saved = errno;
      close(fd);
      throw std::runtime_error("ftruncate failed for " + name + ": " + strerror(saved));
    }
  } else {
    fd = shm_open(name.c_str(), O_RDONLY, 0);
    if (fd < 0) {
      throw std::runtime_error("shm_open(read) failed for " + name + ": " + strerror(errno));
    }
    if (size_bytes == 0) {
      struct stat st {};
      if (fstat(fd, &st) == 0) {
        size_bytes = static_cast<size_t>(st.st_size);
      }
    }
  }

  if (size_bytes == 0) {
    close(fd);
    throw std::runtime_error("shm segment has zero size: " + name);
  }

  const int prot = writable ? (PROT_READ | PROT_WRITE) : PROT_READ;
  void*     ptr  = mmap(nullptr, size_bytes, prot, MAP_SHARED, fd, 0);
  if (ptr == MAP_FAILED) {
    const int saved = errno;
    close(fd);
    throw std::runtime_error("mmap failed for " + name + ": " + strerror(saved));
  }

  return std::make_shared<ShmBuffer>(ptr, static_cast<int64_t>(size_bytes), fd);
}

// ---------------------------------------------------------------------------
// StorageBackend implementation
// ---------------------------------------------------------------------------

/*
  Allocate: create the shm segment so the client can map and write into it.
  The server holds the mmap'd buffer so spill/promotion can read it.
*/
std::shared_ptr<arrow::Buffer> RamArrowStore::Allocate(const PayloadID& id, uint64_t size_bytes) {
  const auto name = ShmName(id);
  auto       buf  = OpenShm(name, size_bytes, /*writable=*/true);

  std::unique_lock lock(mutex_);
  buffers_[Key(id)] = buf;
  return buf;
}

/*
  Read: return the cached mmap buffer. If not cached (after a restart, the
  shm segment persists), re-open and cache it.
*/
std::shared_ptr<arrow::Buffer> RamArrowStore::Read(const PayloadID& id) {
  {
    std::shared_lock lock(mutex_);
    auto             it = buffers_.find(Key(id));
    if (it != buffers_.end()) {
      return it->second;
    }
  }

  // Re-open after restart
  const auto name = ShmName(id);
  auto       buf  = OpenShm(name, 0, /*writable=*/false);

  std::unique_lock lock(mutex_);
  buffers_[Key(id)] = buf;
  return buf;
}

/*
  Write: used for promotions (DISK → RAM). Create shm and copy data in.
*/
void RamArrowStore::Write(const PayloadID& id, const std::shared_ptr<arrow::Buffer>& buffer, bool /*fsync*/) {
  const auto name       = ShmName(id);
  const auto size_bytes = static_cast<size_t>(buffer->size());
  auto       buf        = OpenShm(name, size_bytes, /*writable=*/true);

  std::memcpy(reinterpret_cast<uint8_t*>(buf->mutable_data()), buffer->data(), size_bytes);

  std::unique_lock lock(mutex_);
  buffers_[Key(id)] = buf;
}

/*
  Remove: unlink the shm segment and drop the cached buffer.
  Existing client mappings continue to work until they munmap.
*/
void RamArrowStore::Remove(const PayloadID& id) {
  const auto name = ShmName(id);
  shm_unlink(name.c_str()); // best-effort; ignore error if already gone

  std::unique_lock lock(mutex_);
  buffers_.erase(Key(id));
}

} // namespace payload::storage

#include "internal/ring/ring.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>

#include "spdlog/spdlog.h"

namespace payload::ring {

namespace {

// Build "/pm-ring-<ring_id>-slot<idx>". The leading '/' is required
// by POSIX shm_open. Names live alongside RamArrowStore's
// "/pm-<uuid>" segments; the "-ring-" infix avoids collisions.
std::string SlotShmName(const std::string& shm_prefix, const std::string& ring_id, uint32_t idx) {
  return "/" + shm_prefix + "-ring-" + ring_id + "-slot" + std::to_string(idx);
}

// Open (or create) a writable POSIX shm region, ftruncate to
// `bytes`, mmap RW. Returns fd + mapped pointer. Throws on failure.
// Mirrors RamArrowStore::OpenShm's writable path but kept private to
// the ring module so we don't pull in arrow::Buffer just to allocate
// raw memory.
struct ShmRegion {
  int    fd    = -1;
  void*  ptr   = nullptr;
  size_t bytes = 0;
};
ShmRegion OpenWritableShm(const std::string& name, size_t bytes) {
  // Cleanly recreate. A stale segment from a prior crashed PM would
  // already have an ftruncated size; reusing it without unlink risks
  // mismatched sizes if the operator changed slot_size_bytes between
  // restarts.
  shm_unlink(name.c_str());

  int fd = shm_open(name.c_str(), O_CREAT | O_RDWR, 0666);
  if (fd < 0) {
    throw std::runtime_error("ring shm_open(create) failed for " + name + ": " + std::strerror(errno));
  }
  if (fchmod(fd, 0666) != 0) {
    // Best-effort; tmpfs may not honor fchmod (NFS-mounted tmpfs etc).
    (void)errno;
  }
  if (ftruncate(fd, static_cast<off_t>(bytes)) != 0) {
    const int saved = errno;
    close(fd);
    shm_unlink(name.c_str());
    throw std::runtime_error("ring ftruncate(" + std::to_string(bytes) + ") failed for " + name + ": " + std::strerror(saved));
  }
  void* ptr = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (ptr == MAP_FAILED) {
    const int saved = errno;
    close(fd);
    shm_unlink(name.c_str());
    throw std::runtime_error("ring mmap failed for " + name + ": " + std::strerror(saved));
  }
  return ShmRegion{fd, ptr, bytes};
}

} // namespace

Ring::Ring(Config cfg) : cfg_(std::move(cfg)) {
  if (cfg_.n_slots == 0) {
    throw std::invalid_argument("ring '" + cfg_.ring_id + "': n_slots must be > 0");
  }
  if (cfg_.slot_size_bytes == 0) {
    throw std::invalid_argument("ring '" + cfg_.ring_id + "': slot_size_bytes must be > 0");
  }
  if (cfg_.shm_prefix.empty()) {
    throw std::invalid_argument("ring '" + cfg_.ring_id + "': shm_prefix must be non-empty");
  }

  slots_.resize(cfg_.n_slots);
  for (uint32_t i = 0; i < cfg_.n_slots; ++i) {
    slots_[i].idx            = i;
    slots_[i].shm_name       = SlotShmName(cfg_.shm_prefix, cfg_.ring_id, i);
    slots_[i].capacity_bytes = cfg_.slot_size_bytes;
    slots_[i].status         = SlotStatus::kAvailable;

    try {
      auto region       = OpenWritableShm(slots_[i].shm_name, cfg_.slot_size_bytes);
      slots_[i].fd      = region.fd;
      slots_[i].host_va = region.ptr;
    } catch (...) {
      // Tear down what we got before bubbling. Otherwise a half-built
      // ring leaks fds + /dev/shm segments at the next PM start.
      for (uint32_t j = 0; j < i; ++j) {
        if (slots_[j].host_va) munmap(slots_[j].host_va, slots_[j].capacity_bytes);
        if (slots_[j].fd >= 0) close(slots_[j].fd);
        shm_unlink(slots_[j].shm_name.c_str());
      }
      throw;
    }
  }
  spdlog::info("payload-manager: ring '{}' created with {} slots of {} bytes each ({} total)", cfg_.ring_id, cfg_.n_slots, cfg_.slot_size_bytes,
               static_cast<uint64_t>(cfg_.n_slots) * cfg_.slot_size_bytes);
}

Ring::~Ring() {
  for (auto& s : slots_) {
    if (s.host_va) munmap(s.host_va, s.capacity_bytes);
    if (s.fd >= 0) close(s.fd);
    shm_unlink(s.shm_name.c_str());
  }
}

std::vector<std::string> Ring::SlotShmNames() const {
  std::vector<std::string> out;
  out.reserve(slots_.size());
  for (const auto& s : slots_) out.push_back(s.shm_name);
  return out;
}

Slot* Ring::PickAcquireSlot_() {
  // Round-robin scan starting at acquire_cursor_. A slot is eligible if:
  //   - status != WRITING (someone else has it open for write), AND
  //   - refcount == 0  (no consumer is still reading the prior gen)
  // The READABLE-but-refcount-0 case is the common steady-state: previous
  // consumers already released, slot is ready for a fresh write.
  const uint32_t n = cfg_.n_slots;
  for (uint32_t off = 0; off < n; ++off) {
    const uint32_t i = (acquire_cursor_ + off) % n;
    Slot&          s = slots_[i];
    if (s.status == SlotStatus::kWriting) continue;
    if (s.refcount > 0) continue;
    acquire_cursor_ = (i + 1) % n;
    return &s;
  }
  return nullptr;
}

Slot* Ring::ReclaimStaleWritingSlot_(std::chrono::steady_clock::time_point now) {
  // Only reached when PickAcquireSlot_ came up empty. A slot still
  // WRITING past the timeout means its producer never came back —
  // crashed, or was killed between AcquireRingSlot and CommitRingSlot.
  // Nothing else will ever clear that state, so the choice is to take
  // the slot back or to lose it permanently.
  //
  // Take the *oldest* such slot: it is the one least likely to belong to
  // a producer that is merely slow, and picking deterministically keeps
  // repeated reclaims from thrashing across slots.
  Slot* oldest = nullptr;
  for (auto& s : slots_) {
    if (s.status != SlotStatus::kWriting) continue;
    if (s.refcount > 0) continue; // can't happen: WRITING is not leasable. Cheap to assert by skipping.
    if (now - s.acquired_at < cfg_.slot_write_timeout) continue;
    if (!oldest || s.acquired_at < oldest->acquired_at) oldest = &s;
  }
  if (!oldest) return nullptr;

  const auto held = std::chrono::duration_cast<std::chrono::milliseconds>(now - oldest->acquired_at);
  spdlog::warn(
      "payload-manager: ring '{}' reclaiming slot {} (gen {}) held WRITING for {} ms, past the {} ms write "
      "timeout — assuming the producer died; its late CommitRingSlot will be rejected on generation",
      cfg_.ring_id, oldest->idx, oldest->generation, held.count(), cfg_.slot_write_timeout.count());
  reclaimed_writing_ += 1;
  // Caller bumps the generation, which is what makes the zombie
  // producer's eventual Commit fail its generation check rather than
  // publishing bytes over the next producer's capture.
  acquire_cursor_ = (oldest->idx + 1) % cfg_.n_slots;
  return oldest;
}

std::optional<AcquireResult> Ring::Acquire() {
  const auto                  now = std::chrono::steady_clock::now();
  std::lock_guard<std::mutex> lk(mu_);
  Slot*                       slot = PickAcquireSlot_();
  if (!slot) slot = ReclaimStaleWritingSlot_(now);
  if (!slot) {
    // DROP_NEW: caller surfaces RESOURCE_EXHAUSTED. BLOCK is v2.
    return std::nullopt;
  }
  // Bump generation, transition to WRITING. Producer fills the shm
  // region via its own mmap of slot->shm_name (PM never copies bytes
  // through this path).
  slot->generation += 1;
  slot->status      = SlotStatus::kWriting;
  slot->size_bytes  = 0;
  slot->acquired_at = now;
  return AcquireResult{
      .slot_idx       = slot->idx,
      .generation     = slot->generation,
      .shm_name       = slot->shm_name,
      .capacity_bytes = slot->capacity_bytes,
  };
}

bool Ring::Commit(uint32_t slot_idx, uint64_t generation, uint64_t size_bytes) {
  std::lock_guard<std::mutex> lk(mu_);
  if (slot_idx >= slots_.size()) return false;
  Slot& s = slots_[slot_idx];
  // Reject mismatched handles. Either the producer is double-committing
  // (status already kReadable) or the generation drifted (impossible
  // under the current single-Acquire-per-cycle protocol, but cheap to
  // enforce).
  if (s.status != SlotStatus::kWriting) return false;
  if (s.generation != generation) return false;
  if (size_bytes > s.capacity_bytes) return false;
  s.status       = SlotStatus::kReadable;
  s.size_bytes   = size_bytes;
  s.committed_at = std::chrono::steady_clock::now();
  return true;
}

std::optional<LeaseGrant> Ring::Lease(uint32_t slot_idx, uint64_t generation) {
  std::lock_guard<std::mutex> lk(mu_);
  if (slot_idx >= slots_.size()) return std::nullopt;
  Slot& s = slots_[slot_idx];
  // Generation mismatch -> consumer was slow; producer already
  // recycled this slot. Caller surfaces FAILED_PRECONDITION.
  if (s.status != SlotStatus::kReadable) return std::nullopt;
  if (s.generation != generation) return std::nullopt;
  s.refcount += 1;
  return LeaseGrant{.slot_idx = slot_idx, .generation = generation};
}

void Ring::Release(uint32_t slot_idx, uint64_t generation) {
  std::lock_guard<std::mutex> lk(mu_);
  if (slot_idx >= slots_.size()) return;
  Slot& s = slots_[slot_idx];
  // Silently no-op on a stale release (the lease was for a generation
  // that's since been recycled and presumably all current-gen consumers
  // already released). Without this guard a double-release on a stale
  // handle could underflow the refcount of the freshly-acquired slot.
  if (s.generation != generation) return;
  if (s.refcount > 0) s.refcount -= 1;
  // (When refcount hits 0 we don't change status: stays kReadable so
  // a re-issued event for the same gen could in principle re-Lease.
  // The slot becomes Acquire-eligible because PickAcquireSlot_ filters
  // on refcount==0 regardless of status.)
}

Ring::Stats Ring::GetStats() const {
  std::lock_guard<std::mutex> lk(mu_);
  Stats                       out;
  out.slots_total         = cfg_.n_slots;
  out.slots_reclaimed     = reclaimed_writing_;
  out.slot_capacity_bytes = cfg_.slot_size_bytes;
  for (const auto& s : slots_) {
    if (s.status == SlotStatus::kWriting) ++out.slots_writing;
    if (s.refcount > 0) {
      ++out.slots_leased;
      out.leases_active += s.refcount;
    }
    // Mirrors PickAcquireSlot_ exactly: anything it would hand out is
    // headroom. Keep the two in step if that predicate ever changes.
    if (s.status != SlotStatus::kWriting && s.refcount == 0) ++out.slots_available;
  }
  return out;
}

uint64_t Ring::reclaimed_writing_slots() const {
  std::lock_guard<std::mutex> lk(mu_);
  return reclaimed_writing_;
}

std::vector<Ring::SlotSnapshot> Ring::Snapshot() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<SlotSnapshot>   out;
  out.reserve(slots_.size());
  for (const auto& s : slots_) {
    out.push_back({.slot_idx = s.idx, .status = s.status, .generation = s.generation, .refcount = s.refcount, .size_bytes = s.size_bytes});
  }
  return out;
}

} // namespace payload::ring

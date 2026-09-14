#include "client/cpp/ring.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <utility>

#if PAYLOAD_CLIENT_ARROW_CUDA
#include <cuda_runtime.h>
#endif

#include "spdlog/spdlog.h"

namespace payload::manager::client {

namespace core_v1 = payload::manager::core::v1;

// ============================================================================
// Consumer
// ============================================================================

RingConsumer::RingConsumer(PayloadClient* pm_client, Options opts) : pm_client_(pm_client), opts_(std::move(opts)) {}

RingConsumer::~RingConsumer() {
  for (auto& [ring_id, mapping] : rings_) TearDownRing_(mapping);
}

void RingConsumer::TearDownRing_(RingMapping& r) const {
  for (auto& s : r.slots) {
#if PAYLOAD_CLIENT_ARROW_CUDA
    // dev_va set ⇒ host_va was registered. Unregister before munmap.
    if (s.dev_va) cudaHostUnregister(s.host_va);
#endif
    if (s.host_va) munmap(s.host_va, s.capacity);
    if (s.fd >= 0) close(s.fd);
  }
  r.slots.clear();
}

bool RingConsumer::BuildMapping_(const std::string& ring_id, RingMapping& mapping) {
  auto resp = pm_client_->MapRing(ring_id);
  if (!resp.ok()) {
    spdlog::error("{}: MapRing('{}') failed: {}", opts_.log_prefix, ring_id, resp.status().ToString());
    return false;
  }
  const auto& m = resp.ValueOrDie();

  mapping.n_slots = m.n_slots();
  mapping.slot_capacity = m.slot_capacity_bytes();
  mapping.slots.resize(m.n_slots());

  for (int i = 0; i < m.slot_shm_names_size(); ++i) {
    auto& slot = mapping.slots[i];
    const std::string& name = m.slot_shm_names(i);

    // GPU consumers must map the slot writable: L4T R36's cudaHostRegister
    // rejects PROT_READ-only mappings with `invalid argument`. The consumer
    // never writes — the writable mapping is purely to satisfy registration.
    const int open_flags = opts_.register_for_gpu ? O_RDWR : O_RDONLY;
    const int prot = opts_.register_for_gpu ? (PROT_READ | PROT_WRITE) : PROT_READ;

    int fd = shm_open(name.c_str(), open_flags, 0);
    if (fd < 0) {
      spdlog::error("{}: shm_open('{}') failed: {} — tearing down ring '{}'", opts_.log_prefix, name,
                    std::strerror(errno), ring_id);
      TearDownRing_(mapping);
      return false;
    }
    void* va = mmap(nullptr, m.slot_capacity_bytes(), prot, MAP_SHARED, fd, 0);
    if (va == MAP_FAILED) {
      spdlog::error("{}: mmap('{}', {} bytes) failed: {} — tearing down ring '{}'", opts_.log_prefix, name,
                    m.slot_capacity_bytes(), std::strerror(errno), ring_id);
      close(fd);
      TearDownRing_(mapping);
      return false;
    }
    slot.host_va = va;
    slot.capacity = m.slot_capacity_bytes();
    slot.fd = fd;

#if PAYLOAD_CLIENT_ARROW_CUDA
    // Register the slot for the GPU ONCE here. Slots are stable and
    // reused, so this is a 100%-hit cache — versus the per-capture UUID
    // path where every payload had a fresh VA and registration could
    // never be amortized.
    if (opts_.register_for_gpu) {
      cudaError_t reg = cudaHostRegister(va, m.slot_capacity_bytes(), cudaHostRegisterMapped);
      if (reg != cudaSuccess) {
        spdlog::error("{}: cudaHostRegister(slot '{}', {} bytes) failed: {} — tearing down ring '{}'", opts_.log_prefix,
                      name, m.slot_capacity_bytes(), cudaGetErrorString(reg), ring_id);
        TearDownRing_(mapping);
        return false;
      }
      void* dev = nullptr;
      cudaError_t gp = cudaHostGetDevicePointer(&dev, va, 0);
      if (gp != cudaSuccess) {
        spdlog::error("{}: cudaHostGetDevicePointer(slot '{}') failed: {} — tearing down ring '{}'", opts_.log_prefix,
                      name, cudaGetErrorString(gp), ring_id);
        TearDownRing_(mapping);
        return false;
      }
      slot.dev_va = dev;
    }
#endif
  }

  spdlog::info("{}: mapped ring '{}' — {} slots × {} bytes = {} total bytes{}", opts_.log_prefix, ring_id,
               mapping.n_slots, mapping.slot_capacity, static_cast<uint64_t>(mapping.n_slots) * mapping.slot_capacity,
               opts_.register_for_gpu ? " (GPU-registered)" : "");
  return true;
}

bool RingConsumer::EnsureMapped(const std::string& ring_id) {
  if (!pm_client_) {
    spdlog::warn("{}: EnsureMapped('{}') but PM client is null", opts_.log_prefix, ring_id);
    return false;
  }
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (rings_.contains(ring_id)) return true;  // fast path
  }

  // Build outside the lock: MapRing is an RPC and the per-slot mmap +
  // cudaHostRegister of a large ring is slow enough that holding the
  // mutex across it would stall every other ring's lease path.
  RingMapping mapping;
  if (!BuildMapping_(ring_id, mapping)) return false;

  std::lock_guard<std::mutex> lk(mu_);
  // Another thread may have mapped the same ring while we worked. Keep
  // the published mapping and drop ours rather than orphaning its
  // pointers, which callers may already hold.
  // try_emplace leaves `mapping` untouched when the key already exists,
  // so the loser is still ours to tear down.
  if (!rings_.try_emplace(ring_id, std::move(mapping)).second) {
    RingMapping loser = std::move(mapping);
    TearDownRing_(loser);
  }
  return true;
}

std::optional<RingConsumer::Lease> RingConsumer::LeaseAndOpen(const std::string& ring_id, uint32_t slot_idx,
                                                              uint64_t generation, uint64_t size_bytes) {
  if (!pm_client_) return std::nullopt;
  // Lazy MapRing on first encounter; a no-op afterwards.
  if (!EnsureMapped(ring_id)) return std::nullopt;

  // Resolve the cached pointers under the lock, then drop it — the
  // LeaseRingSlot RPC below must not serialize concurrent consumers.
  // The pointers stay valid because entries are never erased or
  // resized once published.
  const void* host_va = nullptr;
  const void* dev_va = nullptr;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = rings_.find(ring_id);
    if (it == rings_.end()) return std::nullopt;  // EnsureMapped tore it down
    auto& mapping = it->second;
    if (slot_idx >= mapping.slots.size()) {
      spdlog::warn("{}: slot_idx {} OOB for ring '{}' (n_slots={})", opts_.log_prefix, slot_idx, ring_id,
                   mapping.n_slots);
      return std::nullopt;
    }
    if (size_bytes > mapping.slot_capacity) {
      spdlog::warn("{}: event claims size_bytes={} but ring '{}' slot capacity is {} — capping at capacity",
                   opts_.log_prefix, size_bytes, ring_id, mapping.slot_capacity);
      size_bytes = mapping.slot_capacity;
    }
    host_va = mapping.slots[slot_idx].host_va;
    dev_va = mapping.slots[slot_idx].dev_va;
  }

  auto resp = pm_client_->LeaseRingSlot(ring_id, slot_idx, generation);
  if (!resp.ok()) {
    // FAILED_PRECONDITION (stale generation) is the expected outcome
    // when the consumer was too slow and the producer recycled the
    // slot. Drop the capture; the next event will be fresh.
    spdlog::debug("{}: LeaseRingSlot('{}', slot={}, gen={}) refused: {}", opts_.log_prefix, ring_id, slot_idx,
                  generation, resp.status().ToString());
    return std::nullopt;
  }

  return Lease{
      .host_va = host_va,
      .dev_va = dev_va,
      .size_bytes = size_bytes,
      .lease_id = resp.ValueOrDie().lease_id(),
  };
}

void RingConsumer::Release(const std::string& lease_id) {
  if (!pm_client_ || lease_id.empty()) return;
  auto status = pm_client_->ReleaseRingSlot(lease_id);
  if (!status.ok()) {
    // PM treats unknown lease_ids as OK, so the only path here is a
    // gRPC transport failure. Non-fatal.
    spdlog::debug("{}: ReleaseRingSlot failed: {}", opts_.log_prefix, status.ToString());
  }
}

// ============================================================================
// Producer
// ============================================================================

RingProducerSlot::~RingProducerSlot() {
  if (mmap_va) munmap(mmap_va, capacity);
  if (mmap_fd >= 0) close(mmap_fd);
  // No shm_unlink — PM owns the slot lifetime. We only release our
  // process's view.
}

RingProducerSlot::RingProducerSlot(RingProducerSlot&& other) noexcept
    : ring_id(std::move(other.ring_id)),
      slot_idx(other.slot_idx),
      generation(other.generation),
      capacity(other.capacity),
      offset(other.offset),
      mmap_va(other.mmap_va),
      mmap_fd(other.mmap_fd),
      log_prefix(std::move(other.log_prefix)) {
  other.mmap_va = nullptr;
  other.mmap_fd = -1;
  other.capacity = 0;
}

RingProducerSlot& RingProducerSlot::operator=(RingProducerSlot&& other) noexcept {
  if (this != &other) {
    if (mmap_va) munmap(mmap_va, capacity);
    if (mmap_fd >= 0) close(mmap_fd);
    ring_id = std::move(other.ring_id);
    slot_idx = other.slot_idx;
    generation = other.generation;
    capacity = other.capacity;
    offset = other.offset;
    mmap_va = other.mmap_va;
    mmap_fd = other.mmap_fd;
    log_prefix = std::move(other.log_prefix);
    other.mmap_va = nullptr;
    other.mmap_fd = -1;
    other.capacity = 0;
  }
  return *this;
}

std::size_t RingProducerSlot::Append(const void* src, std::size_t src_bytes) {
  if (!valid() || src_bytes == 0) return 0;
  const std::size_t room = (offset >= capacity) ? 0 : static_cast<std::size_t>(capacity - offset);
  const std::size_t to_copy = std::min(src_bytes, room);
  if (to_copy < src_bytes) {
    spdlog::warn("{}: ring slot write truncated — offset={} capacity={} src={} writing={}", log_prefix, offset,
                 capacity, src_bytes, to_copy);
  }
  std::memcpy(static_cast<std::uint8_t*>(mmap_va) + offset, src, to_copy);
  offset += to_copy;
  return to_copy;
}

RingProducerSlot AcquireRingProducerSlot(PayloadClient& client, const std::string& ring_id, std::string log_prefix) {
  RingProducerSlot s;
  s.log_prefix = std::move(log_prefix);

  auto resp = client.AcquireRingSlot(ring_id);
  if (!resp.ok()) {
    // RESOURCE_EXHAUSTED (every slot leased) maps to
    // arrow::Status::CapacityError; callers distinguish via valid().
    spdlog::warn("{}: AcquireRingSlot('{}') failed: {}", s.log_prefix, ring_id, resp.status().ToString());
    return s;
  }
  const auto& acq = resp.ValueOrDie();

  int fd = shm_open(acq.shm_name().c_str(), O_RDWR, 0);
  if (fd < 0) {
    spdlog::error("{}: ring slot shm_open('{}') failed: {} — committing empty to release the slot", s.log_prefix,
                  acq.shm_name(), std::strerror(errno));
    // Don't leak the slot: commit size 0 so PM marks it Readable and
    // consumers see an empty payload (and skip it).
    (void)client.CommitRingSlot(ring_id, acq.slot_idx(), acq.generation(), 0);
    return s;
  }
  void* va = mmap(nullptr, acq.capacity_bytes(), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (va == MAP_FAILED) {
    spdlog::error("{}: ring slot mmap('{}', {} bytes) failed: {} — committing empty to release", s.log_prefix,
                  acq.shm_name(), acq.capacity_bytes(), std::strerror(errno));
    close(fd);
    (void)client.CommitRingSlot(ring_id, acq.slot_idx(), acq.generation(), 0);
    return s;
  }

  s.ring_id = ring_id;
  s.slot_idx = acq.slot_idx();
  s.generation = acq.generation();
  s.capacity = acq.capacity_bytes();
  s.offset = 0;
  s.mmap_va = va;
  s.mmap_fd = fd;
  return s;
}

bool CommitRingProducerSlot(PayloadClient& client, RingProducerSlot& s, core_v1::RingSlotRef* out_ref) {
  if (!s.valid()) return false;
  auto status = client.CommitRingSlot(s.ring_id, s.slot_idx, s.generation, s.offset);
  if (!status.ok()) {
    spdlog::error("{}: CommitRingSlot failed: {}", s.log_prefix, status.ToString());
    return false;
  }
  if (out_ref) {
    out_ref->set_ring_id(s.ring_id);
    out_ref->set_slot_idx(s.slot_idx);
    out_ref->set_generation(s.generation);
    out_ref->set_size_bytes(s.offset);
  }
  return true;
}

}  // namespace payload::manager::client

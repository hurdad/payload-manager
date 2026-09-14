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

RingConsumer::RingConsumer(PayloadClient* pm_client, Options opts) : pm_client_(pm_client), opts_(std::move(opts)) {
#if !PAYLOAD_CLIENT_ARROW_CUDA
  if (opts_.register_for_gpu) {
    spdlog::warn(
        "{}: register_for_gpu requested but the client was built without "
        "PAYLOAD_MANAGER_CLIENT_ENABLE_CUDA — slots will be mapped read-only and dev_va will be null",
        opts_.log_prefix);
  }
#endif
}

RingConsumer::~RingConsumer() {
  for (auto& [ring_id, mapping] : rings_) TearDownRing_(mapping);
}

// ---------------------------------------------------------------------------
// RingConsumer::Lease
// ---------------------------------------------------------------------------

RingConsumer::Lease::~Lease() {
  Release();
}

RingConsumer::Lease::Lease(Lease&& other) noexcept
    : host_va(other.host_va), dev_va(other.dev_va), size_bytes(other.size_bytes), owner_(other.owner_), lease_id_(std::move(other.lease_id_)) {
  // Neutralize the source: exactly one of the two must release.
  other.owner_ = nullptr;
  other.lease_id_.clear();
  other.host_va    = nullptr;
  other.dev_va     = nullptr;
  other.size_bytes = 0;
}

RingConsumer::Lease& RingConsumer::Lease::operator=(Lease&& other) noexcept {
  if (this != &other) {
    Release(); // drop whatever we were holding first
    host_va      = other.host_va;
    dev_va       = other.dev_va;
    size_bytes   = other.size_bytes;
    owner_       = other.owner_;
    lease_id_    = std::move(other.lease_id_);
    other.owner_ = nullptr;
    other.lease_id_.clear();
    other.host_va    = nullptr;
    other.dev_va     = nullptr;
    other.size_bytes = 0;
  }
  return *this;
}

void RingConsumer::Lease::Release() {
  if (!owner_ || lease_id_.empty()) return;
  // Clear before the RPC so a throw (or a re-entrant call) can't release twice.
  RingConsumer*     owner = std::exchange(owner_, nullptr);
  const std::string id    = std::exchange(lease_id_, std::string{});
  owner->Release(id);
}

// ---------------------------------------------------------------------------
// Mapping
// ---------------------------------------------------------------------------

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

  // Validate the response before sizing anything off it. n_slots and
  // slot_shm_names are independent wire fields; trusting them to agree
  // would mean indexing a vector sized by one with a bound taken from
  // the other.
  if (m.n_slots() == 0 || m.slot_capacity_bytes() == 0) {
    spdlog::error("{}: MapRing('{}') returned a degenerate ring: n_slots={} slot_capacity_bytes={}", opts_.log_prefix, ring_id, m.n_slots(),
                  m.slot_capacity_bytes());
    return false;
  }
  if (m.slot_shm_names_size() != static_cast<int>(m.n_slots())) {
    spdlog::error("{}: MapRing('{}') returned {} shm names for {} slots — refusing to map", opts_.log_prefix, ring_id, m.slot_shm_names_size(),
                  m.n_slots());
    return false;
  }

  // A non-CUDA build has nothing to register, so it keeps the
  // kernel-enforced read-only mapping rather than opening the producer's
  // live slot for writing to no purpose.
#if PAYLOAD_CLIENT_ARROW_CUDA
  const bool want_gpu = opts_.register_for_gpu;
#else
  const bool want_gpu = false;
#endif

  mapping.n_slots       = m.n_slots();
  mapping.slot_capacity = m.slot_capacity_bytes();
  mapping.slots.resize(m.n_slots());

  for (int i = 0; i < m.slot_shm_names_size(); ++i) {
    auto&              slot = mapping.slots[i];
    const std::string& name = m.slot_shm_names(i);

    // GPU consumers must map the slot writable: L4T R36's cudaHostRegister
    // rejects PROT_READ-only mappings with `invalid argument`. The consumer
    // never writes — the writable mapping is purely to satisfy registration.
    const int open_flags = want_gpu ? O_RDWR : O_RDONLY;
    const int prot       = want_gpu ? (PROT_READ | PROT_WRITE) : PROT_READ;

    int fd = shm_open(name.c_str(), open_flags, 0);
    if (fd < 0) {
      spdlog::error("{}: shm_open('{}') failed: {} — tearing down ring '{}'", opts_.log_prefix, name, std::strerror(errno), ring_id);
      TearDownRing_(mapping);
      return false;
    }
    void* va = mmap(nullptr, m.slot_capacity_bytes(), prot, MAP_SHARED, fd, 0);
    if (va == MAP_FAILED) {
      spdlog::error("{}: mmap('{}', {} bytes) failed: {} — tearing down ring '{}'", opts_.log_prefix, name, m.slot_capacity_bytes(),
                    std::strerror(errno), ring_id);
      close(fd);
      TearDownRing_(mapping);
      return false;
    }
    slot.host_va  = va;
    slot.capacity = m.slot_capacity_bytes();
    slot.fd       = fd;

#if PAYLOAD_CLIENT_ARROW_CUDA
    // Register the slot for the GPU ONCE here. Slots are stable and
    // reused, so this is a 100%-hit cache — versus the per-capture UUID
    // path where every payload had a fresh VA and registration could
    // never be amortized.
    if (want_gpu) {
      cudaError_t reg = cudaHostRegister(va, m.slot_capacity_bytes(), cudaHostRegisterMapped);
      if (reg != cudaSuccess) {
        spdlog::error("{}: cudaHostRegister(slot '{}', {} bytes) failed: {} — tearing down ring '{}'", opts_.log_prefix, name,
                      m.slot_capacity_bytes(), cudaGetErrorString(reg), ring_id);
        TearDownRing_(mapping);
        return false;
      }
      void*       dev = nullptr;
      cudaError_t gp  = cudaHostGetDevicePointer(&dev, va, 0);
      if (gp != cudaSuccess) {
        spdlog::error("{}: cudaHostGetDevicePointer(slot '{}') failed: {} — tearing down ring '{}'", opts_.log_prefix, name, cudaGetErrorString(gp),
                      ring_id);
        TearDownRing_(mapping);
        return false;
      }
      slot.dev_va = dev;
    }
#endif
  }

  spdlog::info("{}: mapped ring '{}' — {} slots × {} bytes = {} total bytes{}", opts_.log_prefix, ring_id, mapping.n_slots, mapping.slot_capacity,
               static_cast<uint64_t>(mapping.n_slots) * mapping.slot_capacity, want_gpu ? " (GPU-registered)" : "");
  return true;
}

std::shared_ptr<std::mutex> RingConsumer::BuildMutexFor_(const std::string& ring_id) {
  auto it = build_mus_.find(ring_id);
  if (it != build_mus_.end()) return it->second;
  return build_mus_.emplace(ring_id, std::make_shared<std::mutex>()).first->second;
}

bool RingConsumer::EnsureMapped(const std::string& ring_id) {
  if (!pm_client_) {
    spdlog::warn("{}: EnsureMapped('{}') but PM client is null", opts_.log_prefix, ring_id);
    return false;
  }

  std::shared_ptr<std::mutex> build_mu;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (rings_.contains(ring_id)) return true; // fast path
    build_mu = BuildMutexFor_(ring_id);
  }

  // Build outside mu_: MapRing is an RPC and the per-slot mmap +
  // cudaHostRegister of a large ring is slow enough that holding the
  // cache mutex across it would stall every other ring's lease path.
  // The per-ring build mutex instead serializes only threads racing on
  // the *same* ring's first event, so they don't each issue a MapRing
  // and each cudaHostRegister the same shm objects.
  std::lock_guard<std::mutex> build_lk(*build_mu);
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (rings_.contains(ring_id)) return true; // a previous holder published it
  }

  RingMapping mapping;
  if (!BuildMapping_(ring_id, mapping)) return false;

  std::lock_guard<std::mutex> lk(mu_);
  // build_mu makes us the only builder for this ring_id, so try_emplace
  // should always win. Keep the losing branch anyway: publishing the
  // winner and tearing down our copy is the only safe outcome if that
  // invariant ever changes, since callers may already hold the winner's
  // pointers.
  if (!rings_.try_emplace(ring_id, std::move(mapping)).second) {
    RingMapping loser = std::move(mapping);
    TearDownRing_(loser);
  }
  return true;
}

std::optional<RingConsumer::Lease> RingConsumer::LeaseAndOpen(const std::string& ring_id, uint32_t slot_idx, uint64_t generation,
                                                              uint64_t size_bytes) {
  if (!pm_client_) return std::nullopt;
  // Lazy MapRing on first encounter; a no-op afterwards.
  if (!EnsureMapped(ring_id)) return std::nullopt;

  // Resolve the cached pointers under the lock, then drop it — the
  // LeaseRingSlot RPC below must not serialize concurrent consumers.
  // The pointers stay valid because entries are never erased or
  // resized once published.
  const void* host_va = nullptr;
  const void* dev_va  = nullptr;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto                        it = rings_.find(ring_id);
    if (it == rings_.end()) return std::nullopt; // EnsureMapped tore it down
    auto& mapping = it->second;
    if (slot_idx >= mapping.slots.size()) {
      spdlog::warn("{}: slot_idx {} OOB for ring '{}' (n_slots={})", opts_.log_prefix, slot_idx, ring_id, mapping.n_slots);
      return std::nullopt;
    }
    if (size_bytes > mapping.slot_capacity) {
      spdlog::warn("{}: event claims size_bytes={} but ring '{}' slot capacity is {} — capping at capacity", opts_.log_prefix, size_bytes, ring_id,
                   mapping.slot_capacity);
      size_bytes = mapping.slot_capacity;
    }
    host_va = mapping.slots[slot_idx].host_va;
    dev_va  = mapping.slots[slot_idx].dev_va;
  }

  // Belt and braces against a half-built mapping: BuildMapping_ either
  // maps every slot or publishes nothing, so a null here means the
  // invariant broke and handing the pointer back would segfault the
  // caller instead of dropping one capture.
  if (host_va == nullptr) {
    spdlog::error("{}: ring '{}' slot {} has no host mapping — dropping capture", opts_.log_prefix, ring_id, slot_idx);
    return std::nullopt;
  }

  auto resp = pm_client_->LeaseRingSlot(ring_id, slot_idx, generation);
  if (!resp.ok()) {
    // A stale generation (the consumer was too slow and the producer
    // recycled the slot) arrives as Invalid and is the expected outcome
    // at capture rate, so it stays at debug. Anything else is a real
    // failure and must not hide there.
    if (resp.status().IsInvalid()) {
      spdlog::debug("{}: LeaseRingSlot('{}', slot={}, gen={}) refused as stale: {}", opts_.log_prefix, ring_id, slot_idx, generation,
                    resp.status().ToString());
    } else {
      spdlog::error("{}: LeaseRingSlot('{}', slot={}, gen={}) failed: {}", opts_.log_prefix, ring_id, slot_idx, generation, resp.status().ToString());
    }
    return std::nullopt;
  }

  Lease lease;
  lease.host_va    = host_va;
  lease.dev_va     = dev_va;
  lease.size_bytes = size_bytes;
  lease.owner_     = this;
  lease.lease_id_  = resp.ValueOrDie().lease_id();
  return lease;
}

void RingConsumer::Release(const std::string& lease_id) {
  if (!pm_client_ || lease_id.empty()) return;
  auto status = pm_client_->ReleaseRingSlot(lease_id);
  if (!status.ok()) {
    // PM treats unknown lease_ids as OK, so the only path here is a
    // gRPC transport failure — which means the slot's refcount is still
    // up and PM will never reclaim it on its own. Worth an error.
    spdlog::error("{}: ReleaseRingSlot failed, slot stays pinned server-side: {}", opts_.log_prefix, status.ToString());
  }
}

// ============================================================================
// Producer
// ============================================================================

RingProducerSlot::~RingProducerSlot() {
  // Rescue commit. PM leaves an acquired slot in WRITING forever and has
  // no reaper, so returning early between acquire and commit would
  // retire this slot for the life of the PM process. Commit zero rather
  // than `offset`: the caller never committed, so whatever is in the
  // slot is a partial capture nobody announced, and consumers skip an
  // empty payload.
  if (client && !committed && valid()) {
    spdlog::warn("{}: ring '{}' slot {} (gen {}) destroyed uncommitted — committing empty so PM can recycle it", log_prefix, ring_id, slot_idx,
                 generation);
    auto status = client->CommitRingSlot(ring_id, slot_idx, generation, 0);
    if (!status.ok()) {
      spdlog::error("{}: rescue CommitRingSlot for ring '{}' slot {} failed, slot is now stuck: {}", log_prefix, ring_id, slot_idx,
                    status.ToString());
    }
  }
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
      log_prefix(std::move(other.log_prefix)),
      client(other.client),
      committed(other.committed) {
  other.mmap_va  = nullptr;
  other.mmap_fd  = -1;
  other.capacity = 0;
  // Neutralize the source's rescue commit: exactly one of the two
  // handles owns this reservation, and it is now us.
  other.client    = nullptr;
  other.committed = true;
}

RingProducerSlot& RingProducerSlot::operator=(RingProducerSlot&& other) noexcept {
  if (this != &other) {
    // Our own reservation goes away here, so it needs the same rescue
    // commit the destructor does.
    if (client && !committed && valid()) {
      spdlog::warn("{}: ring '{}' slot {} (gen {}) overwritten uncommitted — committing empty so PM can recycle it", log_prefix, ring_id, slot_idx,
                   generation);
      auto status = client->CommitRingSlot(ring_id, slot_idx, generation, 0);
      if (!status.ok()) {
        spdlog::error("{}: rescue CommitRingSlot for ring '{}' slot {} failed, slot is now stuck: {}", log_prefix, ring_id, slot_idx,
                      status.ToString());
      }
    }
    if (mmap_va) munmap(mmap_va, capacity);
    if (mmap_fd >= 0) close(mmap_fd);
    ring_id         = std::move(other.ring_id);
    slot_idx        = other.slot_idx;
    generation      = other.generation;
    capacity        = other.capacity;
    offset          = other.offset;
    mmap_va         = other.mmap_va;
    mmap_fd         = other.mmap_fd;
    log_prefix      = std::move(other.log_prefix);
    client          = other.client;
    committed       = other.committed;
    other.mmap_va   = nullptr;
    other.mmap_fd   = -1;
    other.capacity  = 0;
    other.client    = nullptr;
    other.committed = true;
  }
  return *this;
}

std::size_t RingProducerSlot::Append(const void* src, std::size_t src_bytes) {
  if (!valid() || src_bytes == 0) return 0;
  if (committed) {
    // PM has published this slot; consumers may be reading it right now.
    // Writing here would be a silent data race, not an append.
    spdlog::warn("{}: Append of {} bytes to ring '{}' slot {} after commit — ignored", log_prefix, src_bytes, ring_id, slot_idx);
    return 0;
  }
  const std::size_t room    = (offset >= capacity) ? 0 : static_cast<std::size_t>(capacity - offset);
  const std::size_t to_copy = std::min(src_bytes, room);
  if (to_copy < src_bytes) {
    spdlog::warn("{}: ring slot write truncated — offset={} capacity={} src={} writing={}", log_prefix, offset, capacity, src_bytes, to_copy);
  }
  std::memcpy(static_cast<std::uint8_t*>(mmap_va) + offset, src, to_copy);
  offset += to_copy;
  return to_copy;
}

RingProducerSlot AcquireRingProducerSlot(PayloadClient& client, const std::string& ring_id, std::string log_prefix) {
  RingProducerSlot s;
  s.log_prefix = std::move(log_prefix);
  s.client     = &client;

  auto resp = client.AcquireRingSlot(ring_id);
  if (!resp.ok()) {
    // "Every slot is still leased" is the documented steady-state
    // outcome under DROP_NEW and can arrive on every capture cycle, so
    // it stays at debug — callers distinguish via valid() and should
    // count it, not log it. A different status is a real failure.
    if (resp.status().IsCapacityError()) {
      spdlog::debug("{}: AcquireRingSlot('{}') found no free slot: {}", s.log_prefix, ring_id, resp.status().ToString());
    } else {
      spdlog::error("{}: AcquireRingSlot('{}') failed: {}", s.log_prefix, ring_id, resp.status().ToString());
    }
    return s; // capacity stays 0 ⇒ invalid ⇒ no rescue commit to make
  }
  const auto& acq = resp.ValueOrDie();

  // From here the reservation exists server-side. Every failure path
  // below must hand it back or the slot is retired for good.
  int fd = shm_open(acq.shm_name().c_str(), O_RDWR, 0);
  if (fd < 0) {
    spdlog::error("{}: ring slot shm_open('{}') failed: {} — committing empty to release the slot", s.log_prefix, acq.shm_name(),
                  std::strerror(errno));
    auto status = client.CommitRingSlot(ring_id, acq.slot_idx(), acq.generation(), 0);
    if (!status.ok()) {
      spdlog::error("{}: releasing ring '{}' slot {} after shm_open failure did not land, slot is now stuck: {}", s.log_prefix, ring_id,
                    acq.slot_idx(), status.ToString());
    }
    return s;
  }
  void* va = mmap(nullptr, acq.capacity_bytes(), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (va == MAP_FAILED) {
    spdlog::error("{}: ring slot mmap('{}', {} bytes) failed: {} — committing empty to release", s.log_prefix, acq.shm_name(), acq.capacity_bytes(),
                  std::strerror(errno));
    close(fd);
    auto status = client.CommitRingSlot(ring_id, acq.slot_idx(), acq.generation(), 0);
    if (!status.ok()) {
      spdlog::error("{}: releasing ring '{}' slot {} after mmap failure did not land, slot is now stuck: {}", s.log_prefix, ring_id, acq.slot_idx(),
                    status.ToString());
    }
    return s;
  }

  s.ring_id    = ring_id;
  s.slot_idx   = acq.slot_idx();
  s.generation = acq.generation();
  s.capacity   = acq.capacity_bytes();
  s.offset     = 0;
  s.mmap_va    = va;
  s.mmap_fd    = fd;
  return s;
}

bool CommitRingProducerSlot(PayloadClient& client, RingProducerSlot& s, core_v1::RingSlotRef* out_ref) {
  if (!s.valid()) return false;
  if (s.committed) {
    // PM rejects the duplicate with FAILED_PRECONDITION anyway; catching
    // it here keeps a caller bug from looking like a server error.
    spdlog::warn("{}: CommitRingProducerSlot called twice for ring '{}' slot {} — ignored", s.log_prefix, s.ring_id, s.slot_idx);
    return false;
  }
  auto status = client.CommitRingSlot(s.ring_id, s.slot_idx, s.generation, s.offset);
  if (!status.ok()) {
    // Leave `committed` false: the destructor's rescue commit gets one
    // more chance to hand the slot back.
    spdlog::error("{}: CommitRingSlot failed: {}", s.log_prefix, status.ToString());
    return false;
  }
  s.committed = true;
  if (out_ref) {
    out_ref->set_ring_id(s.ring_id);
    out_ref->set_slot_idx(s.slot_idx);
    out_ref->set_generation(s.generation);
    out_ref->set_size_bytes(s.offset);
  }
  return true;
}

} // namespace payload::manager::client

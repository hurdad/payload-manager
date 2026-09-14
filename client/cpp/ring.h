#pragma once

// Shared TIER_RAM_RING consumer / producer helpers.
//
// Every consumer of a ring needs the same three pieces: a lazily-built
// per-ring cache of slot mmaps (MapRing + shm_open + mmap each slot
// once), a per-capture lease that hands back a pointer into that cache,
// and a producer-side RAII slot handle. These were extracted here from
// near-identical copies that had accumulated in several downstream
// pipeline services.
//
// They live next to PayloadClient rather than in a separate library
// because they are thin wrappers over its ring RPCs — see the
// "Ring tier (TIER_RAM_RING)" block in client.h.
//
// Both handles below own a *server-side* reservation, not just a
// mapping. PM has no reaper for either one: an uncommitted slot stays
// WRITING forever and an unreleased lease holds the slot's refcount
// above zero forever, and in both cases the slot never becomes
// acquirable again for the life of the PM process. Dropping N of them
// kills an N-slot ring. That is why both are RAII and why neither is
// copyable — releasing on destruction is the whole point of the types.
//
// Consumer usage (per capture):
//   auto lease = consumer.LeaseAndOpen(ref.ring_id(), ref.slot_idx(),
//                                      ref.generation(), ref.size_bytes());
//   if (!lease) return;               // stale generation — skip the capture
//   DoWork(lease->host_va, lease->size_bytes);
//   // lease releases on scope exit; call lease->Release() to release early.
//
// Producer usage (one RingProducer for the process, then per capture):
//   auto slot = producer.Acquire(ring_id);
//   if (!slot.valid()) return;        // ring full — caller's drop policy
//   slot.Append(bytes, n);
//   slot.Commit(event.mutable_ring_slot());
//   // if the scope exits before the commit, the slot is committed empty
//   // so PM can recycle it.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "client.h"
#include "payload/manager/core/v1/ring_slot.pb.h"

namespace payload::manager::client {

// ============================================================================
// Consumer
// ============================================================================

/// Per-ring slot-mmap cache plus lease/release.
///
/// Rings are discovered lazily: a consumer generally does not know at
/// startup which rings it will see (that depends on upstream producer
/// config), so the first event carrying a given ring_id triggers
/// MapRing + per-slot mmap, and the resulting pointers are cached for
/// the object's lifetime.
///
/// Thread-safety: safe to share across threads. The mutex covers only
/// the ring cache, never an in-flight RPC — LeaseRingSlot is issued
/// outside the lock so concurrent consumers don't serialize on it.
/// Cached slot pointers stay valid once published: entries are never
/// erased or resized before destruction.
///
/// Lifetime: the destructor munmaps every cached slot without checking
/// for outstanding Leases, and a Lease holds a bare back-pointer to its
/// consumer. Every Lease must therefore be destroyed before the
/// RingConsumer that issued it.
class RingConsumer {
 public:
  struct Options {
    /// When true, open each slot O_RDWR and mmap it PROT_READ|PROT_WRITE,
    /// then cudaHostRegister + cudaHostGetDevicePointer it ONCE so the
    /// GPU can read the slot zero-copy (Lease::dev_va).
    ///
    /// The writable mapping is required even though the consumer only
    /// reads: L4T R36's cudaHostRegister rejects PROT_READ-only mappings
    /// with `invalid argument`. Read-only-ness comes from the consumer's
    /// own discipline, not from the mapping permissions.
    ///
    /// Ignored entirely — no CUDA symbol referenced, and the slot is
    /// still mapped read-only — unless the library was built with
    /// PAYLOAD_MANAGER_CLIENT_ENABLE_CUDA=ON. A non-CUDA build has
    /// nothing to gain from the writable mapping and no reason to give
    /// up the kernel-enforced read-only-ness.
    bool register_for_gpu = false;

    /// Prefix for this consumer's log lines, e.g. "spectral ring" —
    /// keeps per-module log greppability after the extraction.
    std::string log_prefix = "ring";
  };

  // No default for `opts`: a nested-class NSDMI can't be used in a
  // default argument of its own enclosing class, and every call site
  // wants its own log_prefix anyway.
  RingConsumer(PayloadClient* pm_client, Options opts);
  ~RingConsumer();

  RingConsumer(const RingConsumer&)            = delete;
  RingConsumer& operator=(const RingConsumer&) = delete;

  /// A granted read lease. Releases on destruction.
  ///
  /// Move-only: the lease is a server-side refcount on the slot, and PM
  /// expires nothing, so an accidental copy would double-release (benign)
  /// or a dropped copy would leak the slot permanently (not). Must not
  /// outlive the RingConsumer that issued it.
  class Lease {
   public:
    Lease() = default;
    ~Lease();

    Lease(const Lease&)            = delete;
    Lease& operator=(const Lease&) = delete;
    Lease(Lease&& other) noexcept;
    Lease& operator=(Lease&& other) noexcept;

    /// Host mapping of the slot. Always set on a granted lease.
    const void* host_va = nullptr;
    /// GPU device pointer for the slot; null unless Options::register_for_gpu
    /// and a CUDA-enabled build.
    const void* dev_va = nullptr;
    /// Bytes safe to read, already clamped to the slot's capacity.
    uint64_t size_bytes = 0;

    /// Opaque server-side handle. Empty on a default-constructed or
    /// moved-from lease.
    const std::string& lease_id() const {
      return lease_id_;
    }

    /// Release now rather than at scope exit — useful when the read is
    /// short but the enclosing scope is long, since a held lease blocks
    /// the producer from recycling the slot. Idempotent; the destructor
    /// then does nothing. The mapping pointers are left as they are:
    /// they stay mapped, but reading through them after this point
    /// races the producer.
    void Release();

   private:
    friend class RingConsumer;
    RingConsumer* owner_ = nullptr;
    std::string   lease_id_;
  };

  /// Map `ring_id` if not already mapped. Idempotent; returns false on
  /// MapRing or mmap failure (already logged), leaving nothing cached.
  bool EnsureMapped(const std::string& ring_id);

  /// Lease (slot_idx, generation) and return the cached pointers.
  /// Returns nullopt — already logged — when the ring can't be mapped,
  /// slot_idx is out of range, or the lease is refused. A refused lease
  /// is the expected outcome for a stale generation (the producer
  /// recycled the slot before this consumer got to it); callers should
  /// treat it as "drop this capture", not as an error.
  ///
  /// `size_bytes` comes from the wire and is clamped to the slot's
  /// capacity before being returned.
  std::optional<Lease> LeaseAndOpen(const std::string& ring_id, uint32_t slot_idx, uint64_t generation, uint64_t size_bytes);

  /// Drop a lease by id. Prefer letting the Lease handle do this; this
  /// is the primitive it is built on, exposed for callers that carry a
  /// bare lease_id across an API boundary. Idempotent at the protocol
  /// level (PM treats unknown lease_ids as OK), so the only failure path
  /// is gRPC transport.
  void Release(const std::string& lease_id);

 private:
  struct SlotMapping {
    void*  host_va  = nullptr;
    void*  dev_va   = nullptr; // cudaHostGetDevicePointer result
    size_t capacity = 0;       // ftruncated size; caps any read
    int    fd       = -1;
  };
  struct RingMapping {
    uint32_t                 n_slots       = 0;
    uint64_t                 slot_capacity = 0;
    std::vector<SlotMapping> slots;
  };

  /// MapRing + per-slot open/mmap/(register). No locking — the caller
  /// publishes the result. Returns false having torn down partial work.
  bool BuildMapping_(const std::string& ring_id, RingMapping& out);
  void TearDownRing_(RingMapping& r) const;
  /// Get-or-create the per-ring build mutex. Caller must hold mu_.
  std::shared_ptr<std::mutex> BuildMutexFor_(const std::string& ring_id);

  PayloadClient* pm_client_; // not owned; may be null (ring ops no-op)
  Options        opts_;

  mutable std::mutex                           mu_;
  std::unordered_map<std::string, RingMapping> rings_;
  /// One mutex per ring_id, held across that ring's BuildMapping_ so two
  /// threads racing on the same first event don't both issue MapRing and
  /// both cudaHostRegister the same shm objects. Per-ring rather than
  /// global so an unrelated ring's first touch still proceeds in
  /// parallel. shared_ptr because a waiter may still hold the mutex when
  /// the map entry is looked up again.
  std::unordered_map<std::string, std::shared_ptr<std::mutex>> build_mus_;
};

// ============================================================================
// Producer
// ============================================================================

/// Per-ring slot-mmap cache plus acquire/commit — the write side of
/// RingConsumer, and deliberately the same shape.
///
/// The mapping is built once per ring and reused for the life of the
/// object. That matters more here than on the read side: a producer
/// writes on every cycle, and mapping per capture meant shm_open + mmap
/// + munmap + close each time round, on the same handful of segments,
/// with a TLB shootdown on every unmap. A ring exists for rates where
/// that is worth removing.
///
/// Thread-safety and lifetime match RingConsumer: safe to share across
/// threads, the mutex covers only the cache and never an in-flight RPC,
/// and every Slot must be destroyed before the RingProducer that issued
/// it — the destructor unmaps without checking for live slots.
class RingProducer {
 public:
  struct Options {
    /// Prefix for this producer's log lines, e.g. "radio ring".
    std::string log_prefix = "ring";
  };

  RingProducer(PayloadClient* pm_client, Options opts);
  ~RingProducer();

  RingProducer(const RingProducer&)            = delete;
  RingProducer& operator=(const RingProducer&) = delete;

  /// RAII handle for one acquired slot.
  ///
  /// Move-only, and for the same reason as Lease: the handle owns a
  /// server-side reservation. PM leaves an acquired slot WRITING forever
  /// and reclaims nothing before slot_write_timeout_ms, so a dropped
  /// copy would retire a slot rather than merely leak a mapping.
  ///
  /// The destructor commits at length zero when the slot was acquired
  /// and never committed, which makes it acquirable again and gives
  /// consumers an empty payload they will skip.
  class Slot {
   public:
    Slot() = default;
    ~Slot();

    Slot(const Slot&)            = delete;
    Slot& operator=(const Slot&) = delete;
    Slot(Slot&& other) noexcept;
    Slot& operator=(Slot&& other) noexcept;

    /// Writable mapping of the slot, sized by capacity().
    ///
    /// Null once committed. The mapping itself stays alive — the producer
    /// owns it — so this is what keeps a late write from silently
    /// corrupting a slot a consumer is already reading: it faults instead.
    void* data() const {
      return host_va_;
    }
    /// Slot capacity in bytes. Zero on an invalid handle.
    std::uint64_t capacity() const {
      return capacity_;
    }
    /// Bytes appended so far; what Commit reports as size_bytes.
    std::uint64_t size() const {
      return offset_;
    }
    bool valid() const {
      return capacity_ > 0 && host_va_ != nullptr;
    }

    const std::string& ring_id() const {
      return ring_id_;
    }
    std::uint32_t slot_idx() const {
      return slot_idx_;
    }
    std::uint64_t generation() const {
      return generation_;
    }

    /// Copy `src_bytes` in at the current offset, advancing it. Truncates
    /// at capacity (logged) and returns the bytes actually written.
    ///
    /// Returns 0 without writing once committed: PM has published the
    /// slot and consumers may be reading it, so a late Append would be a
    /// data race rather than an append.
    std::size_t Append(const void* src, std::size_t src_bytes);

    /// CommitRingSlot, and when `out_ref` is non-null fill in the
    /// (ring_id, slot_idx, generation, size_bytes) a consumer needs.
    /// Suppresses the destructor's rescue commit and freezes the slot.
    /// Committing twice is refused locally, without an RPC.
    bool Commit(payload::manager::core::v1::RingSlotRef* out_ref);

   private:
    friend class RingProducer;
    RingProducer* owner_ = nullptr;
    std::string   ring_id_;
    std::uint32_t slot_idx_   = 0;
    std::uint64_t generation_ = 0;
    std::uint64_t capacity_   = 0;
    std::uint64_t offset_     = 0;
    void*         host_va_    = nullptr;
    bool          committed_  = false;
  };

  /// AcquireRingSlot, mapping the ring first if this is its first use.
  /// An invalid handle means either the ring could not be mapped or every
  /// slot is still leased — check valid(). The second is the documented
  /// steady state under DROP_NEW, not an error: callers should count it
  /// and drop the capture.
  Slot Acquire(const std::string& ring_id);

  /// Map `ring_id` if not already mapped. Idempotent; false on MapRing or
  /// mmap failure (already logged), leaving nothing cached.
  bool EnsureMapped(const std::string& ring_id);

 private:
  struct SlotMapping {
    void*  host_va  = nullptr;
    size_t capacity = 0;
    int    fd       = -1;
  };
  struct RingMapping {
    std::uint32_t            n_slots       = 0;
    std::uint64_t            slot_capacity = 0;
    std::vector<SlotMapping> slots;
  };

  bool                        BuildMapping_(const std::string& ring_id, RingMapping& out);
  void                        TearDownRing_(RingMapping& r) const;
  std::shared_ptr<std::mutex> BuildMutexFor_(const std::string& ring_id);

  /// Hand a reservation straight back at length zero. Used when a slot is
  /// acquired but cannot be mapped, and by ~Slot.
  void ReleaseEmpty_(const std::string& ring_id, std::uint32_t slot_idx, std::uint64_t generation);

  PayloadClient* pm_client_;
  Options        opts_;

  mutable std::mutex                                           mu_;
  std::unordered_map<std::string, RingMapping>                 rings_;
  std::unordered_map<std::string, std::shared_ptr<std::mutex>> build_mus_;
};

} // namespace payload::manager::client

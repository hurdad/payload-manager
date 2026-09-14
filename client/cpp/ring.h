#pragma once

// Shared TIER_RAM_RING consumer / producer helpers.
//
// Every module that touches a ring needs the same three pieces: a
// lazily-built per-ring cache of slot mmaps (MapRing + shm_open + mmap
// each slot once), a per-capture lease that hands back a pointer into
// that cache, and a producer-side RAII slot handle. Before this header
// those lived as near-identical copies in modules/radio,
// modules/iq-conditioner, modules/spectrum-analyzer and
// services/jetstream-replication-exporter.
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
// Producer usage (per capture):
//   auto slot = AcquireRingProducerSlot(client, ring_id, "radio");
//   if (!slot.valid()) return;        // ring full — caller's drop policy
//   slot.Append(bytes, n);
//   CommitRingProducerSlot(client, slot, event.mutable_ring_slot());
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

/// RAII handle for one acquired slot: AcquireRingSlot + shm_open + mmap RW
/// on acquire, commit-if-uncommitted + munmap + close on destruction. No
/// shm_unlink — PM owns the slot lifetime; this only releases the
/// process's view.
///
/// The destructor commits a zero-length payload when the slot was
/// acquired but never committed. PM leaves an acquired slot in WRITING
/// forever and never reclaims it, so without this an early return or a
/// throw between acquire and commit would retire one ring slot
/// permanently. A zero-length commit makes the slot acquirable again and
/// gives consumers an empty payload they will skip.
struct RingProducerSlot {
  std::string ring_id;
  uint32_t    slot_idx   = 0;
  uint64_t    generation = 0;
  uint64_t    capacity   = 0;
  uint64_t    offset     = 0; // bytes written so far
  void*       mmap_va    = nullptr;
  int         mmap_fd    = -1;
  std::string log_prefix = "ring";

  /// Issuing client, for the destructor's rescue commit. Not owned; must
  /// outlive the slot. Null on a default-constructed handle.
  PayloadClient* client = nullptr;
  /// Set once CommitRingProducerSlot succeeds. Suppresses the
  /// destructor's rescue commit and freezes Append.
  bool committed = false;

  RingProducerSlot() = default;
  ~RingProducerSlot();
  RingProducerSlot(const RingProducerSlot&)            = delete;
  RingProducerSlot& operator=(const RingProducerSlot&) = delete;
  RingProducerSlot(RingProducerSlot&&) noexcept;
  RingProducerSlot& operator=(RingProducerSlot&&) noexcept;

  bool valid() const {
    return capacity > 0 && mmap_va != nullptr;
  }

  /// Copy `src_bytes` in at the current offset, advancing it. Truncates
  /// at capacity (logged) and returns the number of bytes actually written.
  ///
  /// Returns 0 without writing once the slot has been committed: PM has
  /// already published the slot and consumers may be reading it, so a
  /// late Append would be a silent data race rather than an append.
  std::size_t Append(const void* src, std::size_t src_bytes);
};

/// AcquireRingSlot + open + mmap RW. On CapacityError (every slot still
/// leased) or any other failure the returned handle is invalid — check
/// valid(). When the slot was acquired but could not be mapped, it is
/// committed with size 0 so PM can recycle it rather than leaking it.
RingProducerSlot AcquireRingProducerSlot(PayloadClient& client, const std::string& ring_id, std::string log_prefix = "ring");

/// CommitRingSlot for `s` and, when `out_ref` is non-null, populate it
/// with the (ring_id, slot_idx, generation, size_bytes) a consumer needs.
/// Marks `s` committed on success, which suppresses the destructor's
/// rescue commit and freezes further Appends. Committing an
/// already-committed slot is refused locally (returns false without an
/// RPC) — PM would reject the duplicate anyway.
bool CommitRingProducerSlot(PayloadClient& client, RingProducerSlot& s, payload::manager::core::v1::RingSlotRef* out_ref);

} // namespace payload::manager::client

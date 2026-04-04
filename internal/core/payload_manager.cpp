#include "payload_manager.hpp"

#include <mutex>
#include <stdexcept>

#include "internal/core/placement_engine.hpp"
#include "internal/db/api/result.hpp"
#include "internal/db/model/payload_record.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/storage/ram/ram_arrow_store.hpp"
#include "internal/storage/storage_backend.hpp"
#include "payload/manager/catalog/v1/archive_metadata.pb.h"
#include "payload/manager/core/v1/policy.pb.h"
#if PAYLOAD_MANAGER_ARROW_CUDA
#include "internal/storage/gpu/cuda_arrow_store.hpp"
#endif
#include "internal/metadata/metadata_cache.hpp"
#include "internal/observability/logging.hpp"
#include "internal/observability/spans.hpp"
#include "internal/util/errors.hpp"
#include "internal/util/time.hpp"
#include "internal/util/uuid.hpp"
#include "payload/manager/v1.hpp"

namespace payload::core {

using namespace payload::manager::v1;

namespace {

std::string_view TierName(Tier tier) {
  switch (tier) {
    case TIER_RAM:
      return "ram";
    case TIER_DISK:
      return "disk";
    case TIER_GPU:
      return "gpu";
    case TIER_OBJECT:
      return "object";
    default:
      return "unknown";
  }
}

} // namespace

void PayloadManager::UpdateTierBytes(Tier tier, int64_t delta) {
  uint64_t bytes = 0;
  {
    std::lock_guard<std::mutex> lock(tier_bytes_guard_);
    auto&                       val = tier_bytes_[static_cast<int>(tier)];
    if (delta < 0 && static_cast<uint64_t>(-delta) > val) {
      PAYLOAD_LOG_ERROR("tier byte accounting underflow: programming error — UpdateTierBytes called with "
                        "delta that exceeds current total; clamping to 0. This indicates a missed Allocate "
                        "accounting call.",
                        {payload::observability::StringField("tier", TierName(tier)),
                         payload::observability::IntField("delta", delta),
                         payload::observability::IntField("current", static_cast<int64_t>(val))});
      val = 0;
    } else {
      val = static_cast<uint64_t>(static_cast<int64_t>(val) + delta);
    }
    bytes = val;
  }
  payload::observability::Metrics::Instance().SetTierOccupancyBytes(TierName(tier), bytes);
}

void PayloadManager::UpdateTierCount(Tier tier, int64_t delta) {
  uint64_t count = 0;
  {
    std::lock_guard<std::mutex> lock(tier_count_guard_);
    auto&                       val = tier_count_[static_cast<int>(tier)];
    if (delta < 0 && static_cast<uint64_t>(-delta) > val) {
      PAYLOAD_LOG_ERROR("tier count accounting underflow: programming error — UpdateTierCount called with "
                        "delta that exceeds current total; clamping to 0. This indicates a missed Allocate "
                        "accounting call.",
                        {payload::observability::StringField("tier", TierName(tier)),
                         payload::observability::IntField("delta", delta),
                         payload::observability::IntField("current", static_cast<int64_t>(val))});
      val = 0;
    } else {
      val = static_cast<uint64_t>(static_cast<int64_t>(val) + delta);
    }
    count = val;
  }
  payload::observability::Metrics::Instance().SetTierPayloadCount(TierName(tier), count);
}

namespace {

bool IsDurableTier(Tier tier) {
  return tier == TIER_DISK || tier == TIER_OBJECT;
}

payload::manager::catalog::v1::PayloadArchiveMetadata BuildSidecar(const PayloadDescriptor& descriptor) {
  payload::manager::catalog::v1::PayloadArchiveMetadata meta;
  *meta.mutable_uuid()               = descriptor.payload_id();
  meta.set_metadata_version(1);
  *meta.mutable_archived_at()        = payload::util::ToProto(payload::util::Now());
  *meta.mutable_payload_descriptor() = descriptor;
  return meta;
}

bool IsReadableState(PayloadState state) {
  return state == PAYLOAD_STATE_ACTIVE || state == PAYLOAD_STATE_SPILLING || state == PAYLOAD_STATE_DURABLE;
}

void ThrowIfDbError(const payload::db::Result& result, const std::string& context) {
  if (result) {
    return;
  }

  const auto message = result.message.empty() ? context : context + ": " + result.message;
  switch (result.code) {
    case payload::db::ErrorCode::AlreadyExists:
      throw payload::util::AlreadyExists(message);
    case payload::db::ErrorCode::NotFound:
      throw payload::util::NotFound(message);
    case payload::db::ErrorCode::Conflict:
      throw payload::util::InvalidState(message);
    default:
      throw std::runtime_error(message);
  }
}

db::model::PayloadRecord ToPayloadRecord(const PayloadDescriptor& descriptor) {
  db::model::PayloadRecord record;
  record.id      = payload::util::FromProto(descriptor.payload_id());
  record.tier    = descriptor.tier();
  record.state   = descriptor.state();
  record.version = descriptor.version();
  if (descriptor.has_ram()) {
    record.size_bytes = descriptor.ram().length_bytes();
  } else if (descriptor.has_gpu()) {
    record.size_bytes = descriptor.gpu().length_bytes();
  } else if (descriptor.has_disk()) {
    record.size_bytes = descriptor.disk().length_bytes();
  } else {
    record.size_bytes = 0;
  }
  return record;
}

PayloadDescriptor ToPayloadDescriptor(const db::model::PayloadRecord& record, const std::string& shm_prefix) {
  PayloadDescriptor descriptor;
  *descriptor.mutable_payload_id() = payload::util::ToProto(record.id);
  descriptor.set_tier(record.tier);
  descriptor.set_state(record.state);
  descriptor.set_version(record.version);
  if (record.size_bytes > 0) {
    switch (record.tier) {
      case TIER_GPU: {
        GpuLocation gpu;
        gpu.set_device_id(0);
        gpu.set_length_bytes(record.size_bytes);
        *descriptor.mutable_gpu() = gpu;
        break;
      }
      case TIER_DISK:
      case TIER_OBJECT: {
        DiskLocation disk;
        disk.set_path(payload::util::ToString(record.id) + ".bin");
        disk.set_offset_bytes(0);
        disk.set_length_bytes(record.size_bytes);
        *descriptor.mutable_disk() = disk;
        break;
      }
      case TIER_RAM:
      default: {
        RamLocation ram;
        ram.set_length_bytes(record.size_bytes);
        ram.set_slab_id(0);
        ram.set_block_index(0);
        ram.set_shm_name(payload::storage::RamArrowStore::ShmName(payload::util::ToProto(record.id), shm_prefix));
        *descriptor.mutable_ram() = ram;
        break;
      }
    }
  }
  return descriptor;
}

} // namespace

PayloadManager::PayloadManager(payload::storage::StorageFactory::TierMap storage, std::shared_ptr<payload::lease::LeaseManager> lease_mgr,
                               std::shared_ptr<payload::db::Repository> repository, std::shared_ptr<payload::metadata::MetadataCache> metadata_cache)
    : storage_(std::move(storage)), lease_mgr_(std::move(lease_mgr)), repository_(std::move(repository)), metadata_cache_(std::move(metadata_cache)) {
  // Cache the shm prefix from the RAM backend so descriptor building is consistent.
  const auto ram_it = storage_.find(TIER_RAM);
  if (ram_it != storage_.end() && ram_it->second) {
    if (auto* ram = dynamic_cast<payload::storage::RamArrowStore*>(ram_it->second.get())) {
      shm_prefix_ = ram->GetShmPrefix();
    }
  }
}

payload::util::UUID PayloadManager::Key(const PayloadID& id) {
  if (id.value().size() != 16) {
    throw payload::util::NotFound("payload not found; invalid or missing payload id");
  }
  return payload::util::FromProto(id);
}

std::shared_ptr<std::shared_mutex> PayloadManager::PayloadMutex(const PayloadID& id) {
  std::lock_guard<std::mutex> lock(payload_mutexes_guard_);
  auto&                       payload_mutex = payload_mutexes_[Key(id)];
  if (!payload_mutex) {
    payload_mutex = std::make_shared<std::shared_mutex>();
  }
  return payload_mutex;
}

bool PayloadManager::IsPinnedLocked(const payload::util::UUID& key, uint64_t now_ms) {
  const auto it = pins_.find(key);
  if (it == pins_.end()) {
    return false;
  }

  if (!it->second.expires_at_ms.has_value()) {
    return true;
  }

  if (*it->second.expires_at_ms > now_ms) {
    return true;
  }

  pins_.erase(it);
  return false;
}

void PayloadManager::SweepExpiredPins() {
  const uint64_t              now_ms = payload::util::ToUnixMillis(payload::util::Now());
  std::lock_guard<std::mutex> lock(pins_guard_);
  for (auto it = pins_.begin(); it != pins_.end();) {
    const auto& state = it->second;
    if (state.expires_at_ms.has_value() && *state.expires_at_ms <= now_ms) {
      it = pins_.erase(it);
    } else {
      ++it;
    }
  }
}

void PayloadManager::CacheSnapshot(const PayloadDescriptor& descriptor) {
  std::unique_lock lock(snapshot_cache_mutex_);
  snapshot_cache_[Key(descriptor.payload_id())] = descriptor;
}

void PayloadManager::PopulateLocation(PayloadDescriptor* descriptor) {
  if (!descriptor) {
    return;
  }

  const auto storage_it = storage_.find(descriptor->tier());
  if (storage_it == storage_.end() || !storage_it->second) {
    return;
  }

  auto&       backend = storage_it->second;
  const auto& id      = descriptor->payload_id();

  switch (descriptor->tier()) {
    case TIER_RAM: {
      const auto  size = backend->Size(id);
      RamLocation ram;
      ram.set_length_bytes(size);
      ram.set_slab_id(0);
      ram.set_block_index(0);
      ram.set_shm_name(payload::storage::RamArrowStore::ShmName(id, shm_prefix_));
      *descriptor->mutable_ram() = ram;
      return;
    }
    case TIER_DISK: {
      const auto   size = backend->Size(id);
      DiskLocation disk;
      disk.set_length_bytes(size);
      disk.set_offset_bytes(0);
      disk.set_path(payload::util::ToString(Key(id)) + ".bin");
      *descriptor->mutable_disk() = disk;
      return;
    }
    case TIER_OBJECT: {
      // Object storage is exposed via DiskLocation (path-based); the descriptor's
      // tier field distinguishes TIER_OBJECT from TIER_DISK at the client side.
      const auto   size = backend->Size(id);
      DiskLocation disk;
      disk.set_length_bytes(size);
      disk.set_offset_bytes(0);
      disk.set_path(payload::util::ToString(Key(id)) + ".bin");
      *descriptor->mutable_disk() = disk;
      return;
    }
    case TIER_GPU: {
#if PAYLOAD_MANAGER_ARROW_CUDA
      auto cuda_backend = std::dynamic_pointer_cast<payload::storage::CudaArrowStore>(backend);
      if (!cuda_backend) {
        throw payload::util::InvalidState("payload GPU backend is not CUDA-capable");
      }

      auto ipc_handle = cuda_backend->ExportIPC(id);
      if (!ipc_handle) {
        throw payload::util::InvalidState("GPU IPC handle export returned null");
      }
      auto gpu_buffer       = backend->Read(id);
      auto maybe_serialized = ipc_handle->Serialize();
      if (!maybe_serialized.ok()) {
        throw payload::util::InvalidState("GPU IPC handle serialize failed: " + maybe_serialized.status().ToString());
      }
      auto serialized = *maybe_serialized;

      GpuLocation gpu;
      gpu.set_device_id(0);
      gpu.set_length_bytes(static_cast<uint64_t>(gpu_buffer->size()));
      gpu.set_ipc_handle(serialized->data(), static_cast<size_t>(serialized->size()));
      *descriptor->mutable_gpu() = gpu;
      return;
#else
      throw payload::util::InvalidState("payload GPU tier requested but payload manager was built without CUDA support");
#endif
    }
    default:
      throw payload::util::InvalidState("payload tier is unspecified");
  }
}

PayloadDescriptor PayloadManager::Allocate(uint64_t size_bytes, Tier preferred, uint64_t ttl_ms, bool no_evict,
                                           const payload::manager::core::v1::EvictionPolicy& eviction_policy) {
  if (size_bytes == 0) {
    throw payload::util::InvalidArgument("allocate payload: size_bytes must be greater than zero");
  }
  constexpr uint64_t kMaxPayloadBytes = uint64_t{128} * 1024 * 1024 * 1024; // 128 GiB
  if (size_bytes > kMaxPayloadBytes) {
    throw payload::util::InvalidArgument("allocate payload: size_bytes exceeds maximum allowed size (128 GiB)");
  }

  PayloadDescriptor desc;
  *desc.mutable_payload_id() = payload::util::ToProto(payload::util::GenerateUUID());
  desc.set_tier(preferred);
  desc.set_state(PAYLOAD_STATE_ALLOCATED);
  desc.set_version(1);
  *desc.mutable_created_at() = payload::util::ToProto(payload::util::Now());

  const auto storage_it = storage_.find(preferred);
  if (storage_it != storage_.end() && storage_it->second) {
    try {
      storage_it->second->Allocate(desc.payload_id(), size_bytes);
    } catch (...) {
      payload::observability::Metrics::Instance().RecordAllocationFailure(TierName(preferred));
      throw;
    }
    PopulateLocation(&desc);
  } else {
    switch (preferred) {
      case TIER_GPU: {
        auto* gpu = desc.mutable_gpu();
        gpu->set_device_id(0);
        gpu->set_length_bytes(size_bytes);
        break;
      }
      case TIER_DISK:
      case TIER_OBJECT: {
        auto* disk = desc.mutable_disk();
        disk->set_path(payload::util::ToString(Key(desc.payload_id())) + ".bin");
        disk->set_offset_bytes(0);
        disk->set_length_bytes(size_bytes);
        break;
      }
      case TIER_RAM:
      default: {
        auto* ram = desc.mutable_ram();
        ram->set_length_bytes(size_bytes);
        ram->set_slab_id(0);
        ram->set_block_index(0);
        ram->set_shm_name(payload::storage::RamArrowStore::ShmName(desc.payload_id(), shm_prefix_));
        break;
      }
    }
  }

  // Determine effective eviction policy.
  using payload::manager::core::v1::EVICTION_PRIORITY_NEVER;
  const bool never_evict = no_evict || eviction_policy.priority() == EVICTION_PRIORITY_NEVER;

  auto record              = ToPayloadRecord(desc);
  // Always store the requested allocation size, not the backend-reported size.
  // PopulateLocation derives size from backend->Size() which may return 0 for
  // stub or write-only backends; the authoritative accounting size is the
  // caller-supplied size_bytes used in UpdateTierBytes below.
  record.size_bytes        = size_bytes;
  record.no_evict             = no_evict;
  record.eviction_priority    = static_cast<int>(eviction_policy.priority());
  record.min_residency_tier   = static_cast<int>(eviction_policy.min_residency_tier());
  record.require_durable      = eviction_policy.require_durable();

  // Determine spill target: use policy hint if set, otherwise fall back to TIER_DISK.
  const Tier spill_tier = (eviction_policy.spill_target() != TIER_UNSPECIFIED) ? eviction_policy.spill_target() : TIER_DISK;
  record.spill_target   = static_cast<int>(spill_tier);

  // no_evict overrides TTL: a no_evict payload never auto-expires.
  const auto now_ms    = payload::util::ToUnixMillis(payload::util::Now());
  record.created_at_ms = now_ms;
  if (!never_evict && ttl_ms > 0) {
    record.expires_at_ms = now_ms + ttl_ms;
  }

  try {
    auto tx = repository_->Begin();
    ThrowIfDbError(repository_->InsertPayload(*tx, record), "allocate payload");
    tx->Commit();
  } catch (...) {
    // Roll back the storage allocation so bytes are not orphaned.
    if (storage_it != storage_.end() && storage_it->second) {
      try {
        storage_it->second->Remove(desc.payload_id());
      } catch (...) {
      }
    }
    throw;
  }

  const auto key = Key(desc.payload_id());
  if (never_evict) {
    std::lock_guard<std::mutex> lock(no_evict_guard_);
    no_evict_ids_.insert(key);
  }
  {
    std::lock_guard<std::mutex> lock(spill_targets_guard_);
    spill_targets_[key] = spill_tier;
  }

  CacheSnapshot(desc);
  UpdateTierBytes(preferred, static_cast<int64_t>(size_bytes));
  UpdateTierCount(preferred, 1);
  return desc;
}

void PayloadManager::ExpireStale() {
  // Proactively remove expired pin entries so they don't accumulate indefinitely.
  // IsPinnedLocked prunes lazily on access, but payloads that are never re-checked
  // would otherwise hold stale map entries until process restart.
  SweepExpiredPins();

  const uint64_t now_ms = payload::util::ToUnixMillis(payload::util::Now());

  auto       tx      = repository_->Begin();
  const auto expired = repository_->ListExpiredPayloads(*tx, now_ms);
  tx->Commit();

  for (const auto& record : expired) {
    const PayloadID id = payload::util::ToProto(record.id);
    try {
      Delete(id, /*force=*/true);
    } catch (const std::exception& e) {
      PAYLOAD_LOG_WARN("expire stale: failed to delete expired payload (best effort)", {payload::observability::StringField("error", e.what())});
    }
  }
}

PayloadDescriptor PayloadManager::Commit(const PayloadID& id) {
  auto tx     = repository_->Begin();
  auto record = repository_->GetPayload(*tx, Key(id));
  if (!record.has_value()) throw payload::util::NotFound("commit payload: payload not found; allocate first and retry");
  if (record->state != PAYLOAD_STATE_ALLOCATED) {
    throw payload::util::InvalidState("commit payload: payload must be in allocated state before commit");
  }
  record->state = IsDurableTier(static_cast<Tier>(record->tier)) ? PAYLOAD_STATE_DURABLE : PAYLOAD_STATE_ACTIVE;
  record->version++;
  ThrowIfDbError(repository_->UpdatePayload(*tx, *record), "commit payload");
  const auto parents = repository_->GetParents(*tx, payload::util::ToString(Key(id)));
  tx->Commit();
  const auto descriptor = ToPayloadDescriptor(*record, shm_prefix_);
  auto       hydrated   = descriptor;
  PopulateLocation(&hydrated);
  CacheSnapshot(hydrated);
  // Write a JSON sidecar alongside the data file for durable tiers so that
  // the stored payload is self-describing independent of the database.
  if (IsDurableTier(hydrated.tier())) {
    const auto storage_it = storage_.find(hydrated.tier());
    if (storage_it != storage_.end() && storage_it->second) {
      try {
        auto sidecar = BuildSidecar(hydrated);
        for (const auto& p : parents) {
          auto* edge = sidecar.add_lineage();
          try {
            *edge->mutable_parent() = payload::util::ToProto(payload::util::FromString(p.parent_id));
          } catch (...) {
            edge->mutable_parent()->set_value(p.parent_id);
          }
          edge->set_operation(p.operation);
          edge->set_role(p.role);
          if (!p.parameters.empty()) {
            edge->set_parameters(p.parameters);
          }
        }
        storage_it->second->WriteSidecar(id, sidecar);
      } catch (const std::exception& e) {
        PAYLOAD_LOG_WARN("commit: sidecar write failed (non-fatal)",
                         {payload::observability::StringField("payload_id", payload::util::ToString(Key(id))),
                          payload::observability::StringField("error", e.what())});
      }
    }
  }
  // Register with the metadata cache so this payload is a candidate for
  // LRU-based tier eviction even if UpsertMetadata was never called.
  if (metadata_cache_) {
    payload::manager::v1::PayloadMetadata minimal;
    *minimal.mutable_id() = id;
    metadata_cache_->Put(id, minimal);
  }
  return hydrated;
}

void PayloadManager::Delete(const PayloadID& id, bool force) {
  // Hold delete_mutex_ to prevent new leases from being acquired between the check and the DB mutation.
  std::lock_guard<std::mutex> delete_lock(delete_mutex_);

  if (force) {
    // Invalidate active leases: marks them as invalid so HasActiveLeases
    // returns false immediately, but keeps entries in the table until holders
    // call ReleaseLease().
    lease_mgr_->InvalidateAll(id);
    // Wait for in-flight holders to finish and call ReleaseLease() BEFORE
    // acquiring payload_lock below.  ResolveSnapshot() also acquires
    // payload_lock (shared), so waiting inside payload_lock would deadlock
    // with a lease holder that calls ResolveSnapshot() before ReleaseLease().
    // AcquireReadLease() holds delete_mutex_, so no new leases can be granted
    // while we wait here, making the window safe.
    constexpr auto kForceDeleteLeaseWaitMs = 5'000u;
    lease_mgr_->WaitUntilNoLeases(id, std::chrono::steady_clock::now() + std::chrono::milliseconds(kForceDeleteLeaseWaitMs));
  }

  {
    std::unique_lock<std::shared_mutex> payload_lock(*PayloadMutex(id));

    if (!force && lease_mgr_->HasActiveLeases(id)) {
      throw payload::util::LeaseConflict("delete payload: active lease present; release leases or set force=true");
    }

    auto tx     = repository_->Begin();
    auto record = repository_->GetPayload(*tx, payload::util::FromProto(id));
    if (!record.has_value()) {
      throw payload::util::NotFound("delete payload: payload not found; verify payload id");
    }

    const Tier     payload_tier = record->tier;
    const uint64_t payload_size = record->size_bytes;
    ThrowIfDbError(repository_->DeletePayload(*tx, payload::util::FromProto(id)), "delete payload");
    tx->Commit();

    // Storage removal is best-effort: the DB commit is the authoritative deletion.
    // Suppress exceptions here to avoid leaving the manager in an inconsistent state
    // after a successful commit (orphaned storage bytes are preferable to a half-deleted payload).
    const auto storage_it = storage_.find(payload_tier);
    if (storage_it != storage_.end() && storage_it->second) {
      try {
        storage_it->second->Remove(id);
      } catch (const std::exception& e) {
        PAYLOAD_LOG_WARN("delete payload: storage removal failed after DB commit (orphaned storage bytes)",
                         {payload::observability::StringField("payload_id", payload::util::ToString(Key(id))), payload::observability::StringField("error", e.what())});
      }
    }

    {
      std::unique_lock lock(snapshot_cache_mutex_);
      snapshot_cache_.erase(Key(id));
    }

    {
      std::lock_guard<std::mutex> pins_lock(pins_guard_);
      pins_.erase(Key(id));
    }

    {
      std::lock_guard<std::mutex> lock(no_evict_guard_);
      no_evict_ids_.erase(Key(id));
    }

    {
      std::lock_guard<std::mutex> lock(spill_targets_guard_);
      spill_targets_.erase(Key(id));
    }

    UpdateTierBytes(payload_tier, -static_cast<int64_t>(payload_size));
    UpdateTierCount(payload_tier, -1);
    if (metadata_cache_) {
      metadata_cache_->Remove(id);
    }
  } // payload_lock released

  // Prune the per-payload mutex now that the payload is fully deleted.
  {
    std::lock_guard<std::mutex> guard(payload_mutexes_guard_);
    payload_mutexes_.erase(Key(id));
  }
}

PayloadDescriptor PayloadManager::ResolveSnapshot(const PayloadID& id) {
  std::shared_lock<std::shared_mutex> payload_lock(*PayloadMutex(id));

  {
    std::shared_lock lock(snapshot_cache_mutex_);
    const auto       cached = snapshot_cache_.find(Key(id));
    if (cached != snapshot_cache_.end()) {
      return cached->second;
    }
  }

  // Cache miss path: repository remains the durable backing store.
  auto tx     = repository_->Begin();
  auto record = repository_->GetPayload(*tx, payload::util::FromProto(id));
  if (!record.has_value()) throw payload::util::NotFound("resolve snapshot: payload not found; verify payload id");
  tx->Commit();

  auto descriptor = ToPayloadDescriptor(*record, shm_prefix_);
  PopulateLocation(&descriptor);
  CacheSnapshot(descriptor);
  return descriptor;
}

AcquireReadLeaseResponse PayloadManager::AcquireReadLease(const PayloadID& id, Tier min_tier, uint64_t min_duration_ms,
                                                          payload::manager::core::v1::PromotionPolicy promotion_policy) {
  std::lock_guard<std::mutex> delete_lock(delete_mutex_);

  auto desc = ResolveSnapshot(id);
  if (min_tier != TIER_UNSPECIFIED && PlacementEngine::IsHigherTier(min_tier, desc.tier())) {
    if (promotion_policy == payload::manager::core::v1::PROMOTION_POLICY_BEST_EFFORT) {
      throw payload::util::InvalidState(
          "acquire lease: best-effort promotion cannot satisfy min_tier; lower min_tier or change promotion policy to BLOCKING");
    }
    desc = PromoteUnlocked(id, min_tier);
  }
  if (!IsReadableState(desc.state())) {
    throw payload::util::InvalidState("acquire lease: payload is not readable; commit or promote payload before leasing");
  }
  auto lease = lease_mgr_->Acquire(id, desc, min_duration_ms);

  // Post-acquire re-check: verify the payload was not deleted between
  // ResolveSnapshot and Acquire. Both operations hold delete_mutex_ so a
  // concurrent Delete cannot slip in, but this guard makes the invariant
  // explicit and protects against future code paths that may skip the lock.
  {
    std::shared_lock cache_lock(snapshot_cache_mutex_);
    if (snapshot_cache_.find(Key(id)) == snapshot_cache_.end()) {
      lease_mgr_->Release(lease.lease_id);
      throw payload::util::NotFound("acquire lease: payload was deleted concurrently");
    }
  }

  AcquireReadLeaseResponse resp;
  *resp.mutable_payload_descriptor() = desc;
  *resp.mutable_lease_id()           = lease.lease_id;
  *resp.mutable_lease_expires_at()   = payload::util::ToProto(lease.expires_at);
  return resp;
}

void PayloadManager::ReleaseLease(const payload::manager::v1::LeaseID& lease_id) {
  lease_mgr_->Release(lease_id);
}

PayloadDescriptor PayloadManager::Promote(const PayloadID& id, Tier target) {
  std::lock_guard<std::mutex> delete_lock(delete_mutex_);

  return PromoteUnlocked(id, target);
}

void PayloadManager::Prefetch(const PayloadID& id, Tier target) {
  std::lock_guard<std::mutex> delete_lock(delete_mutex_);
  (void)PromoteUnlocked(id, target);
}

void PayloadManager::Pin(const PayloadID& id, uint64_t duration_ms) {
  std::lock_guard<std::mutex> delete_lock(delete_mutex_);

  (void)ResolveSnapshot(id);

  std::lock_guard<std::mutex> pins_lock(pins_guard_);
  PinState                    state;
  if (duration_ms > 0) {
    state.expires_at_ms = payload::util::ToUnixMillis(payload::util::Now()) + duration_ms;
  }
  pins_[Key(id)] = state;
}

void PayloadManager::Unpin(const PayloadID& id) {
  std::lock_guard<std::mutex> pins_lock(pins_guard_);
  pins_.erase(Key(id));
}

PayloadDescriptor PayloadManager::PromoteUnlocked(const PayloadID& id, Tier target) {
  std::unique_lock<std::shared_mutex> payload_lock(*PayloadMutex(id));

  auto tx     = repository_->Begin();
  auto record = repository_->GetPayload(*tx, payload::util::FromProto(id));
  if (!record.has_value()) throw payload::util::NotFound("promote payload: payload not found; verify payload id");
  if (record->state == PAYLOAD_STATE_DELETED) {
    throw payload::util::InvalidState("promote payload: payload is deleted and cannot be promoted");
  }

  const Tier source_tier = record->tier;

  if (source_tier != target && lease_mgr_->HasActiveLeases(id)) {
    throw payload::util::LeaseConflict("promote payload: active lease present on source tier; release leases before promoting");
  }

  // Move data between storage tiers when they differ.
  if (source_tier != target) {
    auto src_it = storage_.find(source_tier);
    auto dst_it = storage_.find(target);
    if (src_it == storage_.end() || !src_it->second) {
      throw payload::util::InvalidState("promote payload: source storage tier is not available");
    }
    if (dst_it == storage_.end() || !dst_it->second) {
      throw payload::util::InvalidState("promote payload: target storage tier is not available");
    }

    auto buffer = src_it->second->Read(id);
    dst_it->second->Write(id, buffer, /*fsync=*/false);
  }

  record->tier  = target;
  record->state = IsDurableTier(target) ? PAYLOAD_STATE_DURABLE : PAYLOAD_STATE_ACTIVE;
  record->version++;
  ThrowIfDbError(repository_->UpdatePayload(*tx, *record), "promote payload");
  tx->Commit();

  // Remove source data only after DB commit so a crash cannot lose bytes.
  // Suppress Remove() exceptions: the DB commit is the authoritative tier
  // change; orphaned source bytes are preferable to surfacing an error after
  // a successful commit (the next spill/promote will be a no-op).
  if (source_tier != target) {
    auto src_it = storage_.find(source_tier);
    if (src_it != storage_.end() && src_it->second) {
      try {
        src_it->second->Remove(id);
      } catch (const std::exception& e) {
        PAYLOAD_LOG_WARN("promote: source removal failed after DB commit (orphaned source bytes)",
                         {payload::observability::StringField("payload_id", payload::util::ToString(Key(id))),
                          payload::observability::StringField("error", e.what())});
      }
    }
  }

  auto descriptor = ToPayloadDescriptor(*record, shm_prefix_);
  PopulateLocation(&descriptor);
  CacheSnapshot(descriptor);
  if (source_tier != target) {
    UpdateTierBytes(source_tier, -static_cast<int64_t>(record->size_bytes));
    UpdateTierBytes(target, static_cast<int64_t>(record->size_bytes));
    UpdateTierCount(source_tier, -1);
    UpdateTierCount(target, 1);
  }
  return descriptor;
}

void PayloadManager::HydrateCaches() {
  auto       tx      = repository_->Begin();
  const auto records = repository_->ListPayloads(*tx);
  tx->Commit();

  using payload::manager::core::v1::EVICTION_PRIORITY_NEVER;

  std::unordered_set<payload::util::UUID>                    new_no_evict;
  std::unordered_map<payload::util::UUID, Tier>              new_spill_targets;
  std::unordered_map<payload::util::UUID, PayloadDescriptor> new_snapshot_cache;

  for (const auto& record : records) {
    auto descriptor = ToPayloadDescriptor(record, shm_prefix_);
    try {
      PopulateLocation(&descriptor);
    } catch (const std::exception&) {
      // Ignore hydration failures for missing/evicted bytes; descriptor will be rebuilt on demand.
    }
    new_snapshot_cache[record.id] = descriptor;

    if (record.no_evict || record.eviction_priority == static_cast<int>(EVICTION_PRIORITY_NEVER)) {
      new_no_evict.insert(record.id);
    }

    new_spill_targets[record.id] = (record.spill_target != 0) ? static_cast<Tier>(record.spill_target) : TIER_DISK;
  }

  // Bump the persisted version for every non-terminal payload so that any
  // PayloadDescriptor a client cached before this restart is clearly stale.
  // The in-memory LeaseTable is empty after a restart; without this bump a
  // client could re-acquire a lease and receive the same version it already
  // cached, with no signal that placement may have changed during downtime.
  //
  // Note: this version bump runs in a second transaction after the initial
  // ListPayloads snapshot.  Payloads allocated concurrently between the two
  // transactions are not bumped, but that is harmless — those payloads are
  // brand new and carry no pre-restart cached descriptors.
  {
    PAYLOAD_LOG_WARN("startup: in-memory lease table cleared; bumping payload versions to invalidate pre-restart descriptors");
    auto bump_tx = repository_->Begin();
    for (const auto& record : records) {
      if (record.state == PAYLOAD_STATE_DELETED || record.state == PAYLOAD_STATE_EXPIRED) {
        continue;
      }
      db::model::PayloadRecord bumped = record;
      bumped.version++;
      const auto bump_result = repository_->UpdatePayload(*bump_tx, bumped);
      if (!bump_result && bump_result.code != payload::db::ErrorCode::NotFound) {
        PAYLOAD_LOG_WARN("hydrate: version bump update failed",
                         {payload::observability::StringField("payload_id", payload::util::ToString(record.id)),
                          payload::observability::StringField("error", bump_result.message)});
      }
      auto it = new_snapshot_cache.find(record.id);
      if (it != new_snapshot_cache.end()) {
        it->second.set_version(bumped.version);
      }
    }
    bump_tx->Commit();
  }

  {
    std::unique_lock lock(snapshot_cache_mutex_);
    snapshot_cache_ = std::move(new_snapshot_cache);
  }
  {
    std::lock_guard<std::mutex> lock(no_evict_guard_);
    no_evict_ids_ = std::move(new_no_evict);
  }
  {
    std::lock_guard<std::mutex> lock(spill_targets_guard_);
    spill_targets_ = std::move(new_spill_targets);
  }

  // Register all active payloads in the metadata cache so they are candidates
  // for LRU-based tier eviction even if UpsertMetadata was never called.
  if (metadata_cache_) {
    for (const auto& record : records) {
      if (record.state == PAYLOAD_STATE_ACTIVE || record.state == PAYLOAD_STATE_DURABLE) {
        payload::manager::v1::PayloadID id = payload::util::ToProto(record.id);
        payload::manager::v1::PayloadMetadata minimal;
        *minimal.mutable_id() = id;
        metadata_cache_->Put(id, minimal);
      }
    }
  }

  std::unordered_map<int, uint64_t> new_tier_bytes;
  std::unordered_map<int, uint64_t> new_tier_count;
  for (const auto& record : records) {
    new_tier_bytes[static_cast<int>(record.tier)] += record.size_bytes;
    new_tier_count[static_cast<int>(record.tier)] += 1;
  }
  std::unordered_map<int, uint64_t> tier_snapshot;
  {
    std::lock_guard<std::mutex> lock(tier_bytes_guard_);
    tier_bytes_   = std::move(new_tier_bytes);
    tier_snapshot = tier_bytes_;
  }
  std::unordered_map<int, uint64_t> count_snapshot;
  {
    std::lock_guard<std::mutex> lock(tier_count_guard_);
    tier_count_    = std::move(new_tier_count);
    count_snapshot = tier_count_;
  }
  for (const auto& [tier_int, bytes] : tier_snapshot) {
    payload::observability::Metrics::Instance().SetTierOccupancyBytes(TierName(static_cast<Tier>(tier_int)), bytes);
  }
  for (const auto& [tier_int, count] : count_snapshot) {
    payload::observability::Metrics::Instance().SetTierPayloadCount(TierName(static_cast<Tier>(tier_int)), count);
  }
}

void PayloadManager::ExecuteSpill(const PayloadID& id, Tier target, bool fsync) {
  std::unique_lock<std::shared_mutex> payload_lock(*PayloadMutex(id));

  // Read record and validate inside Phase 1 transaction.
  auto tx1    = repository_->Begin();
  auto record = repository_->GetPayload(*tx1, payload::util::FromProto(id));
  if (!record.has_value()) throw payload::util::NotFound("spill payload: payload not found; verify payload id");
  if (record->state == PAYLOAD_STATE_DELETED) {
    throw payload::util::InvalidState("spill payload: payload is deleted and cannot be spilled");
  }

  const Tier source_tier = record->tier;

  // Enforce require_durable: if set, only allow spill to a durable tier.
  if (record->require_durable && !IsDurableTier(target)) {
    throw payload::util::InvalidState("spill payload: eviction policy requires durable target tier");
  }

  // Enforce min_residency_tier: target must not be below (slower than) minimum.
  if (record->min_residency_tier != 0) {
    const Tier min_tier = static_cast<Tier>(record->min_residency_tier);
    // IsHigherTier(a, b) returns true if a is faster; "below minimum" means target is slower.
    if (PlacementEngine::IsHigherTier(min_tier, target)) {
      throw payload::util::InvalidState("spill payload: target tier is below the payload's min_residency_tier");
    }
  }

  {
    std::lock_guard<std::mutex> pins_lock(pins_guard_);
    if (source_tier != target && IsPinnedLocked(Key(id), payload::util::ToUnixMillis(payload::util::Now()))) {
      throw payload::util::LeaseConflict("spill payload: payload is pinned; unpin or wait for pin expiry before spilling");
    }
  }

  // Respect active read leases: spilling moves bytes to a new location, which
  // invalidates any descriptor held by a leaseholder.  This mirrors the check
  // in PromoteUnlocked and prevents silent data-location races.
  if (source_tier != target && lease_mgr_->HasActiveLeases(id)) {
    throw payload::util::LeaseConflict("spill payload: active lease present; release leases before spilling");
  }

  if (source_tier != target) {
    auto src_it = storage_.find(source_tier);
    auto dst_it = storage_.find(target);
    if (src_it == storage_.end() || !src_it->second) {
      throw payload::util::InvalidState("spill payload: source storage tier is not available");
    }
    if (dst_it == storage_.end() || !dst_it->second) {
      throw payload::util::InvalidState("spill payload: target storage tier is not available");
    }

    // --- Phase 1: mark SPILLING before touching bytes ---
    record->state = PAYLOAD_STATE_SPILLING;
    record->version++;
    ThrowIfDbError(repository_->UpdatePayload(*tx1, *record), "spill payload: phase 1");
    tx1->Commit();
    {
      auto spilling_descriptor = ToPayloadDescriptor(*record, shm_prefix_);
      PopulateLocation(&spilling_descriptor);
      CacheSnapshot(spilling_descriptor);
    }

    // --- Copy bytes; revert to ACTIVE/DURABLE on failure ---
    try {
      auto buffer = src_it->second->Read(id);
      dst_it->second->Write(id, buffer, fsync);
    } catch (...) {
      try {
        auto tx_revert  = repository_->Begin();
        auto revert_rec = repository_->GetPayload(*tx_revert, payload::util::FromProto(id));
        if (revert_rec.has_value()) {
          revert_rec->state = IsDurableTier(static_cast<Tier>(revert_rec->tier)) ? PAYLOAD_STATE_DURABLE : PAYLOAD_STATE_ACTIVE;
          revert_rec->version++;
          repository_->UpdatePayload(*tx_revert, *revert_rec);
          tx_revert->Commit();
          auto reverted_descriptor = ToPayloadDescriptor(*revert_rec, shm_prefix_);
          PopulateLocation(&reverted_descriptor);
          CacheSnapshot(reverted_descriptor);
        }
      } catch (const std::exception& revert_err) {
        PAYLOAD_LOG_WARN("spill: failed to revert SPILLING state after copy failure",
                         {payload::observability::StringField("payload_id", payload::util::ToString(Key(id))),
                          payload::observability::StringField("error", revert_err.what())});
      }
      throw;
    }

    // --- Phase 2: commit new tier + DURABLE/ACTIVE state ---
    // Re-use the in-memory record; no re-read needed since payload_lock is held throughout.
    auto tx2      = repository_->Begin();
    record->tier  = target;
    record->state = IsDurableTier(target) ? PAYLOAD_STATE_DURABLE : PAYLOAD_STATE_ACTIVE;
    record->version++;
    ThrowIfDbError(repository_->UpdatePayload(*tx2, *record), "spill payload: phase 2");
    tx2->Commit();
  } else {
    // No-op tier change: just commit without altering state.
    tx1->Commit();
  }

  // Remove source data only after DB commit so a crash cannot lose bytes.
  // Suppress Remove() exceptions: the DB commit is authoritative; orphaned
  // source bytes are preferable to surfacing an error after a successful commit.
  if (source_tier != target) {
    auto src_it = storage_.find(source_tier);
    if (src_it != storage_.end() && src_it->second) {
      try {
        src_it->second->Remove(id);
      } catch (const std::exception& e) {
        PAYLOAD_LOG_WARN("spill: source removal failed after DB commit (orphaned source bytes)",
                         {payload::observability::StringField("payload_id", payload::util::ToString(Key(id))),
                          payload::observability::StringField("error", e.what())});
      }
    }
  }

  auto descriptor = ToPayloadDescriptor(*record, shm_prefix_);
  PopulateLocation(&descriptor);
  CacheSnapshot(descriptor);
  // Write sidecar after a successful spill to a durable tier.
  if (source_tier != target && IsDurableTier(target)) {
    const auto dst_it = storage_.find(target);
    if (dst_it != storage_.end() && dst_it->second) {
      try {
        dst_it->second->WriteSidecar(id, BuildSidecar(descriptor));
      } catch (const std::exception& e) {
        PAYLOAD_LOG_WARN("spill: sidecar write failed (non-fatal)",
                         {payload::observability::StringField("payload_id", payload::util::ToString(Key(id))),
                          payload::observability::StringField("error", e.what())});
      }
    }
  }
  if (source_tier != target) {
    payload::observability::Metrics::Instance().RecordSpillBytes("background", record->size_bytes);
    UpdateTierBytes(source_tier, -static_cast<int64_t>(record->size_bytes));
    UpdateTierBytes(target, static_cast<int64_t>(record->size_bytes));
    UpdateTierCount(source_tier, -1);
    UpdateTierCount(target, 1);
  }
}

std::unordered_map<int, uint64_t> PayloadManager::GetTierBytes() const {
  std::lock_guard<std::mutex> lock(tier_bytes_guard_);
  return tier_bytes_;
}

bool PayloadManager::IsEvictionExempt(const PayloadID& id) const {
  std::lock_guard<std::mutex> lock(no_evict_guard_);
  return no_evict_ids_.count(Key(id)) > 0;
}

Tier PayloadManager::GetSpillTarget(const PayloadID& id) const {
  std::lock_guard<std::mutex> lock(spill_targets_guard_);
  const auto                  it = spill_targets_.find(Key(id));
  return (it != spill_targets_.end()) ? it->second : TIER_DISK;
}

} // namespace payload::core

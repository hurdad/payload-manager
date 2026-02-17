#include "payload_manager.hpp"

#include <mutex>
#include <stdexcept>

#include "internal/db/model/payload_record.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/storage/storage_backend.hpp"
#if PAYLOAD_MANAGER_ARROW_CUDA
#include "internal/storage/gpu/cuda_arrow_store.hpp"
#endif
#include "internal/util/errors.hpp"
#include "internal/util/time.hpp"
#include "internal/util/uuid.hpp"
#include "payload/manager/v1.hpp"

namespace payload::core {

using namespace payload::manager::v1;

namespace {

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
  record.id      = descriptor.id().value();
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

PayloadDescriptor ToPayloadDescriptor(const db::model::PayloadRecord& record) {
  PayloadDescriptor descriptor;
  descriptor.mutable_id()->set_value(record.id);
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
        disk.set_path(record.id + ".bin");
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
        ram.set_shm_name("payload");
        *descriptor.mutable_ram() = ram;
        break;
      }
    }
  }
  return descriptor;
}

} // namespace

PayloadManager::PayloadManager(payload::storage::StorageFactory::TierMap storage, std::shared_ptr<payload::lease::LeaseManager> lease_mgr,
                               std::shared_ptr<payload::metadata::MetadataCache>, std::shared_ptr<payload::lineage::LineageGraph>,
                               std::shared_ptr<payload::db::Repository> repository)
    : storage_(std::move(storage)), lease_mgr_(std::move(lease_mgr)), repository_(std::move(repository)) {
}

std::string PayloadManager::Key(const PayloadID& id) {
  return id.value();
}

std::shared_ptr<std::shared_mutex> PayloadManager::PayloadMutex(const PayloadID& id) {
  std::lock_guard<std::mutex> lock(payload_mutexes_guard_);
  auto&                       payload_mutex = payload_mutexes_[Key(id)];
  if (!payload_mutex) {
    payload_mutex = std::make_shared<std::shared_mutex>();
  }
  return payload_mutex;
}

void PayloadManager::CacheSnapshot(const PayloadDescriptor& descriptor) {
  std::unique_lock lock(snapshot_cache_mutex_);
  snapshot_cache_[descriptor.id().value()] = descriptor;
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
  const auto& id      = descriptor->id();

  switch (descriptor->tier()) {
    case TIER_RAM: {
      const auto  buffer = backend->Read(id);
      RamLocation ram;
      ram.set_length_bytes(static_cast<uint64_t>(buffer->size()));
      ram.set_slab_id(0);
      ram.set_block_index(0);
      ram.set_shm_name("payload");
      *descriptor->mutable_ram() = ram;
      return;
    }
    case TIER_DISK: {
      const auto   buffer = backend->Read(id);
      DiskLocation disk;
      disk.set_length_bytes(static_cast<uint64_t>(buffer->size()));
      disk.set_offset_bytes(0);
      disk.set_path(id.value() + ".bin");
      *descriptor->mutable_disk() = disk;
      return;
    }
    case TIER_OBJECT: {
      const auto   buffer = backend->Read(id);
      DiskLocation object;
      object.set_length_bytes(static_cast<uint64_t>(buffer->size()));
      object.set_offset_bytes(0);
      object.set_path(id.value() + ".bin");
      *descriptor->mutable_disk() = object;
      return;
    }
    case TIER_GPU: {
#if PAYLOAD_MANAGER_ARROW_CUDA
      auto cuda_backend = std::dynamic_pointer_cast<payload::storage::CudaArrowStore>(backend);
      if (!cuda_backend) {
        throw payload::util::InvalidState("payload GPU backend is not CUDA-capable");
      }

      auto ipc_handle       = cuda_backend->ExportIPC(id);
      auto gpu_buffer       = backend->Read(id);
      auto maybe_serialized = ipc_handle->Serialize();
      if (!maybe_serialized.ok()) throw std::runtime_error("GPU IPC handle serialize failed: " + maybe_serialized.status().ToString());
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

PayloadDescriptor PayloadManager::Allocate(uint64_t size_bytes, Tier preferred) {
  PayloadDescriptor desc;
  *desc.mutable_id() = payload::util::ToProto(payload::util::GenerateUUID());
  desc.set_tier(preferred);
  desc.set_state(PAYLOAD_STATE_ALLOCATED);
  desc.set_version(1);
  *desc.mutable_created_at() = payload::util::ToProto(payload::util::Now());

  const auto storage_it = storage_.find(preferred);
  if (storage_it != storage_.end() && storage_it->second) {
    storage_it->second->Allocate(desc.id(), size_bytes);
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
        disk->set_path(desc.id().value() + ".bin");
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
        ram->set_shm_name("payload");
        break;
      }
    }
  }

  auto tx = repository_->Begin();
  ThrowIfDbError(repository_->InsertPayload(*tx, ToPayloadRecord(desc)), "allocate payload");
  tx->Commit();
  CacheSnapshot(desc);
  return desc;
}

PayloadDescriptor PayloadManager::Commit(const PayloadID& id) {
  auto tx     = repository_->Begin();
  auto record = repository_->GetPayload(*tx, Key(id));
  if (!record.has_value()) throw payload::util::NotFound("commit payload: payload not found; allocate first and retry");
  if (record->state != PAYLOAD_STATE_ALLOCATED) {
    throw payload::util::InvalidState("commit payload: payload must be in allocated state before commit");
  }
  record->state = PAYLOAD_STATE_ACTIVE;
  record->version++;
  ThrowIfDbError(repository_->UpdatePayload(*tx, *record), "commit payload");
  tx->Commit();
  const auto descriptor = ToPayloadDescriptor(*record);
  auto       hydrated   = descriptor;
  PopulateLocation(&hydrated);
  CacheSnapshot(hydrated);
  return hydrated;
}

void PayloadManager::Delete(const PayloadID& id, bool force) {
  // Hold delete_mutex_ to prevent new leases from being acquired between the check and the DB mutation.
  std::lock_guard<std::mutex> delete_lock(delete_mutex_);
  std::unique_lock<std::shared_mutex> payload_lock(*PayloadMutex(id));

  if (force) {
    lease_mgr_->InvalidateAll(id);
  }
  if (!force && lease_mgr_->HasActiveLeases(id)) {
    throw payload::util::LeaseConflict("delete payload: active lease present; release leases or set force=true");
  }

  Tier payload_tier = TIER_UNSPECIFIED;
  {
    auto existing = ResolveSnapshot(id);
    payload_tier  = existing.tier();
  }

  auto tx = repository_->Begin();
  ThrowIfDbError(repository_->DeletePayload(*tx, Key(id)), "delete payload");
  tx->Commit();

  const auto storage_it = storage_.find(payload_tier);
  if (storage_it != storage_.end() && storage_it->second) {
    storage_it->second->Remove(id);
  }

  std::unique_lock lock(snapshot_cache_mutex_);
  snapshot_cache_.erase(Key(id));
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
  auto record = repository_->GetPayload(*tx, Key(id));
  if (!record.has_value()) throw payload::util::NotFound("resolve snapshot: payload not found; verify payload id");
  tx->Commit();

  auto descriptor = ToPayloadDescriptor(*record);
  PopulateLocation(&descriptor);
  CacheSnapshot(descriptor);
  return descriptor;
}

AcquireReadLeaseResponse PayloadManager::AcquireReadLease(const PayloadID& id, Tier min_tier, uint64_t min_duration_ms) {
  std::lock_guard<std::mutex> delete_lock(delete_mutex_);

  auto desc = ResolveSnapshot(id);
  if (desc.tier() < min_tier) {
    desc = PromoteUnlocked(id, min_tier);
  }
  if (!IsReadableState(desc.state())) {
    throw payload::util::InvalidState("acquire lease: payload is not readable; commit or promote payload before leasing");
  }
  auto lease = lease_mgr_->Acquire(id, desc, min_duration_ms);

  AcquireReadLeaseResponse resp;
  *resp.mutable_payload_descriptor() = desc;
  *resp.mutable_lease_id() = lease.lease_id;
  *resp.mutable_lease_expires_at() = payload::util::ToProto(lease.expires_at);
  return resp;
}

void PayloadManager::ReleaseLease(const payload::manager::v1::LeaseID& lease_id) {
  lease_mgr_->Release(lease_id);
}

PayloadDescriptor PayloadManager::Promote(const PayloadID& id, Tier target) {
  std::lock_guard<std::mutex> delete_lock(delete_mutex_);

  return PromoteUnlocked(id, target);
}

PayloadDescriptor PayloadManager::PromoteUnlocked(const PayloadID& id, Tier target) {
  std::unique_lock<std::shared_mutex> payload_lock(*PayloadMutex(id));

  auto tx     = repository_->Begin();
  auto record = repository_->GetPayload(*tx, Key(id));
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
    src_it->second->Remove(id);
  }

  record->tier = target;
  record->version++;
  ThrowIfDbError(repository_->UpdatePayload(*tx, *record), "promote payload");
  tx->Commit();

  auto descriptor = ToPayloadDescriptor(*record);
  PopulateLocation(&descriptor);
  CacheSnapshot(descriptor);
  return descriptor;
}

void PayloadManager::HydrateCaches() {
  auto       tx      = repository_->Begin();
  const auto records = repository_->ListPayloads(*tx);
  tx->Commit();

  std::unique_lock lock(snapshot_cache_mutex_);
  snapshot_cache_.clear();
  for (const auto& record : records) {
    auto descriptor = ToPayloadDescriptor(record);
    try {
      PopulateLocation(&descriptor);
    } catch (const std::exception&) {
      // Ignore hydration failures for missing/evicted bytes; descriptor will be rebuilt on demand.
    }
    snapshot_cache_[descriptor.id().value()] = descriptor;
  }
}

void PayloadManager::ExecuteSpill(const PayloadID& id, Tier target, bool fsync) {
  std::unique_lock<std::shared_mutex> payload_lock(*PayloadMutex(id));

  auto tx     = repository_->Begin();
  auto record = repository_->GetPayload(*tx, Key(id));
  if (!record.has_value()) throw payload::util::NotFound("spill payload: payload not found; verify payload id");
  if (record->state == PAYLOAD_STATE_DELETED) {
    throw payload::util::InvalidState("spill payload: payload is deleted and cannot be spilled");
  }

  const Tier source_tier = record->tier;

  if (source_tier != target) {
    auto src_it = storage_.find(source_tier);
    auto dst_it = storage_.find(target);
    if (src_it == storage_.end() || !src_it->second) {
      throw payload::util::InvalidState("spill payload: source storage tier is not available");
    }
    if (dst_it == storage_.end() || !dst_it->second) {
      throw payload::util::InvalidState("spill payload: target storage tier is not available");
    }

    auto buffer = src_it->second->Read(id);
    dst_it->second->Write(id, buffer, fsync);
    src_it->second->Remove(id);
  }

  record->tier = target;
  record->version++;
  ThrowIfDbError(repository_->UpdatePayload(*tx, *record), "spill payload");
  tx->Commit();

  auto descriptor = ToPayloadDescriptor(*record);
  PopulateLocation(&descriptor);
  CacheSnapshot(descriptor);
}

} // namespace payload::core

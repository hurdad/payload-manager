#include "payload_manager_service.h"

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <random>
#include <sstream>
#include <utility>

namespace {

namespace pb = google::protobuf;
using payload::manager::v1::EvictionPolicy;
using payload::manager::v1::LineageEdge;
using payload::manager::v1::MetadataUpdateMode;
using payload::manager::v1::PayloadDescriptor;
using payload::manager::v1::PayloadMetadata;
using payload::manager::v1::PayloadMetadataEvent;
using payload::manager::v1::PayloadState;
using payload::manager::v1::Tier;

pb::Timestamp ToTimestamp(std::chrono::system_clock::time_point tp) {
  const auto sec = std::chrono::time_point_cast<std::chrono::seconds>(tp);
  const auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(tp - sec);

  pb::Timestamp ts;
  ts.set_seconds(sec.time_since_epoch().count());
  ts.set_nanos(static_cast<int32_t>(nanos.count()));
  return ts;
}

std::chrono::system_clock::time_point Now() { return std::chrono::system_clock::now(); }

std::string RandomBytesAsString(std::size_t len) {
  static thread_local std::mt19937_64 rng{std::random_device{}()};
  std::uniform_int_distribution<int> dist(0, 255);
  std::string out(len, '\0');
  for (std::size_t i = 0; i < len; ++i) {
    out[i] = static_cast<char>(dist(rng));
  }
  return out;
}

std::string RandomTokenHex(std::size_t bytes) {
  static thread_local std::mt19937_64 rng{std::random_device{}()};
  std::uniform_int_distribution<int> dist(0, 255);

  std::ostringstream oss;
  oss << std::hex << std::setfill('0');
  for (std::size_t i = 0; i < bytes; ++i) {
    oss << std::setw(2) << dist(rng);
  }

  return oss.str();
}

bool IsValidUuid(const std::string& uuid) { return uuid.size() == 16; }

}  // namespace

grpc::Status PayloadManagerServiceImpl::AllocatePayload(
    grpc::ServerContext*, const payload::manager::v1::AllocatePayloadRequest* request,
    payload::manager::v1::AllocatePayloadResponse* response) {
  std::lock_guard<std::mutex> lock(mu_);

  std::string uuid;
  do {
    uuid = RandomBytesAsString(16);
  } while (payloads_.find(uuid) != payloads_.end());

  PayloadRecord rec;
  rec.size_bytes = request->size_bytes();

  auto* d = &rec.descriptor;
  d->set_uuid(uuid);
  const Tier tier = request->preferred_tier() == Tier::TIER_UNSPECIFIED ? Tier::TIER_RAM
                                                                         : request->preferred_tier();
  d->set_tier(tier);
  d->set_state(PayloadState::STATE_ALLOCATED);
  d->set_version(1);
  *d->mutable_created_at() = ToTimestamp(Now());

  if (request->ttl_ms() > 0) {
    *d->mutable_expires_at() = ToTimestamp(Now() + std::chrono::milliseconds(request->ttl_ms()));
  }

  if (request->has_eviction_policy()) {
    *d->mutable_eviction_policy() = request->eviction_policy();
  } else if (request->persist()) {
    EvictionPolicy policy;
    policy.set_require_durable(true);
    policy.set_spill_target(Tier::TIER_DISK);
    *d->mutable_eviction_policy() = policy;
  }

  PopulateLocation(*d, rec.size_bytes);
  payloads_.emplace(uuid, rec);

  *response->mutable_payload_descriptor() = *d;
  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::CommitPayload(
    grpc::ServerContext*, const payload::manager::v1::CommitPayloadRequest* request,
    payload::manager::v1::CommitPayloadResponse* response) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* rec = FindPayloadOrNull(request->uuid());
  if (rec == nullptr) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "payload not found");
  }

  rec->descriptor.set_state(PayloadState::STATE_ACTIVE);
  rec->descriptor.set_version(rec->descriptor.version() + 1);
  *response->mutable_payload_descriptor() = rec->descriptor;
  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::Resolve(
    grpc::ServerContext*, const payload::manager::v1::ResolveRequest* request,
    payload::manager::v1::ResolveResponse* response) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* rec = FindPayloadOrNull(request->uuid());
  if (rec == nullptr) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "payload not found");
  }

  *response->mutable_payload_descriptor() = rec->descriptor;
  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::BatchResolve(
    grpc::ServerContext*, const payload::manager::v1::BatchResolveRequest* request,
    payload::manager::v1::BatchResolveResponse* response) {
  std::lock_guard<std::mutex> lock(mu_);

  for (const auto& uuid : request->uuids()) {
    auto* rec = FindPayloadOrNull(uuid);
    if (rec != nullptr) {
      *response->add_descriptors() = rec->descriptor;
    }
  }

  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::Acquire(
    grpc::ServerContext*, const payload::manager::v1::AcquireRequest* request,
    payload::manager::v1::AcquireResponse* response) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* rec = FindPayloadOrNull(request->uuid());
  if (rec == nullptr) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "payload not found");
  }

  if (rec->descriptor.state() != PayloadState::STATE_ACTIVE) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "payload must be active before acquire");
  }

  if (request->min_tier() != Tier::TIER_UNSPECIFIED && rec->descriptor.tier() < request->min_tier()) {
    rec->descriptor.set_tier(request->min_tier());
    PopulateLocation(rec->descriptor, rec->size_bytes);
    rec->descriptor.set_version(rec->descriptor.version() + 1);
  }

  const uint64_t min_ms = request->min_lease_duration_ms() == 0 ? 60000 : request->min_lease_duration_ms();
  const auto expires_at = Now() + std::chrono::milliseconds(min_ms);
  const std::string lease_id = RandomTokenHex(16);

  leases_[lease_id] = LeaseInfo{rec->descriptor.uuid(), expires_at};

  *response->mutable_payload_descriptor() = rec->descriptor;
  response->set_lease_id(lease_id);
  *response->mutable_lease_expires_at() = ToTimestamp(expires_at);
  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::Release(
    grpc::ServerContext*, const payload::manager::v1::ReleaseRequest* request,
    google::protobuf::Empty*) {
  std::lock_guard<std::mutex> lock(mu_);
  const auto erased = leases_.erase(request->lease_id());
  if (erased == 0) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "lease not found");
  }

  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::Promote(
    grpc::ServerContext*, const payload::manager::v1::PromoteRequest* request,
    payload::manager::v1::PromoteResponse* response) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* rec = FindPayloadOrNull(request->uuid());
  if (rec == nullptr) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "payload not found");
  }

  if (request->target_tier() == Tier::TIER_UNSPECIFIED) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "target tier is required");
  }

  rec->descriptor.set_tier(request->target_tier());
  PopulateLocation(rec->descriptor, rec->size_bytes);
  rec->descriptor.set_version(rec->descriptor.version() + 1);

  *response->mutable_payload_descriptor() = rec->descriptor;
  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::Spill(
    grpc::ServerContext*, const payload::manager::v1::SpillRequest* request,
    payload::manager::v1::SpillResponse* response) {
  std::lock_guard<std::mutex> lock(mu_);

  for (const auto& uuid : request->uuids()) {
    auto* result = response->add_results();
    result->set_uuid(uuid);

    auto* rec = FindPayloadOrNull(uuid);
    if (rec == nullptr) {
      result->set_ok(false);
      result->set_error_message("payload not found");
      continue;
    }

    rec->descriptor.set_tier(Tier::TIER_DISK);
    PopulateLocation(rec->descriptor, rec->size_bytes);
    rec->descriptor.set_version(rec->descriptor.version() + 1);
    *result->mutable_payload_descriptor() = rec->descriptor;
    result->set_ok(true);
  }

  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::Delete(
    grpc::ServerContext*, const payload::manager::v1::DeleteRequest* request,
    google::protobuf::Empty*) {
  std::lock_guard<std::mutex> lock(mu_);
  if (!request->force() && ActiveLeaseCount(request->uuid()) > 0) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "active leases present; set force=true to override");
  }

  payloads_.erase(request->uuid());
  children_.erase(request->uuid());
  for (auto& [_, child_edges] : children_) {
    child_edges.erase(
        std::remove_if(child_edges.begin(), child_edges.end(),
                       [&](const LineageEdge& edge) { return edge.parent_uuid() == request->uuid(); }),
        child_edges.end());
  }

  CleanupLeasesForUuid(request->uuid());
  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::AddLineage(
    grpc::ServerContext*, const payload::manager::v1::AddLineageRequest* request,
    google::protobuf::Empty*) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* rec = FindPayloadOrNull(request->child_uuid());
  if (rec == nullptr) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "child payload not found");
  }

  for (const auto& edge : request->parents()) {
    rec->parents.push_back(edge);
    children_[edge.parent_uuid()].push_back(MakeChildEdge(request->child_uuid(), edge));
  }

  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::GetLineage(
    grpc::ServerContext*, const payload::manager::v1::GetLineageRequest* request,
    payload::manager::v1::GetLineageResponse* response) {
  std::lock_guard<std::mutex> lock(mu_);

  if (request->upstream()) {
    auto* rec = FindPayloadOrNull(request->uuid());
    if (rec == nullptr) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "payload not found");
    }

    for (const auto& edge : rec->parents) {
      *response->add_edges() = edge;
    }
    return grpc::Status::OK;
  }

  const auto it = children_.find(request->uuid());
  if (it == children_.end()) {
    return grpc::Status::OK;
  }

  for (const auto& edge : it->second) {
    *response->add_edges() = edge;
  }

  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::UpdatePayloadMetadata(
    grpc::ServerContext*, const payload::manager::v1::UpdatePayloadMetadataRequest* request,
    payload::manager::v1::UpdatePayloadMetadataResponse* response) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* rec = FindPayloadOrNull(request->uuid());
  if (rec == nullptr) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "payload not found");
  }

  if (request->mode() == MetadataUpdateMode::METADATA_UPDATE_MODE_REPLACE ||
      request->mode() == MetadataUpdateMode::METADATA_UPDATE_MODE_UNSPECIFIED) {
    rec->metadata = request->metadata();
  } else {
    PayloadMetadata merged = rec->metadata;
    if (!request->metadata().json().empty()) {
      std::string combined = merged.json();
      if (!combined.empty()) {
        combined.push_back('\n');
      }
      combined += request->metadata().json();
      merged.set_json(combined);
    }
    if (!request->metadata().schema().empty()) {
      merged.set_schema(request->metadata().schema());
    }
    rec->metadata = std::move(merged);
  }

  response->set_uuid(request->uuid());
  *response->mutable_metadata() = rec->metadata;
  *response->mutable_updated_at() = ToTimestamp(Now());
  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::AppendPayloadMetadataEvent(
    grpc::ServerContext*, const payload::manager::v1::AppendPayloadMetadataEventRequest* request,
    payload::manager::v1::AppendPayloadMetadataEventResponse* response) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* rec = FindPayloadOrNull(request->uuid());
  if (rec == nullptr) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "payload not found");
  }

  PayloadMetadataEvent event;
  *event.mutable_ts() = ToTimestamp(Now());
  *event.mutable_metadata() = request->metadata();
  event.set_source(request->source());
  event.set_version(request->version());

  rec->metadata_events.push_back(event);

  response->set_uuid(request->uuid());
  *response->mutable_event_time() = event.ts();
  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::GetPayloadMetadata(
    grpc::ServerContext*, const payload::manager::v1::GetPayloadMetadataRequest* request,
    payload::manager::v1::GetPayloadMetadataResponse* response) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* rec = FindPayloadOrNull(request->uuid());
  if (rec == nullptr) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "payload not found");
  }

  response->set_uuid(request->uuid());
  *response->mutable_metadata() = rec->metadata;
  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::ListPayloadMetadataEvents(
    grpc::ServerContext*, const payload::manager::v1::ListPayloadMetadataEventsRequest* request,
    payload::manager::v1::ListPayloadMetadataEventsResponse* response) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* rec = FindPayloadOrNull(request->uuid());
  if (rec == nullptr) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "payload not found");
  }

  const bool has_start = request->has_start_time();
  const bool has_end = request->has_end_time();

  uint32_t emitted = 0;
  for (const auto& event : rec->metadata_events) {
    if (has_start && event.ts().seconds() < request->start_time().seconds()) {
      continue;
    }
    if (has_end && event.ts().seconds() > request->end_time().seconds()) {
      continue;
    }

    *response->add_events() = event;
    ++emitted;
    if (request->limit() > 0 && emitted >= request->limit()) {
      break;
    }
  }

  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::UpdateEvictionPolicy(
    grpc::ServerContext*, const payload::manager::v1::UpdateEvictionPolicyRequest* request,
    payload::manager::v1::UpdateEvictionPolicyResponse* response) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* rec = FindPayloadOrNull(request->uuid());
  if (rec == nullptr) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "payload not found");
  }

  *rec->descriptor.mutable_eviction_policy() = request->eviction_policy();
  rec->descriptor.set_version(rec->descriptor.version() + 1);
  *response->mutable_payload_descriptor() = rec->descriptor;
  *response->mutable_updated_at() = ToTimestamp(Now());
  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::Prefetch(
    grpc::ServerContext*, const payload::manager::v1::PrefetchRequest* request,
    google::protobuf::Empty*) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* rec = FindPayloadOrNull(request->uuid());
  if (rec == nullptr) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "payload not found");
  }

  if (request->target_tier() != Tier::TIER_UNSPECIFIED && rec->descriptor.tier() < request->target_tier()) {
    rec->descriptor.set_tier(request->target_tier());
    PopulateLocation(rec->descriptor, rec->size_bytes);
    rec->descriptor.set_version(rec->descriptor.version() + 1);
  }

  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::Pin(
    grpc::ServerContext*, const payload::manager::v1::PinRequest* request,
    google::protobuf::Empty*) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* rec = FindPayloadOrNull(request->uuid());
  if (rec == nullptr) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "payload not found");
  }

  rec->pin_active = true;
  rec->pin_until = Now() + std::chrono::milliseconds(request->duration_ms());
  return grpc::Status::OK;
}

grpc::Status PayloadManagerServiceImpl::Stats(
    grpc::ServerContext*, const payload::manager::v1::StatsRequest*,
    payload::manager::v1::StatsResponse* response) {
  std::lock_guard<std::mutex> lock(mu_);
  for (const auto& [_, rec] : payloads_) {
    switch (rec.descriptor.tier()) {
      case Tier::TIER_GPU:
        response->set_payloads_gpu(response->payloads_gpu() + 1);
        response->set_bytes_gpu(response->bytes_gpu() + rec.size_bytes);
        break;
      case Tier::TIER_RAM:
        response->set_payloads_ram(response->payloads_ram() + 1);
        response->set_bytes_ram(response->bytes_ram() + rec.size_bytes);
        break;
      case Tier::TIER_DISK:
        response->set_payloads_disk(response->payloads_disk() + 1);
        response->set_bytes_disk(response->bytes_disk() + rec.size_bytes);
        break;
      default:
        break;
    }
  }

  return grpc::Status::OK;
}

void PayloadManagerServiceImpl::PopulateLocation(PayloadDescriptor& descriptor, uint64_t size_bytes) {
  descriptor.clear_gpu();
  descriptor.clear_ram();
  descriptor.clear_disk();

  if (descriptor.tier() == Tier::TIER_GPU) {
    auto* gpu = descriptor.mutable_gpu();
    gpu->set_device_id(0);
    gpu->set_ipc_handle("in-memory-handle");
    gpu->set_length_bytes(size_bytes);
    return;
  }

  if (descriptor.tier() == Tier::TIER_DISK) {
    auto* disk = descriptor.mutable_disk();
    disk->set_path("/tmp/payloads/" + RandomTokenHex(8));
    disk->set_offset_bytes(0);
    disk->set_length_bytes(size_bytes);
    return;
  }

  auto* ram = descriptor.mutable_ram();
  ram->set_shm_name("/payload_manager_shm");
  ram->set_slab_id(0);
  ram->set_block_index(0);
  ram->set_length_bytes(size_bytes);
  descriptor.set_tier(Tier::TIER_RAM);
}

PayloadManagerServiceImpl::PayloadRecord* PayloadManagerServiceImpl::FindPayloadOrNull(
    const std::string& uuid) {
  if (!IsValidUuid(uuid)) {
    return nullptr;
  }

  const auto it = payloads_.find(uuid);
  if (it == payloads_.end()) {
    return nullptr;
  }

  return &it->second;
}

uint64_t PayloadManagerServiceImpl::ActiveLeaseCount(const std::string& uuid) const {
  uint64_t count = 0;
  const auto now = Now();
  for (const auto& [_, lease] : leases_) {
    if (lease.uuid == uuid && lease.expires_at > now) {
      ++count;
    }
  }
  return count;
}

void PayloadManagerServiceImpl::CleanupLeasesForUuid(const std::string& uuid) {
  for (auto it = leases_.begin(); it != leases_.end();) {
    if (it->second.uuid == uuid) {
      it = leases_.erase(it);
    } else {
      ++it;
    }
  }
}

LineageEdge PayloadManagerServiceImpl::MakeChildEdge(const std::string& child_uuid,
                                                     const LineageEdge& source) {
  LineageEdge edge;
  edge.set_parent_uuid(child_uuid);
  edge.set_operation(source.operation());
  edge.set_role(source.role());
  edge.set_parameters(source.parameters());
  return edge;
}

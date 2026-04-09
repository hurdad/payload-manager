#include "client/cpp/client.h"

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/status.h>
#ifdef PAYLOAD_CLIENT_ENABLE_OTEL
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>
#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/nostd/string_view.h>
#endif
#if PAYLOAD_CLIENT_ARROW_CUDA
#include <arrow/gpu/cuda_context.h>
#include <arrow/gpu/cuda_memory.h>
#endif
#include <fcntl.h>
#include <grpcpp/client_context.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <vector>

#include "payload/manager/v1.hpp"

namespace payload::manager::client {

#ifndef PAYLOAD_CLIENT_ARROW_CUDA
#define PAYLOAD_CLIENT_ARROW_CUDA 0
#endif

namespace {

#ifdef PAYLOAD_CLIENT_ENABLE_OTEL
class GrpcClientMetadataCarrier : public opentelemetry::context::propagation::TextMapCarrier {
 public:
  explicit GrpcClientMetadataCarrier(grpc::ClientContext& ctx) : ctx_(ctx) {
  }

  opentelemetry::nostd::string_view Get(opentelemetry::nostd::string_view) const noexcept override {
    return {};
  }

  void Set(opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) noexcept override {
    ctx_.AddMetadata(std::string(key), std::string(value));
  }

 private:
  grpc::ClientContext& ctx_;
};

void InjectTraceContext(grpc::ClientContext& ctx) {
  GrpcClientMetadataCarrier carrier(ctx);
  auto                      propagator = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
  propagator->Inject(carrier, opentelemetry::context::RuntimeContext::GetCurrent());
}
#else
void InjectTraceContext(grpc::ClientContext&) {
}
#endif

arrow::Status GrpcToArrow(const grpc::Status& status, std::string_view action) {
  if (status.ok()) {
    return arrow::Status::OK();
  }
  return arrow::Status::IOError(std::string(action), " failed: ", status.error_message());
}

arrow::Status ErrnoToArrow(std::string_view action, std::string_view target) {
  return arrow::Status::IOError(std::string(action), " on ", std::string(target), " failed: ", std::strerror(errno));
}

int HexNibble(char c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  }
  if (c >= 'a' && c <= 'f') {
    return 10 + (c - 'a');
  }
  if (c >= 'A' && c <= 'F') {
    return 10 + (c - 'A');
  }
  return -1;
}

arrow::Result<std::string> ParseUuidBytes(std::string_view uuid) {
  std::string hex;
  hex.reserve(32);
  for (char c : uuid) {
    if (c == '-') {
      continue;
    }
    const int nibble = HexNibble(c);
    if (nibble < 0) {
      return arrow::Status::Invalid("uuid contains non-hex character: ", uuid);
    }
    hex.push_back(c);
  }

  if (hex.size() != 32) {
    return arrow::Status::Invalid("uuid must contain 32 hex chars after removing dashes");
  }

  std::string bytes;
  bytes.resize(16);
  for (size_t i = 0; i < 16; ++i) {
    const int hi = HexNibble(hex[2 * i]);
    const int lo = HexNibble(hex[2 * i + 1]);
    bytes[i]     = static_cast<char>((hi << 4) | lo);
  }
  return bytes;
}

arrow::Status SetPayloadIdFromUuid(std::string_view uuid, payload::manager::v1::PayloadID* id) {
  ARROW_ASSIGN_OR_RAISE(auto value, ParseUuidBytes(uuid));
  id->set_value(value);
  return arrow::Status::OK();
}

arrow::Status ValidatePayloadIdValue(const payload::manager::v1::PayloadID& id) {
  if (id.value().size() != 16) {
    return arrow::Status::Invalid("payload_id must contain 16 bytes, got ", id.value().size());
  }
  return arrow::Status::OK();
}

class ReadOnlyMMapBuffer final : public arrow::Buffer {
 public:
  ReadOnlyMMapBuffer(const uint8_t* data, int64_t size, void* base_addr, size_t mapped_size, int fd)
      : arrow::Buffer(data, size), base_addr_(base_addr), mapped_size_(mapped_size), fd_(fd) {
  }

  ~ReadOnlyMMapBuffer() override {
    if (base_addr_ != nullptr && mapped_size_ > 0) {
      munmap(base_addr_, mapped_size_);
    }
    if (fd_ >= 0) {
      close(fd_);
    }
  }

 private:
  void*  base_addr_;
  size_t mapped_size_;
  int    fd_;
};

class MutableMMapBuffer final : public arrow::MutableBuffer {
 public:
  MutableMMapBuffer(uint8_t* data, int64_t size, void* base_addr, size_t mapped_size, int fd)
      : arrow::MutableBuffer(data, size), base_addr_(base_addr), mapped_size_(mapped_size), fd_(fd) {
  }

  ~MutableMMapBuffer() override {
    if (base_addr_ != nullptr && mapped_size_ > 0) {
      munmap(base_addr_, mapped_size_);
    }
    if (fd_ >= 0) {
      close(fd_);
    }
  }

 private:
  void*  base_addr_;
  size_t mapped_size_;
  int    fd_;
};

#if PAYLOAD_CLIENT_ARROW_CUDA
class MutableCudaIpcBuffer final : public arrow::MutableBuffer {
 public:
  explicit MutableCudaIpcBuffer(std::shared_ptr<arrow::cuda::CudaBuffer> buffer)
      // Use address() rather than mutable_data(): CudaBuffer::mutable_data() returns
      // nullptr because is_cpu_ is false, but address() always returns the raw pointer.
      : arrow::MutableBuffer(reinterpret_cast<uint8_t*>(buffer->address()), buffer->size()), buffer_(std::move(buffer)) {
  }

 private:
  std::shared_ptr<arrow::cuda::CudaBuffer> buffer_;
};

arrow::Result<std::shared_ptr<arrow::cuda::CudaBuffer>> OpenCudaIpcBuffer(const payload::manager::v1::PayloadDescriptor& descriptor) {
  const auto& gpu = descriptor.gpu();

  if (gpu.ipc_handle().empty()) {
    return arrow::Status::Invalid("payload descriptor GPU location has empty IPC handle");
  }

  // Arrow serializes CudaIpcMemHandle as [int64_t offset (8B)][CUipcMemHandle (64B)] = 72B.
  // Do not validate against sizeof(CUipcMemHandle) here; let FromBuffer() parse the format.
  ARROW_ASSIGN_OR_RAISE(auto* device_manager, arrow::cuda::CudaDeviceManager::Instance());
  ARROW_ASSIGN_OR_RAISE(auto context, device_manager->GetContext(static_cast<int>(gpu.device_id())));
  ARROW_ASSIGN_OR_RAISE(auto ipc_handle, arrow::cuda::CudaIpcMemHandle::FromBuffer(gpu.ipc_handle().data()));
  return context->OpenIpcBuffer(*ipc_handle);
}
#endif

arrow::Result<std::shared_ptr<arrow::Buffer>> MMapReadOnly(int fd, uint64_t offset, uint64_t length) {
  if (length == 0) {
    return std::make_shared<arrow::Buffer>(nullptr, 0);
  }

  const long     page_size      = sysconf(_SC_PAGESIZE);
  const uint64_t page           = page_size <= 0 ? 4096 : static_cast<uint64_t>(page_size);
  const uint64_t aligned_offset = (offset / page) * page;
  const uint64_t delta          = offset - aligned_offset;
  const size_t   map_size       = static_cast<size_t>(delta + length);

  void* base = mmap(nullptr, map_size, PROT_READ, MAP_SHARED, fd, static_cast<off_t>(aligned_offset));
  if (base == MAP_FAILED) {
    return ErrnoToArrow("mmap", "read-only region");
  }

  const auto* data = reinterpret_cast<const uint8_t*>(base) + delta;
  return std::make_shared<ReadOnlyMMapBuffer>(data, static_cast<int64_t>(length), base, map_size, fd);
}

arrow::Result<std::shared_ptr<arrow::MutableBuffer>> MMapMutable(int fd, uint64_t offset, uint64_t length) {
  if (length == 0) {
    return std::make_shared<arrow::MutableBuffer>(nullptr, 0);
  }

  const long     page_size      = sysconf(_SC_PAGESIZE);
  const uint64_t page           = page_size <= 0 ? 4096 : static_cast<uint64_t>(page_size);
  const uint64_t aligned_offset = (offset / page) * page;
  const uint64_t delta          = offset - aligned_offset;
  const size_t   map_size       = static_cast<size_t>(delta + length);

  void* base = mmap(nullptr, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, static_cast<off_t>(aligned_offset));
  if (base == MAP_FAILED) {
    return ErrnoToArrow("mmap", "writable region");
  }

  auto* data = reinterpret_cast<uint8_t*>(base) + delta;
  return std::make_shared<MutableMMapBuffer>(data, static_cast<int64_t>(length), base, map_size, fd);
}

arrow::Result<int> OpenShm(std::string_view shm_name, bool writable) {
  const int flags = writable ? O_RDWR : O_RDONLY;
  int       fd    = shm_open(std::string(shm_name).c_str(), flags, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    return ErrnoToArrow("shm_open", shm_name);
  }
  return fd;
}

// ---------------------------------------------------------------------------
// Object-tier client upload helpers
// ---------------------------------------------------------------------------

// Hex representation of a 16-byte binary UUID — used as map key.
std::string UuidBytesToHex(std::string_view bytes) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string           hex;
  hex.reserve(bytes.size() * 2);
  for (unsigned char c : bytes) {
    hex.push_back(kHex[c >> 4]);
    hex.push_back(kHex[c & 0x0fu]);
  }
  return hex;
}

// Arrow MutableBuffer that owns its heap allocation via a std::vector<uint8_t>.
// std::move on a vector transfers internal buffer ownership; the heap address
// does not change, so the pointer stored in the Arrow base class remains valid.
class VectorOwningMutableBuffer final : public arrow::MutableBuffer {
 public:
  explicit VectorOwningMutableBuffer(std::vector<uint8_t> data)
      : arrow::MutableBuffer(data.data(), static_cast<int64_t>(data.size())), owned_(std::move(data)) {
  }

 private:
  std::vector<uint8_t> owned_;
};

struct PendingObjectUpload {
  std::string                           upload_path;
  std::shared_ptr<arrow::MutableBuffer> buffer; // VectorOwningMutableBuffer
};

// Per-PayloadClient pending object-tier uploads.  Keyed by (client*, uuid_hex).
// A global singleton avoids modifying the header beyond adding the object_fs_ member.
class PendingObjectRegistry {
 public:
  static PendingObjectRegistry& Instance() {
    static PendingObjectRegistry inst;
    return inst;
  }

  void Insert(const PayloadClient* client, std::string uuid_hex, PendingObjectUpload upload) {
    std::lock_guard lock(mutex_);
    registry_[client][std::move(uuid_hex)] = std::move(upload);
  }

  std::optional<PendingObjectUpload> Pop(const PayloadClient* client, const std::string& uuid_hex) {
    std::lock_guard lock(mutex_);
    const auto      client_it = registry_.find(client);
    if (client_it == registry_.end()) {
      return std::nullopt;
    }
    const auto it = client_it->second.find(uuid_hex);
    if (it == client_it->second.end()) {
      return std::nullopt;
    }
    auto result = std::move(it->second);
    client_it->second.erase(it);
    if (client_it->second.empty()) {
      registry_.erase(client_it);
    }
    return result;
  }

 private:
  std::mutex                                                                                     mutex_;
  std::unordered_map<const PayloadClient*, std::unordered_map<std::string, PendingObjectUpload>> registry_;
};

// Upload buffer bytes to the given URI using the provided Arrow filesystem.
// If fs is null, the filesystem is derived from the URI via FileSystemFromUri
// (picks up default AWS credential chain / env vars).
arrow::Status UploadToObjectPath(const std::string& upload_uri, const std::shared_ptr<arrow::Buffer>& buffer,
                                 const std::shared_ptr<arrow::fs::FileSystem>& fs) {
  std::string                            path;
  std::shared_ptr<arrow::fs::FileSystem> effective_fs;

  if (fs) {
    // Use the pre-configured filesystem; strip the scheme prefix to get the path.
    const auto scheme_end = upload_uri.find("://");
    path                  = (scheme_end != std::string::npos) ? upload_uri.substr(scheme_end + 3) : upload_uri;
    effective_fs          = fs;
  } else {
    // Initialise S3 SDK lazily before the first URI parse to avoid SDK not initialized error.
    if (upload_uri.size() > 5 && upload_uri.substr(0, 5) == "s3://") {
      ARROW_RETURN_NOT_OK(arrow::fs::EnsureS3Initialized());
    }
    ARROW_ASSIGN_OR_RAISE(effective_fs, arrow::fs::FileSystemFromUri(upload_uri, &path));
  }

  ARROW_ASSIGN_OR_RAISE(auto out, effective_fs->OpenOutputStream(path));
  ARROW_RETURN_NOT_OK(out->Write(buffer->data(), buffer->size()));
  return out->Close();
}

// Best-effort delete of an already-uploaded object after an ImportPayload RPC failure.
void BestEffortDeleteObject(const std::string& upload_uri, const std::shared_ptr<arrow::fs::FileSystem>& fs) {
  try {
    std::string                            path;
    std::shared_ptr<arrow::fs::FileSystem> effective_fs;
    if (fs) {
      const auto scheme_end = upload_uri.find("://");
      path                  = (scheme_end != std::string::npos) ? upload_uri.substr(scheme_end + 3) : upload_uri;
      effective_fs          = fs;
    } else {
      if (arrow::fs::EnsureS3Initialized().ok()) {
        auto result = arrow::fs::FileSystemFromUri(upload_uri, &path);
        if (!result.ok()) return;
        effective_fs = *result;
      }
    }
    if (effective_fs) {
      (void)effective_fs->DeleteFile(path);
    }
  } catch (...) {
  }
}

} // namespace

PayloadClient::PayloadClient(std::shared_ptr<grpc::Channel> channel)
    : catalog_stub_(payload::manager::v1::PayloadCatalogService::NewStub(channel)),
      data_stub_(payload::manager::v1::PayloadDataService::NewStub(channel)),
      admin_stub_(payload::manager::v1::PayloadAdminService::NewStub(channel)),
      stream_stub_(payload::manager::v1::PayloadStreamService::NewStub(std::move(channel))) {
}

PayloadClient::PayloadClient(std::shared_ptr<grpc::Channel> channel, std::shared_ptr<arrow::fs::FileSystem> object_fs)
    : catalog_stub_(payload::manager::v1::PayloadCatalogService::NewStub(channel)),
      data_stub_(payload::manager::v1::PayloadDataService::NewStub(channel)),
      admin_stub_(payload::manager::v1::PayloadAdminService::NewStub(channel)),
      stream_stub_(payload::manager::v1::PayloadStreamService::NewStub(std::move(channel))),
      object_fs_(std::move(object_fs)) {
}

arrow::Result<PayloadClient::WritablePayload> PayloadClient::AllocateWritableBuffer(uint64_t size_bytes, payload::manager::v1::Tier preferred_tier,
                                                                                    uint64_t ttl_ms, bool no_evict) const {
  payload::manager::v1::AllocatePayloadRequest req;
  req.set_size_bytes(size_bytes);
  req.set_preferred_tier(preferred_tier);
  req.set_ttl_ms(ttl_ms);
  req.set_no_evict(no_evict);

  payload::manager::v1::AllocatePayloadResponse resp;
  grpc::ClientContext                           ctx;
  InjectTraceContext(ctx);

  ARROW_RETURN_NOT_OK(GrpcToArrow(catalog_stub_->AllocatePayload(&ctx, req, &resp), "AllocatePayload"));

  if (!resp.object_upload_path().empty()) {
    // Object-tier: allocate a local heap buffer; the caller writes into it.
    // CommitPayload will upload bytes to object_upload_path then call ImportPayload.
    std::vector<uint8_t> data(size_bytes, 0);
    auto                 owned_buf = std::make_shared<VectorOwningMutableBuffer>(std::move(data));
    const auto           uuid_hex  = UuidBytesToHex(resp.payload_descriptor().payload_id().value());
    PendingObjectRegistry::Instance().Insert(this, uuid_hex, PendingObjectUpload{resp.object_upload_path(), owned_buf});
    return WritablePayload{resp.payload_descriptor(), std::move(owned_buf)};
  }

  ARROW_RETURN_NOT_OK(ValidateHasLocation(resp.payload_descriptor()));
  ARROW_ASSIGN_OR_RAISE(auto buffer, OpenMutableBuffer(resp.payload_descriptor()));
  return WritablePayload{resp.payload_descriptor(), std::move(buffer)};
}

arrow::Result<payload::manager::v1::PayloadID> PayloadClient::PayloadIdFromUuid(std::string_view uuid) {
  payload::manager::v1::PayloadID id;
  ARROW_RETURN_NOT_OK(SetPayloadIdFromUuid(uuid, &id));
  return id;
}

arrow::Status PayloadClient::ValidatePayloadId(const payload::manager::v1::PayloadID& payload_id) {
  return ValidatePayloadIdValue(payload_id);
}

arrow::Status PayloadClient::CommitPayload(const payload::manager::v1::PayloadID& payload_id) const {
  ARROW_RETURN_NOT_OK(ValidatePayloadIdValue(payload_id));

  const auto uuid_hex = UuidBytesToHex(payload_id.value());
  auto       pending  = PendingObjectRegistry::Instance().Pop(this, uuid_hex);

  if (pending.has_value()) {
    // Phase 1: upload bytes directly to object storage — no bytes via gRPC.
    ARROW_RETURN_NOT_OK(UploadToObjectPath(pending->upload_path, pending->buffer, object_fs_));

    // Phase 2: transfer ownership to the manager.
    payload::manager::v1::ImportPayloadRequest import_req;
    *import_req.mutable_id() = payload_id;
    import_req.set_size_bytes(static_cast<uint64_t>(pending->buffer->size()));

    payload::manager::v1::ImportPayloadResponse import_resp;
    grpc::ClientContext                         import_ctx;
    InjectTraceContext(import_ctx);

    auto rpc_status = GrpcToArrow(catalog_stub_->ImportPayload(&import_ctx, import_req, &import_resp), "ImportPayload");
    if (!rpc_status.ok()) {
      BestEffortDeleteObject(pending->upload_path, object_fs_);
      return rpc_status;
    }
    return arrow::Status::OK();
  }

  // Normal path for RAM / DISK / GPU payloads.
  payload::manager::v1::CommitPayloadRequest req;
  *req.mutable_id() = payload_id;

  payload::manager::v1::CommitPayloadResponse resp;
  grpc::ClientContext                         ctx;
  InjectTraceContext(ctx);

  return GrpcToArrow(catalog_stub_->CommitPayload(&ctx, req, &resp), "CommitPayload");
}

arrow::Result<payload::manager::v1::ResolveSnapshotResponse> PayloadClient::Resolve(const payload::manager::v1::PayloadID& payload_id) const {
  payload::manager::v1::ResolveSnapshotRequest request;
  ARROW_RETURN_NOT_OK(ValidatePayloadIdValue(payload_id));
  *request.mutable_id() = payload_id;

  payload::manager::v1::ResolveSnapshotResponse response;
  grpc::ClientContext                           ctx;
  InjectTraceContext(ctx);

  ARROW_RETURN_NOT_OK(GrpcToArrow(data_stub_->ResolveSnapshot(&ctx, request, &response), "ResolveSnapshot"));
  return response;
}

arrow::Result<PayloadClient::ReadablePayload> PayloadClient::AcquireReadableBuffer(const payload::manager::v1::PayloadID& payload_id,
                                                                                   payload::manager::v1::Tier             min_tier,
                                                                                   payload::manager::v1::PromotionPolicy  promotion_policy,
                                                                                   uint64_t min_lease_duration_ms) const {
  payload::manager::v1::AcquireReadLeaseRequest req;
  ARROW_RETURN_NOT_OK(ValidatePayloadIdValue(payload_id));
  *req.mutable_id() = payload_id;
  req.set_min_tier(min_tier);
  req.set_promotion_policy(promotion_policy);
  req.set_min_lease_duration_ms(min_lease_duration_ms);
  req.set_mode(payload::manager::v1::LEASE_MODE_READ);

  payload::manager::v1::AcquireReadLeaseResponse resp;
  grpc::ClientContext                            ctx;
  InjectTraceContext(ctx);

  ARROW_RETURN_NOT_OK(GrpcToArrow(data_stub_->AcquireReadLease(&ctx, req, &resp), "AcquireReadLease"));
  ARROW_RETURN_NOT_OK(ValidateHasLocation(resp.payload_descriptor()));

  ARROW_ASSIGN_OR_RAISE(auto buffer, OpenReadableBuffer(resp.payload_descriptor()));
  return ReadablePayload{resp.payload_descriptor(), resp.lease_id(), std::move(buffer)};
}

arrow::Status PayloadClient::Release(const payload::manager::v1::LeaseID& lease_id) const {
  payload::manager::v1::ReleaseLeaseRequest req;
  *req.mutable_lease_id() = lease_id;

  google::protobuf::Empty resp;
  grpc::ClientContext     ctx;
  InjectTraceContext(ctx);

  return GrpcToArrow(data_stub_->ReleaseLease(&ctx, req, &resp), "ReleaseLease");
}

arrow::Result<payload::manager::v1::PromoteResponse> PayloadClient::Promote(const payload::manager::v1::PromoteRequest& request) const {
  payload::manager::v1::PromoteResponse response;
  grpc::ClientContext                   ctx;
  InjectTraceContext(ctx);

  ARROW_RETURN_NOT_OK(GrpcToArrow(catalog_stub_->Promote(&ctx, request, &response), "Promote"));
  return response;
}

arrow::Result<payload::manager::v1::SpillResponse> PayloadClient::Spill(const payload::manager::v1::SpillRequest& request) const {
  payload::manager::v1::SpillResponse response;
  grpc::ClientContext                 ctx;
  InjectTraceContext(ctx);

  ARROW_RETURN_NOT_OK(GrpcToArrow(catalog_stub_->Spill(&ctx, request, &response), "Spill"));
  return response;
}

arrow::Status PayloadClient::Prefetch(const payload::manager::v1::PrefetchRequest& request) const {
  google::protobuf::Empty response;
  grpc::ClientContext     ctx;

  InjectTraceContext(ctx);
  return GrpcToArrow(catalog_stub_->Prefetch(&ctx, request, &response), "Prefetch");
}

arrow::Status PayloadClient::Pin(const payload::manager::v1::PinRequest& request) const {
  google::protobuf::Empty response;
  grpc::ClientContext     ctx;

  InjectTraceContext(ctx);
  return GrpcToArrow(catalog_stub_->Pin(&ctx, request, &response), "Pin");
}

arrow::Status PayloadClient::Unpin(const payload::manager::v1::UnpinRequest& request) const {
  google::protobuf::Empty response;
  grpc::ClientContext     ctx;

  InjectTraceContext(ctx);
  return GrpcToArrow(catalog_stub_->Unpin(&ctx, request, &response), "Unpin");
}

arrow::Status PayloadClient::Delete(const payload::manager::v1::DeleteRequest& request) const {
  google::protobuf::Empty response;
  grpc::ClientContext     ctx;

  InjectTraceContext(ctx);
  return GrpcToArrow(catalog_stub_->Delete(&ctx, request, &response), "Delete");
}

arrow::Status PayloadClient::AddLineage(const payload::manager::v1::AddLineageRequest& request) const {
  google::protobuf::Empty response;
  grpc::ClientContext     ctx;

  InjectTraceContext(ctx);
  return GrpcToArrow(catalog_stub_->AddLineage(&ctx, request, &response), "AddLineage");
}

arrow::Result<payload::manager::v1::GetLineageResponse> PayloadClient::GetLineage(const payload::manager::v1::GetLineageRequest& request) const {
  payload::manager::v1::GetLineageResponse response;
  grpc::ClientContext                      ctx;
  InjectTraceContext(ctx);

  ARROW_RETURN_NOT_OK(GrpcToArrow(catalog_stub_->GetLineage(&ctx, request, &response), "GetLineage"));
  return response;
}

arrow::Result<payload::manager::v1::UpdatePayloadMetadataResponse> PayloadClient::UpdatePayloadMetadata(
    const payload::manager::v1::UpdatePayloadMetadataRequest& request) const {
  payload::manager::v1::UpdatePayloadMetadataResponse response;
  grpc::ClientContext                                 ctx;
  InjectTraceContext(ctx);

  ARROW_RETURN_NOT_OK(GrpcToArrow(catalog_stub_->UpdatePayloadMetadata(&ctx, request, &response), "UpdatePayloadMetadata"));
  return response;
}

arrow::Result<payload::manager::v1::AppendPayloadMetadataEventResponse> PayloadClient::AppendPayloadMetadataEvent(
    const payload::manager::v1::AppendPayloadMetadataEventRequest& request) const {
  payload::manager::v1::AppendPayloadMetadataEventResponse response;
  grpc::ClientContext                                      ctx;
  InjectTraceContext(ctx);

  ARROW_RETURN_NOT_OK(GrpcToArrow(catalog_stub_->AppendPayloadMetadataEvent(&ctx, request, &response), "AppendPayloadMetadataEvent"));
  return response;
}

arrow::Result<payload::manager::v1::ListPayloadsResponse> PayloadClient::ListPayloads(
    const payload::manager::v1::ListPayloadsRequest& request) const {
  payload::manager::v1::ListPayloadsResponse response;
  grpc::ClientContext                        ctx;
  InjectTraceContext(ctx);

  ARROW_RETURN_NOT_OK(GrpcToArrow(catalog_stub_->ListPayloads(&ctx, request, &response), "ListPayloads"));
  return response;
}

arrow::Result<payload::manager::v1::StatsResponse> PayloadClient::Stats(const payload::manager::v1::StatsRequest& request) const {
  payload::manager::v1::StatsResponse response;
  grpc::ClientContext                 ctx;
  InjectTraceContext(ctx);

  ARROW_RETURN_NOT_OK(GrpcToArrow(admin_stub_->Stats(&ctx, request, &response), "Stats"));
  return response;
}

arrow::Status PayloadClient::CreateStream(const payload::manager::v1::CreateStreamRequest& request) const {
  google::protobuf::Empty response;
  grpc::ClientContext     ctx;

  InjectTraceContext(ctx);
  return GrpcToArrow(stream_stub_->CreateStream(&ctx, request, &response), "CreateStream");
}

arrow::Status PayloadClient::DeleteStream(const payload::manager::v1::DeleteStreamRequest& request) const {
  google::protobuf::Empty response;
  grpc::ClientContext     ctx;

  InjectTraceContext(ctx);
  return GrpcToArrow(stream_stub_->DeleteStream(&ctx, request, &response), "DeleteStream");
}

arrow::Result<payload::manager::v1::AppendResponse> PayloadClient::Append(const payload::manager::v1::AppendRequest& request) const {
  payload::manager::v1::AppendResponse response;
  grpc::ClientContext                  ctx;
  InjectTraceContext(ctx);

  ARROW_RETURN_NOT_OK(GrpcToArrow(stream_stub_->Append(&ctx, request, &response), "Append"));
  return response;
}

arrow::Result<payload::manager::v1::ReadResponse> PayloadClient::Read(const payload::manager::v1::ReadRequest& request) const {
  payload::manager::v1::ReadResponse response;
  grpc::ClientContext                ctx;
  InjectTraceContext(ctx);

  ARROW_RETURN_NOT_OK(GrpcToArrow(stream_stub_->Read(&ctx, request, &response), "Read"));
  return response;
}

std::unique_ptr<grpc::ClientReader<payload::manager::v1::SubscribeResponse>> PayloadClient::Subscribe(
    const payload::manager::v1::SubscribeRequest& request, grpc::ClientContext* context) const {
  InjectTraceContext(*context);
  return stream_stub_->Subscribe(context, request);
}

arrow::Status PayloadClient::Commit(const payload::manager::v1::CommitRequest& request) const {
  google::protobuf::Empty response;
  grpc::ClientContext     ctx;

  InjectTraceContext(ctx);
  return GrpcToArrow(stream_stub_->Commit(&ctx, request, &response), "Commit");
}

arrow::Result<payload::manager::v1::GetCommittedResponse> PayloadClient::GetCommitted(
    const payload::manager::v1::GetCommittedRequest& request) const {
  payload::manager::v1::GetCommittedResponse response;
  grpc::ClientContext                        ctx;
  InjectTraceContext(ctx);

  ARROW_RETURN_NOT_OK(GrpcToArrow(stream_stub_->GetCommitted(&ctx, request, &response), "GetCommitted"));
  return response;
}

arrow::Result<payload::manager::v1::GetRangeResponse> PayloadClient::GetRange(const payload::manager::v1::GetRangeRequest& request) const {
  payload::manager::v1::GetRangeResponse response;
  grpc::ClientContext                    ctx;
  InjectTraceContext(ctx);

  ARROW_RETURN_NOT_OK(GrpcToArrow(stream_stub_->GetRange(&ctx, request, &response), "GetRange"));
  return response;
}

arrow::Result<std::shared_ptr<arrow::MutableBuffer>> PayloadClient::OpenMutableBuffer(
    const payload::manager::v1::PayloadDescriptor& descriptor) const {
  const uint64_t length = DescriptorLengthBytes(descriptor);

  if (descriptor.has_gpu()) {
#if PAYLOAD_CLIENT_ARROW_CUDA
    ARROW_ASSIGN_OR_RAISE(auto cuda_buffer, OpenCudaIpcBuffer(descriptor));
    return std::make_shared<MutableCudaIpcBuffer>(std::move(cuda_buffer));
#else
    return arrow::Status::NotImplemented(
        "Payload descriptor tier GPU is not available because this client was built without CUDA support (PAYLOAD_CLIENT_ARROW_CUDA=0)");
#endif
  }

  if (descriptor.has_ram()) {
    ARROW_ASSIGN_OR_RAISE(int fd, OpenShm(descriptor.ram().shm_name(), true));
    if (ftruncate(fd, static_cast<off_t>(length)) != 0) {
      close(fd);
      return ErrnoToArrow("ftruncate", descriptor.ram().shm_name());
    }
    return MMapMutable(fd, 0, length);
  }

  if (descriptor.has_disk()) {
    int fd = open(descriptor.disk().path().c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd < 0) {
      return ErrnoToArrow("open", descriptor.disk().path());
    }

    const uint64_t end = descriptor.disk().offset_bytes() + length;
    if (ftruncate(fd, static_cast<off_t>(end)) != 0) {
      close(fd);
      return ErrnoToArrow("ftruncate", descriptor.disk().path());
    }

    return MMapMutable(fd, descriptor.disk().offset_bytes(), length);
  }

  return arrow::Status::NotImplemented("Writable Arrow buffer for tier ", payload::manager::v1::Tier_Name(descriptor.tier()),
                                       " is not supported in C++ client");
}

arrow::Result<std::shared_ptr<arrow::Buffer>> PayloadClient::OpenReadableBuffer(const payload::manager::v1::PayloadDescriptor& descriptor) const {
  const uint64_t length = DescriptorLengthBytes(descriptor);

  if (descriptor.has_gpu()) {
#if PAYLOAD_CLIENT_ARROW_CUDA
    ARROW_ASSIGN_OR_RAISE(auto cuda_buffer, OpenCudaIpcBuffer(descriptor));
    return std::static_pointer_cast<arrow::Buffer>(std::move(cuda_buffer));
#else
    return arrow::Status::NotImplemented(
        "Payload descriptor tier GPU is not available because this client was built without CUDA support (PAYLOAD_CLIENT_ARROW_CUDA=0)");
#endif
  }

  if (descriptor.has_ram()) {
    ARROW_ASSIGN_OR_RAISE(int fd, OpenShm(descriptor.ram().shm_name(), false));
    return MMapReadOnly(fd, 0, length);
  }

  if (descriptor.has_disk()) {
    int fd = open(descriptor.disk().path().c_str(), O_RDONLY);
    if (fd < 0) {
      return ErrnoToArrow("open", descriptor.disk().path());
    }
    return MMapReadOnly(fd, descriptor.disk().offset_bytes(), length);
  }

  return arrow::Status::NotImplemented("Readable Arrow buffer for tier ", payload::manager::v1::Tier_Name(descriptor.tier()),
                                       " is not supported in C++ client");
}

arrow::Status PayloadClient::ValidateHasLocation(const payload::manager::v1::PayloadDescriptor& descriptor) {
  if (descriptor.has_ram() || descriptor.has_disk()) {
    return arrow::Status::OK();
  }

  if (descriptor.has_gpu()) {
#if PAYLOAD_CLIENT_ARROW_CUDA
    return arrow::Status::OK();
#else
    return arrow::Status::Invalid(
        "payload descriptor requested GPU tier, but this client was built without CUDA support (PAYLOAD_CLIENT_ARROW_CUDA=0)");
#endif
  }

  return arrow::Status::Invalid("payload descriptor is missing location for tier ", payload::manager::v1::Tier_Name(descriptor.tier()));
}

uint64_t PayloadClient::DescriptorLengthBytes(const payload::manager::v1::PayloadDescriptor& descriptor) {
  if (descriptor.has_gpu()) {
    return descriptor.gpu().length_bytes();
  }
  if (descriptor.has_ram()) {
    return descriptor.ram().length_bytes();
  }
  if (descriptor.has_disk()) {
    return descriptor.disk().length_bytes();
  }
  return 0;
}

} // namespace payload::manager::client

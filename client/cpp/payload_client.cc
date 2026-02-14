#include "client/cpp/payload_client.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>

#include <arrow/status.h>
#include <grpcpp/client_context.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "payload/manager/v1.hpp"

namespace payload::manager::client {

namespace {

arrow::Status GrpcToArrow(const grpc::Status& status, std::string_view action) {
  if (status.ok()) {
    return arrow::Status::OK();
  }
  return arrow::Status::IOError(std::string(action), " failed: ", status.error_message());
}

arrow::Status ErrnoToArrow(std::string_view action, std::string_view target) {
  return arrow::Status::IOError(std::string(action), " on ", std::string(target), " failed: ",
                                std::strerror(errno));
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
    bytes[i] = static_cast<char>((hi << 4) | lo);
  }
  return bytes;
}

arrow::Status SetPayloadIdFromUuid(std::string_view uuid, payload::manager::v1::PayloadID* id) {
  ARROW_ASSIGN_OR_RAISE(auto value, ParseUuidBytes(uuid));
  id->set_value(value);
  return arrow::Status::OK();
}

class ReadOnlyMMapBuffer final : public arrow::Buffer {
 public:
  ReadOnlyMMapBuffer(const uint8_t* data, int64_t size, void* base_addr, size_t mapped_size, int fd)
      : arrow::Buffer(data, size), base_addr_(base_addr), mapped_size_(mapped_size), fd_(fd) {}

  ~ReadOnlyMMapBuffer() override {
    if (base_addr_ != nullptr && mapped_size_ > 0) {
      munmap(base_addr_, mapped_size_);
    }
    if (fd_ >= 0) {
      close(fd_);
    }
  }

 private:
  void* base_addr_;
  size_t mapped_size_;
  int fd_;
};

class MutableMMapBuffer final : public arrow::MutableBuffer {
 public:
  MutableMMapBuffer(uint8_t* data, int64_t size, void* base_addr, size_t mapped_size, int fd)
      : arrow::MutableBuffer(data, size), base_addr_(base_addr), mapped_size_(mapped_size), fd_(fd) {}

  ~MutableMMapBuffer() override {
    if (base_addr_ != nullptr && mapped_size_ > 0) {
      munmap(base_addr_, mapped_size_);
    }
    if (fd_ >= 0) {
      close(fd_);
    }
  }

 private:
  void* base_addr_;
  size_t mapped_size_;
  int fd_;
};

arrow::Result<std::shared_ptr<arrow::Buffer>> MMapReadOnly(int fd, uint64_t offset, uint64_t length) {
  if (length == 0) {
    return std::make_shared<arrow::Buffer>(nullptr, 0);
  }

  const long page_size = sysconf(_SC_PAGESIZE);
  const uint64_t page = page_size <= 0 ? 4096 : static_cast<uint64_t>(page_size);
  const uint64_t aligned_offset = (offset / page) * page;
  const uint64_t delta = offset - aligned_offset;
  const size_t map_size = static_cast<size_t>(delta + length);

  void* base = mmap(nullptr, map_size, PROT_READ, MAP_SHARED, fd, static_cast<off_t>(aligned_offset));
  if (base == MAP_FAILED) {
    return ErrnoToArrow("mmap", "read-only region");
  }

  const auto* data = reinterpret_cast<const uint8_t*>(base) + delta;
  return std::make_shared<ReadOnlyMMapBuffer>(data, static_cast<int64_t>(length), base, map_size, fd);
}

arrow::Result<std::shared_ptr<arrow::MutableBuffer>> MMapMutable(int fd, uint64_t offset,
                                                                  uint64_t length) {
  if (length == 0) {
    return std::make_shared<arrow::MutableBuffer>(nullptr, 0);
  }

  const long page_size = sysconf(_SC_PAGESIZE);
  const uint64_t page = page_size <= 0 ? 4096 : static_cast<uint64_t>(page_size);
  const uint64_t aligned_offset = (offset / page) * page;
  const uint64_t delta = offset - aligned_offset;
  const size_t map_size = static_cast<size_t>(delta + length);

  void* base = mmap(nullptr, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
                    static_cast<off_t>(aligned_offset));
  if (base == MAP_FAILED) {
    return ErrnoToArrow("mmap", "writable region");
  }

  auto* data = reinterpret_cast<uint8_t*>(base) + delta;
  return std::make_shared<MutableMMapBuffer>(data, static_cast<int64_t>(length), base, map_size, fd);
}

arrow::Result<int> OpenShm(std::string_view shm_name, bool writable) {
  const int flags = writable ? O_RDWR : O_RDONLY;
  int fd = shm_open(std::string(shm_name).c_str(), flags, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    return ErrnoToArrow("shm_open", shm_name);
  }
  return fd;
}

}  // namespace

PayloadClient::PayloadClient(std::shared_ptr<grpc::Channel> channel)
    : catalog_stub_(payload::manager::v1::PayloadCatalogService::NewStub(channel)),
      data_stub_(payload::manager::v1::PayloadDataService::NewStub(channel)),
      admin_stub_(payload::manager::v1::PayloadAdminService::NewStub(channel)),
      stream_stub_(payload::manager::v1::PayloadStreamService::NewStub(std::move(channel))) {}

arrow::Result<PayloadClient::WritablePayload> PayloadClient::AllocateWritableBuffer(
    uint64_t size_bytes, payload::manager::v1::Tier preferred_tier, uint64_t ttl_ms,
    bool persist) const {
  payload::manager::v1::AllocatePayloadRequest req;
  req.set_size_bytes(size_bytes);
  req.set_preferred_tier(preferred_tier);
  req.set_ttl_ms(ttl_ms);
  req.set_persist(persist);

  payload::manager::v1::AllocatePayloadResponse resp;
  grpc::ClientContext ctx;

  ARROW_RETURN_NOT_OK(GrpcToArrow(catalog_stub_->AllocatePayload(&ctx, req, &resp), "AllocatePayload"));
  ARROW_RETURN_NOT_OK(ValidateHasLocation(resp.payload_descriptor()));

  ARROW_ASSIGN_OR_RAISE(auto buffer, OpenMutableBuffer(resp.payload_descriptor()));
  return WritablePayload{resp.payload_descriptor(), std::move(buffer)};
}

arrow::Status PayloadClient::CommitPayload(const std::string& uuid) const {
  payload::manager::v1::CommitPayloadRequest req;
  ARROW_RETURN_NOT_OK(SetPayloadIdFromUuid(uuid, req.mutable_id()));

  payload::manager::v1::CommitPayloadResponse resp;
  grpc::ClientContext ctx;

  return GrpcToArrow(catalog_stub_->CommitPayload(&ctx, req, &resp), "CommitPayload");
}

arrow::Result<payload::manager::v1::ResolveSnapshotResponse> PayloadClient::Resolve(
    const std::string& uuid) const {
  payload::manager::v1::ResolveSnapshotRequest request;
  ARROW_RETURN_NOT_OK(SetPayloadIdFromUuid(uuid, request.mutable_id()));

  payload::manager::v1::ResolveSnapshotResponse response;
  grpc::ClientContext ctx;

  ARROW_RETURN_NOT_OK(GrpcToArrow(data_stub_->ResolveSnapshot(&ctx, request, &response), "ResolveSnapshot"));
  return response;
}

arrow::Result<PayloadClient::ReadablePayload> PayloadClient::AcquireReadableBuffer(
    const std::string& uuid, payload::manager::v1::Tier min_tier,
    payload::manager::v1::PromotionPolicy promotion_policy, uint64_t min_lease_duration_ms) const {
  payload::manager::v1::AcquireReadLeaseRequest req;
  ARROW_RETURN_NOT_OK(SetPayloadIdFromUuid(uuid, req.mutable_id()));
  req.set_min_tier(min_tier);
  req.set_promotion_policy(promotion_policy);
  req.set_min_lease_duration_ms(min_lease_duration_ms);
  req.set_mode(payload::manager::v1::LEASE_MODE_READ);

  payload::manager::v1::AcquireReadLeaseResponse resp;
  grpc::ClientContext ctx;

  ARROW_RETURN_NOT_OK(GrpcToArrow(data_stub_->AcquireReadLease(&ctx, req, &resp), "AcquireReadLease"));
  ARROW_RETURN_NOT_OK(ValidateHasLocation(resp.payload_descriptor()));

  ARROW_ASSIGN_OR_RAISE(auto buffer, OpenReadableBuffer(resp.payload_descriptor()));
  return ReadablePayload{resp.payload_descriptor(), resp.lease_id(), std::move(buffer)};
}

arrow::Status PayloadClient::Release(const std::string& lease_id) const {
  payload::manager::v1::ReleaseLeaseRequest req;
  req.set_lease_id(lease_id);

  google::protobuf::Empty resp;
  grpc::ClientContext ctx;

  return GrpcToArrow(data_stub_->ReleaseLease(&ctx, req, &resp), "ReleaseLease");
}

arrow::Result<payload::manager::v1::PromoteResponse> PayloadClient::Promote(
    const payload::manager::v1::PromoteRequest& request) const {
  payload::manager::v1::PromoteResponse response;
  grpc::ClientContext ctx;

  ARROW_RETURN_NOT_OK(GrpcToArrow(catalog_stub_->Promote(&ctx, request, &response), "Promote"));
  return response;
}

arrow::Result<payload::manager::v1::SpillResponse> PayloadClient::Spill(
    const payload::manager::v1::SpillRequest& request) const {
  payload::manager::v1::SpillResponse response;
  grpc::ClientContext ctx;

  ARROW_RETURN_NOT_OK(GrpcToArrow(catalog_stub_->Spill(&ctx, request, &response), "Spill"));
  return response;
}

arrow::Status PayloadClient::Delete(const payload::manager::v1::DeleteRequest& request) const {
  google::protobuf::Empty response;
  grpc::ClientContext ctx;

  return GrpcToArrow(catalog_stub_->Delete(&ctx, request, &response), "Delete");
}

arrow::Status PayloadClient::AddLineage(const payload::manager::v1::AddLineageRequest& request) const {
  google::protobuf::Empty response;
  grpc::ClientContext ctx;

  return GrpcToArrow(catalog_stub_->AddLineage(&ctx, request, &response), "AddLineage");
}

arrow::Result<payload::manager::v1::GetLineageResponse> PayloadClient::GetLineage(
    const payload::manager::v1::GetLineageRequest& request) const {
  payload::manager::v1::GetLineageResponse response;
  grpc::ClientContext ctx;

  ARROW_RETURN_NOT_OK(GrpcToArrow(catalog_stub_->GetLineage(&ctx, request, &response), "GetLineage"));
  return response;
}

arrow::Result<payload::manager::v1::UpdatePayloadMetadataResponse> PayloadClient::UpdatePayloadMetadata(
    const payload::manager::v1::UpdatePayloadMetadataRequest& request) const {
  payload::manager::v1::UpdatePayloadMetadataResponse response;
  grpc::ClientContext ctx;

  ARROW_RETURN_NOT_OK(
      GrpcToArrow(catalog_stub_->UpdatePayloadMetadata(&ctx, request, &response), "UpdatePayloadMetadata"));
  return response;
}

arrow::Result<payload::manager::v1::AppendPayloadMetadataEventResponse>
PayloadClient::AppendPayloadMetadataEvent(
    const payload::manager::v1::AppendPayloadMetadataEventRequest& request) const {
  payload::manager::v1::AppendPayloadMetadataEventResponse response;
  grpc::ClientContext ctx;

  ARROW_RETURN_NOT_OK(GrpcToArrow(catalog_stub_->AppendPayloadMetadataEvent(&ctx, request, &response),
                                  "AppendPayloadMetadataEvent"));
  return response;
}

arrow::Result<payload::manager::v1::StatsResponse> PayloadClient::Stats(
    const payload::manager::v1::StatsRequest& request) const {
  payload::manager::v1::StatsResponse response;
  grpc::ClientContext ctx;

  ARROW_RETURN_NOT_OK(GrpcToArrow(admin_stub_->Stats(&ctx, request, &response), "Stats"));
  return response;
}

arrow::Status PayloadClient::CreateStream(
    const payload::manager::v1::CreateStreamRequest& request) const {
  google::protobuf::Empty response;
  grpc::ClientContext ctx;

  return GrpcToArrow(stream_stub_->CreateStream(&ctx, request, &response), "CreateStream");
}

arrow::Status PayloadClient::DeleteStream(
    const payload::manager::v1::DeleteStreamRequest& request) const {
  google::protobuf::Empty response;
  grpc::ClientContext ctx;

  return GrpcToArrow(stream_stub_->DeleteStream(&ctx, request, &response), "DeleteStream");
}

arrow::Result<payload::manager::v1::AppendResponse> PayloadClient::Append(
    const payload::manager::v1::AppendRequest& request) const {
  payload::manager::v1::AppendResponse response;
  grpc::ClientContext ctx;

  ARROW_RETURN_NOT_OK(GrpcToArrow(stream_stub_->Append(&ctx, request, &response), "Append"));
  return response;
}

arrow::Result<payload::manager::v1::ReadResponse> PayloadClient::Read(
    const payload::manager::v1::ReadRequest& request) const {
  payload::manager::v1::ReadResponse response;
  grpc::ClientContext ctx;

  ARROW_RETURN_NOT_OK(GrpcToArrow(stream_stub_->Read(&ctx, request, &response), "Read"));
  return response;
}

std::unique_ptr<grpc::ClientReader<payload::manager::v1::SubscribeResponse>>
PayloadClient::Subscribe(const payload::manager::v1::SubscribeRequest& request,
                         grpc::ClientContext* context) const {
  return stream_stub_->Subscribe(context, request);
}

arrow::Status PayloadClient::Commit(const payload::manager::v1::CommitRequest& request) const {
  google::protobuf::Empty response;
  grpc::ClientContext ctx;

  return GrpcToArrow(stream_stub_->Commit(&ctx, request, &response), "Commit");
}

arrow::Result<payload::manager::v1::GetCommittedResponse> PayloadClient::GetCommitted(
    const payload::manager::v1::GetCommittedRequest& request) const {
  payload::manager::v1::GetCommittedResponse response;
  grpc::ClientContext ctx;

  ARROW_RETURN_NOT_OK(
      GrpcToArrow(stream_stub_->GetCommitted(&ctx, request, &response), "GetCommitted"));
  return response;
}

arrow::Result<payload::manager::v1::GetRangeResponse> PayloadClient::GetRange(
    const payload::manager::v1::GetRangeRequest& request) const {
  payload::manager::v1::GetRangeResponse response;
  grpc::ClientContext ctx;

  ARROW_RETURN_NOT_OK(GrpcToArrow(stream_stub_->GetRange(&ctx, request, &response), "GetRange"));
  return response;
}

arrow::Result<std::shared_ptr<arrow::MutableBuffer>> PayloadClient::OpenMutableBuffer(
    const payload::manager::v1::PayloadDescriptor& descriptor) const {
  const uint64_t length = DescriptorLengthBytes(descriptor);

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

  return arrow::Status::NotImplemented("Writable Arrow buffer for tier ",
                                       payload::manager::v1::Tier_Name(descriptor.tier()),
                                       " is not supported in C++ client");
}

arrow::Result<std::shared_ptr<arrow::Buffer>> PayloadClient::OpenReadableBuffer(
    const payload::manager::v1::PayloadDescriptor& descriptor) const {
  const uint64_t length = DescriptorLengthBytes(descriptor);

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

  return arrow::Status::NotImplemented("Readable Arrow buffer for tier ",
                                       payload::manager::v1::Tier_Name(descriptor.tier()),
                                       " is not supported in C++ client");
}

arrow::Status PayloadClient::ValidateHasLocation(
    const payload::manager::v1::PayloadDescriptor& descriptor) {
  if (descriptor.has_gpu() || descriptor.has_ram() || descriptor.has_disk()) {
    return arrow::Status::OK();
  }
  return arrow::Status::Invalid("payload descriptor is missing location for tier ",
                                payload::manager::v1::Tier_Name(descriptor.tier()));
}

uint64_t PayloadClient::DescriptorLengthBytes(
    const payload::manager::v1::PayloadDescriptor& descriptor) {
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

}  // namespace payload::manager::client

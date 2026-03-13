#pragma once

#include <arrow/buffer.h>
#include <arrow/result.h>
#include <grpcpp/channel.h>
#include <grpcpp/support/sync_stream.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "payload/manager/services/v1/payload_admin_service.grpc.pb.h"
#include "payload/manager/services/v1/payload_catalog_service.grpc.pb.h"
#include "payload/manager/services/v1/payload_data_service.grpc.pb.h"
#include "payload/manager/services/v1/payload_stream_service.grpc.pb.h"
#include "payload/manager/v1.hpp"

namespace payload::manager::client {

/// High-level C++ client for payload-manager gRPC APIs.
///
/// This type combines catalog, data, admin, and stream RPCs behind one facade
/// and includes convenience helpers for opening Arrow buffers from returned
/// payload descriptors.
class PayloadClient {
 public:
  /// Writable allocation result produced by AllocateWritableBuffer().
  struct WritablePayload {
    /// Descriptor returned by the service for the allocated payload.
    payload::manager::v1::PayloadDescriptor descriptor;
    /// Mutable Arrow buffer mapped/opened for writing payload bytes.
    std::shared_ptr<arrow::MutableBuffer>   buffer;
  };

  /// Read lease result produced by AcquireReadableBuffer().
  struct ReadablePayload {
    /// Descriptor for the leased payload snapshot.
    payload::manager::v1::PayloadDescriptor descriptor;
    /// Lease identifier that must be released with Release().
    payload::manager::v1::LeaseID           lease_id;
    /// Read-only Arrow buffer mapped/opened for reading payload bytes.
    std::shared_ptr<arrow::Buffer>          buffer;
  };

  /// Construct a client from a shared gRPC channel.
  explicit PayloadClient(std::shared_ptr<grpc::Channel> channel);

  /// Allocate a payload and open a writable Arrow buffer for it.
  arrow::Result<WritablePayload> AllocateWritableBuffer(uint64_t                   size_bytes,
                                                        payload::manager::v1::Tier preferred_tier = payload::manager::v1::TIER_RAM,
                                                        uint64_t ttl_ms = 0, bool persist = false) const;

  /// Convert a UUID string into a protobuf PayloadID.
  static arrow::Result<payload::manager::v1::PayloadID> PayloadIdFromUuid(std::string_view uuid);
  /// Validate that a PayloadID contains a 16-byte UUID payload.
  static arrow::Status                                  ValidatePayloadId(const payload::manager::v1::PayloadID& payload_id);

  /// Mark a previously allocated payload as committed.
  arrow::Status CommitPayload(const payload::manager::v1::PayloadID& payload_id) const;

  /// Resolve payload metadata for a committed payload.
  arrow::Result<payload::manager::v1::ResolveSnapshotResponse> Resolve(const payload::manager::v1::PayloadID& payload_id) const;

  /// Acquire a read lease and open a readable Arrow buffer.
  arrow::Result<ReadablePayload> AcquireReadableBuffer(
      const payload::manager::v1::PayloadID& payload_id, payload::manager::v1::Tier min_tier = payload::manager::v1::TIER_RAM,
      payload::manager::v1::PromotionPolicy promotion_policy      = payload::manager::v1::PROMOTION_POLICY_BEST_EFFORT,
      uint64_t                              min_lease_duration_ms = 0) const;

  /// Release a previously acquired read lease.
  arrow::Status Release(const payload::manager::v1::LeaseID& lease_id) const;

  /// Request promotion to a higher tier.
  arrow::Result<payload::manager::v1::PromoteResponse> Promote(const payload::manager::v1::PromoteRequest& request) const;

  /// Request spill to a lower tier.
  arrow::Result<payload::manager::v1::SpillResponse> Spill(const payload::manager::v1::SpillRequest& request) const;

  /// Hint the service to prefetch payloads.
  arrow::Status Prefetch(const payload::manager::v1::PrefetchRequest& request) const;

  /// Pin payloads to avoid eviction.
  arrow::Status Pin(const payload::manager::v1::PinRequest& request) const;

  /// Remove previously set pins.
  arrow::Status Unpin(const payload::manager::v1::UnpinRequest& request) const;

  /// Delete payloads.
  arrow::Status Delete(const payload::manager::v1::DeleteRequest& request) const;

  /// Add lineage edges between payloads.
  arrow::Status AddLineage(const payload::manager::v1::AddLineageRequest& request) const;

  /// Query lineage for a payload.
  arrow::Result<payload::manager::v1::GetLineageResponse> GetLineage(const payload::manager::v1::GetLineageRequest& request) const;

  /// Upsert structured metadata on a payload.
  arrow::Result<payload::manager::v1::UpdatePayloadMetadataResponse> UpdatePayloadMetadata(
      const payload::manager::v1::UpdatePayloadMetadataRequest& request) const;

  /// Append an immutable metadata event to a payload.
  arrow::Result<payload::manager::v1::AppendPayloadMetadataEventResponse> AppendPayloadMetadataEvent(
      const payload::manager::v1::AppendPayloadMetadataEventRequest& request) const;

  /// Fetch service stats.
  arrow::Result<payload::manager::v1::StatsResponse> Stats(const payload::manager::v1::StatsRequest& request) const;

  /// Create a stream.
  arrow::Status CreateStream(const payload::manager::v1::CreateStreamRequest& request) const;

  /// Delete a stream.
  arrow::Status DeleteStream(const payload::manager::v1::DeleteStreamRequest& request) const;

  /// Append payload references to a stream.
  arrow::Result<payload::manager::v1::AppendResponse> Append(const payload::manager::v1::AppendRequest& request) const;

  /// Read stream entries at an offset.
  arrow::Result<payload::manager::v1::ReadResponse> Read(const payload::manager::v1::ReadRequest& request) const;

  /// Subscribe to stream events using a server-side streaming RPC.
  std::unique_ptr<grpc::ClientReader<payload::manager::v1::SubscribeResponse>> Subscribe(const payload::manager::v1::SubscribeRequest& request,
                                                                                         grpc::ClientContext*                          context) const;

  /// Commit consumer offset for a stream.
  arrow::Status Commit(const payload::manager::v1::CommitRequest& request) const;

  /// Get the committed offset for a consumer.
  arrow::Result<payload::manager::v1::GetCommittedResponse> GetCommitted(const payload::manager::v1::GetCommittedRequest& request) const;

  /// Read a range of stream entries.
  arrow::Result<payload::manager::v1::GetRangeResponse> GetRange(const payload::manager::v1::GetRangeRequest& request) const;

 private:
  /// Open a mutable Arrow buffer from a descriptor location.
  arrow::Result<std::shared_ptr<arrow::MutableBuffer>> OpenMutableBuffer(const payload::manager::v1::PayloadDescriptor& descriptor) const;
  /// Open a read-only Arrow buffer from a descriptor location.
  arrow::Result<std::shared_ptr<arrow::Buffer>>        OpenReadableBuffer(const payload::manager::v1::PayloadDescriptor& descriptor) const;

  /// Ensure descriptor has a concrete location for its tier.
  static arrow::Status ValidateHasLocation(const payload::manager::v1::PayloadDescriptor& descriptor);
  /// Return descriptor byte length for whichever location is set.
  static uint64_t      DescriptorLengthBytes(const payload::manager::v1::PayloadDescriptor& descriptor);

  /// Catalog service RPC stub.
  std::unique_ptr<payload::manager::v1::PayloadCatalogService::Stub> catalog_stub_;
  /// Data service RPC stub.
  std::unique_ptr<payload::manager::v1::PayloadDataService::Stub>    data_stub_;
  /// Admin service RPC stub.
  std::unique_ptr<payload::manager::v1::PayloadAdminService::Stub>   admin_stub_;
  /// Stream service RPC stub.
  std::unique_ptr<payload::manager::v1::PayloadStreamService::Stub>  stream_stub_;
};

} // namespace payload::manager::client

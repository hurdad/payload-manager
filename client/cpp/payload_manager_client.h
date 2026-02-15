#pragma once

#include <arrow/buffer.h>
#include <arrow/result.h>
#include <grpcpp/channel.h>
#include <grpcpp/support/sync_stream.h>

#include <cstdint>
#include <memory>
#include <string>

#include "payload/manager/services/v1/payload_admin_service.grpc.pb.h"
#include "payload/manager/services/v1/payload_catalog_service.grpc.pb.h"
#include "payload/manager/services/v1/payload_data_service.grpc.pb.h"
#include "payload/manager/services/v1/payload_stream_service.grpc.pb.h"
#include "payload/manager/v1.hpp"

namespace payload::manager::client {

class PayloadClient {
 public:
  struct WritablePayload {
    payload::manager::v1::PayloadDescriptor descriptor;
    std::shared_ptr<arrow::MutableBuffer>   buffer;
  };

  struct ReadablePayload {
    payload::manager::v1::PayloadDescriptor descriptor;
    std::string                             lease_id;
    std::shared_ptr<arrow::Buffer>          buffer;
  };

  explicit PayloadClient(std::shared_ptr<grpc::Channel> channel);

  arrow::Result<WritablePayload> AllocateWritableBuffer(uint64_t                   size_bytes,
                                                        payload::manager::v1::Tier preferred_tier = payload::manager::v1::TIER_RAM,
                                                        uint64_t ttl_ms = 0, bool persist = false) const;

  arrow::Status CommitPayload(const std::string& uuid) const;

  arrow::Result<payload::manager::v1::ResolveSnapshotResponse> Resolve(const std::string& uuid) const;

  arrow::Result<ReadablePayload> AcquireReadableBuffer(
      const std::string& uuid, payload::manager::v1::Tier min_tier = payload::manager::v1::TIER_RAM,
      payload::manager::v1::PromotionPolicy promotion_policy      = payload::manager::v1::PROMOTION_POLICY_BEST_EFFORT,
      uint64_t                              min_lease_duration_ms = 0) const;

  arrow::Status Release(const std::string& lease_id) const;

  arrow::Result<payload::manager::v1::PromoteResponse> Promote(const payload::manager::v1::PromoteRequest& request) const;

  arrow::Result<payload::manager::v1::SpillResponse> Spill(const payload::manager::v1::SpillRequest& request) const;

  arrow::Status Delete(const payload::manager::v1::DeleteRequest& request) const;

  arrow::Status AddLineage(const payload::manager::v1::AddLineageRequest& request) const;

  arrow::Result<payload::manager::v1::GetLineageResponse> GetLineage(const payload::manager::v1::GetLineageRequest& request) const;

  arrow::Result<payload::manager::v1::UpdatePayloadMetadataResponse> UpdatePayloadMetadata(
      const payload::manager::v1::UpdatePayloadMetadataRequest& request) const;

  arrow::Result<payload::manager::v1::AppendPayloadMetadataEventResponse> AppendPayloadMetadataEvent(
      const payload::manager::v1::AppendPayloadMetadataEventRequest& request) const;

  arrow::Result<payload::manager::v1::StatsResponse> Stats(const payload::manager::v1::StatsRequest& request) const;

  arrow::Status CreateStream(const payload::manager::v1::CreateStreamRequest& request) const;

  arrow::Status DeleteStream(const payload::manager::v1::DeleteStreamRequest& request) const;

  arrow::Result<payload::manager::v1::AppendResponse> Append(const payload::manager::v1::AppendRequest& request) const;

  arrow::Result<payload::manager::v1::ReadResponse> Read(const payload::manager::v1::ReadRequest& request) const;

  std::unique_ptr<grpc::ClientReader<payload::manager::v1::SubscribeResponse>> Subscribe(const payload::manager::v1::SubscribeRequest& request,
                                                                                         grpc::ClientContext*                          context) const;

  arrow::Status Commit(const payload::manager::v1::CommitRequest& request) const;

  arrow::Result<payload::manager::v1::GetCommittedResponse> GetCommitted(const payload::manager::v1::GetCommittedRequest& request) const;

  arrow::Result<payload::manager::v1::GetRangeResponse> GetRange(const payload::manager::v1::GetRangeRequest& request) const;

 private:
  arrow::Result<std::shared_ptr<arrow::MutableBuffer>> OpenMutableBuffer(const payload::manager::v1::PayloadDescriptor& descriptor) const;
  arrow::Result<std::shared_ptr<arrow::Buffer>>        OpenReadableBuffer(const payload::manager::v1::PayloadDescriptor& descriptor) const;

  static arrow::Status ValidateHasLocation(const payload::manager::v1::PayloadDescriptor& descriptor);
  static uint64_t      DescriptorLengthBytes(const payload::manager::v1::PayloadDescriptor& descriptor);

  std::unique_ptr<payload::manager::v1::PayloadCatalogService::Stub> catalog_stub_;
  std::unique_ptr<payload::manager::v1::PayloadDataService::Stub>    data_stub_;
  std::unique_ptr<payload::manager::v1::PayloadAdminService::Stub>   admin_stub_;
  std::unique_ptr<payload::manager::v1::PayloadStreamService::Stub>  stream_stub_;
};

} // namespace payload::manager::client

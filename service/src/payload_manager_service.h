#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <google/protobuf/empty.pb.h>
#include <grpcpp/grpcpp.h>

#include "payload/manager/v1/service.grpc.pb.h"

class PayloadManagerServiceImpl final : public payload::manager::v1::PayloadManager::Service {
 public:
  grpc::Status AllocatePayload(
      grpc::ServerContext* context,
      const payload::manager::v1::AllocatePayloadRequest* request,
      payload::manager::v1::AllocatePayloadResponse* response) override;

  grpc::Status CommitPayload(
      grpc::ServerContext* context,
      const payload::manager::v1::CommitPayloadRequest* request,
      payload::manager::v1::CommitPayloadResponse* response) override;

  grpc::Status Resolve(
      grpc::ServerContext* context,
      const payload::manager::v1::ResolveRequest* request,
      payload::manager::v1::ResolveResponse* response) override;

  grpc::Status BatchResolve(
      grpc::ServerContext* context,
      const payload::manager::v1::BatchResolveRequest* request,
      payload::manager::v1::BatchResolveResponse* response) override;

  grpc::Status Acquire(
      grpc::ServerContext* context,
      const payload::manager::v1::AcquireRequest* request,
      payload::manager::v1::AcquireResponse* response) override;

  grpc::Status Release(
      grpc::ServerContext* context,
      const payload::manager::v1::ReleaseRequest* request,
      google::protobuf::Empty* response) override;

  grpc::Status Promote(
      grpc::ServerContext* context,
      const payload::manager::v1::PromoteRequest* request,
      payload::manager::v1::PromoteResponse* response) override;

  grpc::Status Spill(
      grpc::ServerContext* context,
      const payload::manager::v1::SpillRequest* request,
      payload::manager::v1::SpillResponse* response) override;

  grpc::Status Delete(
      grpc::ServerContext* context,
      const payload::manager::v1::DeleteRequest* request,
      google::protobuf::Empty* response) override;

  grpc::Status AddLineage(
      grpc::ServerContext* context,
      const payload::manager::v1::AddLineageRequest* request,
      google::protobuf::Empty* response) override;

  grpc::Status GetLineage(
      grpc::ServerContext* context,
      const payload::manager::v1::GetLineageRequest* request,
      payload::manager::v1::GetLineageResponse* response) override;

  grpc::Status UpdatePayloadMetadata(
      grpc::ServerContext* context,
      const payload::manager::v1::UpdatePayloadMetadataRequest* request,
      payload::manager::v1::UpdatePayloadMetadataResponse* response) override;

  grpc::Status AppendPayloadMetadataEvent(
      grpc::ServerContext* context,
      const payload::manager::v1::AppendPayloadMetadataEventRequest* request,
      payload::manager::v1::AppendPayloadMetadataEventResponse* response) override;

  grpc::Status GetPayloadMetadata(
      grpc::ServerContext* context,
      const payload::manager::v1::GetPayloadMetadataRequest* request,
      payload::manager::v1::GetPayloadMetadataResponse* response) override;

  grpc::Status ListPayloadMetadataEvents(
      grpc::ServerContext* context,
      const payload::manager::v1::ListPayloadMetadataEventsRequest* request,
      payload::manager::v1::ListPayloadMetadataEventsResponse* response) override;

  grpc::Status UpdateEvictionPolicy(
      grpc::ServerContext* context,
      const payload::manager::v1::UpdateEvictionPolicyRequest* request,
      payload::manager::v1::UpdateEvictionPolicyResponse* response) override;

  grpc::Status Prefetch(
      grpc::ServerContext* context,
      const payload::manager::v1::PrefetchRequest* request,
      google::protobuf::Empty* response) override;

  grpc::Status Pin(
      grpc::ServerContext* context,
      const payload::manager::v1::PinRequest* request,
      google::protobuf::Empty* response) override;

  grpc::Status Stats(
      grpc::ServerContext* context,
      const payload::manager::v1::StatsRequest* request,
      payload::manager::v1::StatsResponse* response) override;

 private:
  struct LeaseInfo {
    std::string uuid;
    std::chrono::system_clock::time_point expires_at;
  };

  struct PayloadRecord {
    payload::manager::v1::PayloadDescriptor descriptor;
    uint64_t size_bytes = 0;
    payload::manager::v1::PayloadGenericMetadata metadata;
    std::vector<payload::manager::v1::PayloadMetadataEvent> metadata_events;
    std::vector<payload::manager::v1::LineageEdge> parents;
    bool pin_active = false;
    std::chrono::system_clock::time_point pin_until{};
  };

  static void PopulateLocation(payload::manager::v1::PayloadDescriptor& descriptor, uint64_t size_bytes);
  static payload::manager::v1::LineageEdge MakeChildEdge(
      const std::string& child_uuid,
      const payload::manager::v1::LineageEdge& source);

  PayloadRecord* FindPayloadOrNull(const std::string& uuid);
  uint64_t ActiveLeaseCount(const std::string& uuid) const;
  void CleanupLeasesForUuid(const std::string& uuid);

  mutable std::mutex mu_;
  std::unordered_map<std::string, PayloadRecord> payloads_;
  std::unordered_map<std::string, LeaseInfo> leases_;
  std::unordered_map<std::string, std::vector<payload::manager::v1::LineageEdge>> children_;
};

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include "client/cpp/payload_manager_client.h"
#include "payload/manager/v1.hpp"

namespace {

std::string UuidToHex(const std::string& uuid_bytes) {
  std::ostringstream os;
  for (size_t i = 0; i < uuid_bytes.size(); ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) {
      os << '-';
    }
    os << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(static_cast<unsigned char>(uuid_bytes[i]));
  }
  return os.str();
}

payload::manager::v1::PayloadID MakePayloadId(const std::string& raw_uuid) {
  payload::manager::v1::PayloadID id;
  id.set_value(raw_uuid);
  return id;
}

} // namespace

int main(int argc, char** argv) {
  const std::string target = argc > 1 ? argv[1] : "localhost:50051";

  payload::manager::client::PayloadClient client(grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));

  auto writable = client.AllocateWritableBuffer(16, payload::manager::v1::TIER_RAM, 60'000, false);
  if (!writable.ok()) {
    std::cerr << "AllocateWritableBuffer failed: " << writable.status().ToString() << '\n';
    return 1;
  }

  auto writable_payload = writable.ValueOrDie();
  for (int i = 0; i < 16; ++i) {
    writable_payload.buffer->mutable_data()[i] = static_cast<uint8_t>(i + 1);
  }

  const std::string raw_uuid = writable_payload.descriptor.id().value();
  const std::string uuid     = UuidToHex(raw_uuid);

  auto commit_status = client.CommitPayload(uuid);
  if (!commit_status.ok()) {
    std::cerr << "CommitPayload failed: " << commit_status.ToString() << '\n';
    return 1;
  }

  auto snapshot = client.Resolve(uuid);
  if (!snapshot.ok()) {
    std::cerr << "Resolve failed: " << snapshot.status().ToString() << '\n';
    return 1;
  }

  payload::manager::v1::PromoteRequest promote_request;
  *promote_request.mutable_id() = MakePayloadId(raw_uuid);
  promote_request.set_target_tier(payload::manager::v1::TIER_RAM);
  promote_request.set_policy(payload::manager::v1::PROMOTION_POLICY_BEST_EFFORT);

  auto promote = client.Promote(promote_request);
  if (!promote.ok()) {
    std::cerr << "Promote failed: " << promote.status().ToString() << '\n';
    return 1;
  }

  payload::manager::v1::SpillRequest spill_request;
  *spill_request.add_ids() = MakePayloadId(raw_uuid);
  spill_request.set_policy(payload::manager::v1::SPILL_POLICY_BEST_EFFORT);
  spill_request.set_wait_for_leases(true);

  auto spill = client.Spill(spill_request);
  if (!spill.ok()) {
    std::cerr << "Spill failed: " << spill.status().ToString() << '\n';
    return 1;
  }

  payload::manager::v1::AddLineageRequest add_lineage_request;
  *add_lineage_request.mutable_child() = MakePayloadId(raw_uuid);
  auto* parent_edge                    = add_lineage_request.add_parents();
  *parent_edge->mutable_parent()       = MakePayloadId(raw_uuid);
  parent_edge->set_operation("identity");
  parent_edge->set_role("demo");
  parent_edge->set_parameters("{}", 2);

  auto add_lineage_status = client.AddLineage(add_lineage_request);
  if (!add_lineage_status.ok()) {
    std::cerr << "AddLineage failed: " << add_lineage_status.ToString() << '\n';
    return 1;
  }

  payload::manager::v1::GetLineageRequest get_lineage_request;
  *get_lineage_request.mutable_id() = MakePayloadId(raw_uuid);
  get_lineage_request.set_upstream(true);
  get_lineage_request.set_max_depth(1);

  auto lineage = client.GetLineage(get_lineage_request);
  if (!lineage.ok()) {
    std::cerr << "GetLineage failed: " << lineage.status().ToString() << '\n';
    return 1;
  }

  payload::manager::v1::DeleteRequest delete_request;
  *delete_request.mutable_id() = MakePayloadId(raw_uuid);
  delete_request.set_force(true);

  auto delete_status = client.Delete(delete_request);
  if (!delete_status.ok()) {
    std::cerr << "Delete failed: " << delete_status.ToString() << '\n';
    return 1;
  }

  std::cout << "Catalog/Admin API calls completed for payload " << uuid << " (lineage edges returned=" << lineage.ValueOrDie().edges_size() << ")\n";
  return 0;
}

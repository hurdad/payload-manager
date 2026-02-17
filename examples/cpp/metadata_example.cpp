#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

#include "client/cpp/payload_manager_client.h"
#include "payload/manager/v1.hpp"

namespace {

std::string UuidToHex(const std::string& uuid_bytes) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string           out;
  out.reserve(36);
  for (size_t i = 0; i < uuid_bytes.size(); ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) {
      out.push_back('-');
    }
    const uint8_t byte = static_cast<uint8_t>(uuid_bytes[i]);
    out.push_back(kHex[byte >> 4]);
    out.push_back(kHex[byte & 0x0F]);
  }
  return out;
}

} // namespace

int main(int argc, char** argv) {
  // Endpoint can be passed on the command line for non-default deployments.
  const std::string target = argc > 1 ? argv[1] : "localhost:50051";

  payload::manager::client::PayloadClient client(grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));

  // Create a payload first; metadata APIs reference payload UUIDs.
  auto writable = client.AllocateWritableBuffer(8, payload::manager::v1::TIER_RAM);
  if (!writable.ok()) {
    std::cerr << "AllocateWritableBuffer failed: " << writable.status().ToString() << '\n';
    return 1;
  }

  auto writable_payload                      = writable.ValueOrDie();
  writable_payload.buffer->mutable_data()[0] = 42;

  const auto& payload_id    = writable_payload.descriptor.payload_id();
  const auto  uuid_text     = UuidToHex(payload_id.value());
  auto        commit_status = client.CommitPayload(payload_id);
  if (!commit_status.ok()) {
    std::cerr << "CommitPayload failed: " << commit_status.ToString() << '\n';
    return 1;
  }

  // UpdatePayloadMetadata writes the canonical metadata document for the
  // payload. Here we use REPLACE for full document semantics.
  payload::manager::v1::UpdatePayloadMetadataRequest update_request;
  *update_request.mutable_id() = payload_id;
  update_request.set_mode(payload::manager::v1::METADATA_UPDATE_MODE_REPLACE);
  *update_request.mutable_metadata()->mutable_id() = payload_id;
  update_request.mutable_metadata()->set_schema("example.payload.v1");
  update_request.mutable_metadata()->set_data(R"({"producer":"metadata_example","notes":"hello payload manager"})");
  update_request.set_actor("examples/cpp/metadata_example");
  update_request.set_reason("demonstrate metadata update flow");

  auto update_response = client.UpdatePayloadMetadata(update_request);
  if (!update_response.ok()) {
    std::cerr << "UpdatePayloadMetadata failed: " << update_response.status().ToString() << '\n';
    return 1;
  }

  // AppendPayloadMetadataEvent records an immutable event for audit/history.
  payload::manager::v1::AppendPayloadMetadataEventRequest event_request;
  *event_request.mutable_id()                     = payload_id;
  *event_request.mutable_metadata()->mutable_id() = payload_id;
  event_request.mutable_metadata()->set_schema("example.payload.v1");
  event_request.mutable_metadata()->set_data(R"({"event":"metadata_updated","component":"metadata_example"})");
  event_request.set_source("examples/cpp/metadata_example");
  event_request.set_version("v1");

  auto event_response = client.AppendPayloadMetadataEvent(event_request);
  if (!event_response.ok()) {
    std::cerr << "AppendPayloadMetadataEvent failed: " << event_response.status().ToString() << '\n';
    return 1;
  }

  std::cout << "Metadata updated for payload " << uuid_text << '\n';
  return 0;
}

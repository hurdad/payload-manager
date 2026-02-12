#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "client/cpp/payload_client.h"

int main(int argc, char** argv) {
  const std::string target = argc > 1 ? argv[1] : "localhost:50051";

  payload::manager::client::PayloadClient client(
      grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));

  auto writable = client.AllocateWritableBuffer(8, payload::manager::v1::TIER_RAM);
  if (!writable.ok()) {
    std::cerr << "AllocateWritableBuffer failed: " << writable.status().ToString() << '\n';
    return 1;
  }

  auto writable_payload = writable.ValueOrDie();
  writable_payload.buffer->mutable_data()[0] = 42;

  const std::string uuid = writable_payload.descriptor.uuid();
  auto commit_status = client.CommitPayload(uuid);
  if (!commit_status.ok()) {
    std::cerr << "CommitPayload failed: " << commit_status.ToString() << '\n';
    return 1;
  }

  payload::manager::v1::UpdatePayloadMetadataRequest update_request;
  update_request.set_uuid(uuid);
  update_request.set_mode(payload::manager::v1::METADATA_UPDATE_MODE_REPLACE);
  update_request.mutable_metadata()->set_schema("example.payload.v1");
  update_request.mutable_metadata()->set_json(
      R"({"producer":"metadata_example","notes":"hello payload manager"})");
  update_request.set_actor("examples/cpp/metadata_example");
  update_request.set_reason("demonstrate metadata update/get flow");

  auto update_response = client.UpdatePayloadMetadata(update_request);
  if (!update_response.ok()) {
    std::cerr << "UpdatePayloadMetadata failed: " << update_response.status().ToString() << '\n';
    return 1;
  }

  payload::manager::v1::GetPayloadMetadataRequest get_request;
  get_request.set_uuid(uuid);

  auto get_response = client.GetPayloadMetadata(get_request);
  if (!get_response.ok()) {
    std::cerr << "GetPayloadMetadata failed: " << get_response.status().ToString() << '\n';
    return 1;
  }

  std::cout << "Metadata schema: " << get_response->metadata().schema() << '\n';
  std::cout << "Metadata json: " << get_response->metadata().json() << '\n';

  return 0;
}

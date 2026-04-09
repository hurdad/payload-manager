#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

#include "client/cpp/client.h"
#include "example_util.hpp"
#include "otel_tracer.hpp"
#include "payload/manager/v1.hpp"
#include "traced_channel.hpp"

int main(int argc, char** argv) {
  // argv[1]: server endpoint  (default localhost:50051)
  // argv[2]: OTLP gRPC endpoint (default localhost:4317, empty to disable)
  const std::string target  = argc > 1 ? argv[1] : "localhost:50051";
  const std::string otlp_ep = argc > 2 ? argv[2] : "localhost:4317";

  OtelInit(otlp_ep, "cpp-examples");
  auto                                    channel = StartSpanAndMakeChannel(target, "metadata_example");
  payload::manager::client::PayloadClient client(channel);

  // Create a payload first; metadata APIs reference payload UUIDs.
  auto writable = client.AllocateWritableBuffer(8, payload::manager::v1::TIER_RAM);
  if (!writable.ok()) {
    std::cerr << "AllocateWritableBuffer failed: " << writable.status().ToString() << '\n';
    OtelEndSpan();
    OtelShutdown();
    return 1;
  }

  auto writable_payload                      = writable.ValueOrDie();
  writable_payload.buffer->mutable_data()[0] = 42;

  const auto& payload_id    = writable_payload.descriptor.payload_id();
  const auto  uuid_text     = payload::examples::UuidToHex(payload_id.value());
  auto        commit_status = client.CommitPayload(payload_id);
  if (!commit_status.ok()) {
    std::cerr << "CommitPayload failed: " << commit_status.ToString() << '\n';
    OtelEndSpan();
    OtelShutdown();
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
    OtelEndSpan();
    OtelShutdown();
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
    OtelEndSpan();
    OtelShutdown();
    return 1;
  }

  std::cout << "Metadata updated for payload " << uuid_text << '\n';
  OtelEndSpan();
  OtelShutdown();
  return 0;
}

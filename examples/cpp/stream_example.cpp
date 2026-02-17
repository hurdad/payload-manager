#include <grpcpp/client_context.h>
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


payload::manager::v1::StreamID MakeStreamId() {
  payload::manager::v1::StreamID stream;
  stream.set_name("cpp-client-demo");
  stream.set_namespace_("examples");
  return stream;
}

} // namespace

int main(int argc, char** argv) {
  // Optional endpoint argument keeps the example portable across environments.
  const std::string target = argc > 1 ? argv[1] : "localhost:50051";

  payload::manager::client::PayloadClient client(grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));

  // Create and commit a payload that stream entries will reference by ID.
  auto writable = client.AllocateWritableBuffer(8, payload::manager::v1::TIER_RAM);
  if (!writable.ok()) {
    std::cerr << "AllocateWritableBuffer failed: " << writable.status().ToString() << '\n';
    return 1;
  }

  auto writable_payload = writable.ValueOrDie();
  for (int i = 0; i < 8; ++i) {
    writable_payload.buffer->mutable_data()[i] = static_cast<uint8_t>(10 + i);
  }

  const auto& payload_id = writable_payload.descriptor.payload_id();
  const auto  uuid_text  = UuidToHex(payload_id.value());
  auto        status     = client.CommitPayload(payload_id);
  if (!status.ok()) {
    std::cerr << "CommitPayload failed: " << status.ToString() << '\n';
    return 1;
  }

  const auto stream = MakeStreamId();

  // Create an example stream with bounded retention for repeatable demos.
  payload::manager::v1::CreateStreamRequest create_request;
  *create_request.mutable_stream() = stream;
  create_request.set_retention_max_entries(1024);

  auto create_status = client.CreateStream(create_request);
  if (!create_status.ok()) {
    std::cerr << "CreateStream failed: " << create_status.ToString() << '\n';
    return 1;
  }

  // Append one entry carrying payload reference + simple provenance tags.
  payload::manager::v1::AppendRequest append_request;
  *append_request.mutable_stream() = stream;
  auto* item                       = append_request.add_items();
  *item->mutable_payload_id()      = payload_id;
  item->set_duration_ns(1'000'000);
  (*item->mutable_tags())["source"] = "examples/cpp/stream_example";

  auto append_response = client.Append(append_request);
  if (!append_response.ok()) {
    std::cerr << "Append failed: " << append_response.status().ToString() << '\n';
    return 1;
  }

  payload::manager::v1::ReadRequest read_request;
  *read_request.mutable_stream() = stream;
  read_request.set_start_offset(append_response->first_offset());
  read_request.set_max_entries(10);

  auto read_response = client.Read(read_request);
  if (!read_response.ok()) {
    std::cerr << "Read failed: " << read_response.status().ToString() << '\n';
    return 1;
  }

  // Subscribe demonstrates the streaming RPC path; we read one item then
  // cancel to keep the sample finite.
  grpc::ClientContext                    subscribe_context;
  payload::manager::v1::SubscribeRequest subscribe_request;
  *subscribe_request.mutable_stream() = stream;
  subscribe_request.set_offset(append_response->first_offset());
  subscribe_request.set_max_inflight(1);

  auto                                    reader = client.Subscribe(subscribe_request, &subscribe_context);
  payload::manager::v1::SubscribeResponse subscribe_response;
  bool                                    got_entry = reader->Read(&subscribe_response);
  subscribe_context.TryCancel();
  const grpc::Status subscribe_finish = reader->Finish();

  if (!subscribe_finish.ok() && subscribe_finish.error_code() != grpc::StatusCode::CANCELLED) {
    std::cerr << "Subscribe failed: " << subscribe_finish.error_message() << '\n';
    return 1;
  }

  // Commit consumer progress to a group checkpoint and query it back.
  payload::manager::v1::CommitRequest commit_request;
  *commit_request.mutable_stream() = stream;
  commit_request.set_consumer_group("example-group");
  commit_request.set_offset(append_response->last_offset());

  auto commit_stream_status = client.Commit(commit_request);
  if (!commit_stream_status.ok()) {
    std::cerr << "Commit(stream) failed: " << commit_stream_status.ToString() << '\n';
    return 1;
  }

  payload::manager::v1::GetCommittedRequest committed_request;
  *committed_request.mutable_stream() = stream;
  committed_request.set_consumer_group("example-group");

  auto committed_response = client.GetCommitted(committed_request);
  if (!committed_response.ok()) {
    std::cerr << "GetCommitted failed: " << committed_response.status().ToString() << '\n';
    return 1;
  }

  payload::manager::v1::GetRangeRequest range_request;
  *range_request.mutable_stream() = stream;
  range_request.set_start_offset(append_response->first_offset());
  range_request.set_end_offset(append_response->last_offset());

  auto range_response = client.GetRange(range_request);
  if (!range_response.ok()) {
    std::cerr << "GetRange failed: " << range_response.status().ToString() << '\n';
    return 1;
  }

  // Clean up to make reruns idempotent.
  payload::manager::v1::DeleteStreamRequest delete_request;
  *delete_request.mutable_stream() = stream;

  auto delete_status = client.DeleteStream(delete_request);
  if (!delete_status.ok()) {
    std::cerr << "DeleteStream failed: " << delete_status.ToString() << '\n';
    return 1;
  }

  std::cout << "Stream API calls completed for stream " << stream.namespace_() << "/" << stream.name()
            << ", read entries=" << read_response->entries_size() << ", range entries=" << range_response->entries_size()
            << ", subscribe_received=" << (got_entry ? "yes" : "no") << ", committed_offset=" << committed_response->offset() << '\n';
  return 0;
}

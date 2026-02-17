#include "client/cpp/payload_manager_client.h"

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>

namespace {

using payload::manager::client::PayloadClient;

payload::manager::v1::PayloadID MakePayloadIdOfSize(size_t size) {
  payload::manager::v1::PayloadID id;
  id.set_value(std::string(size, static_cast<char>(0xAB)));
  return id;
}

void TestPayloadIdFromUuidParsesCanonicalUuid() {
  const auto parsed = PayloadClient::PayloadIdFromUuid("00112233-4455-6677-8899-aabbccddeeff");
  assert(parsed.ok());
  assert(parsed->value().size() == 16);

  const std::string expected{"\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xAA\xBB\xCC\xDD\xEE\xFF", 16};
  assert(parsed->value() == expected);
}

void TestPayloadIdFromUuidAcceptsHexWithoutDashes() {
  const auto parsed = PayloadClient::PayloadIdFromUuid("00112233445566778899aabbccddeeff");
  assert(parsed.ok());
  assert(parsed->value().size() == 16);
}

void TestPayloadIdFromUuidRejectsInvalidCharacters() {
  const auto parsed = PayloadClient::PayloadIdFromUuid("00112233-4455-6677-8899-aabbccddeefg");
  assert(!parsed.ok());
  assert(parsed.status().IsInvalid());
}

void TestPayloadIdFromUuidRejectsInvalidLength() {
  const auto parsed = PayloadClient::PayloadIdFromUuid("00112233-4455-6677-8899-aabbccddee");
  assert(!parsed.ok());
  assert(parsed.status().IsInvalid());
}

void TestValidatePayloadIdEnforcesSixteenBytes() {
  const auto ok = PayloadClient::ValidatePayloadId(MakePayloadIdOfSize(16));
  assert(ok.ok());

  const auto short_id = PayloadClient::ValidatePayloadId(MakePayloadIdOfSize(15));
  assert(!short_id.ok());
  assert(short_id.IsInvalid());

  const auto long_id = PayloadClient::ValidatePayloadId(MakePayloadIdOfSize(17));
  assert(!long_id.ok());
  assert(long_id.IsInvalid());
}

void TestRpcHelpersRejectInvalidPayloadIdBeforeGrpcCall() {
  auto channel = grpc::CreateChannel("dns:///127.0.0.1:1", grpc::InsecureChannelCredentials());
  PayloadClient client(channel);

  const auto invalid_id = MakePayloadIdOfSize(8);

  const auto commit = client.CommitPayload(invalid_id);
  assert(!commit.ok());
  assert(commit.IsInvalid());

  const auto resolve = client.Resolve(invalid_id);
  assert(!resolve.ok());
  assert(resolve.status().IsInvalid());

  const auto acquire = client.AcquireReadableBuffer(invalid_id);
  assert(!acquire.ok());
  assert(acquire.status().IsInvalid());
}

} // namespace

int main() {
  TestPayloadIdFromUuidParsesCanonicalUuid();
  TestPayloadIdFromUuidAcceptsHexWithoutDashes();
  TestPayloadIdFromUuidRejectsInvalidCharacters();
  TestPayloadIdFromUuidRejectsInvalidLength();
  TestValidatePayloadIdEnforcesSixteenBytes();
  TestRpcHelpersRejectInvalidPayloadIdBeforeGrpcCall();

  std::cout << "payload_manager_unit_client_payload_manager_client: pass\n";
  return 0;
}

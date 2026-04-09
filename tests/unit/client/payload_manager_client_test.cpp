#include "client/cpp/client.h"

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <string>

namespace {

using payload::manager::client::PayloadClient;

payload::manager::v1::PayloadID MakePayloadIdOfSize(size_t size) {
  payload::manager::v1::PayloadID id;
  id.set_value(std::string(size, static_cast<char>(0xAB)));
  return id;
}

} // namespace

TEST(PayloadClient, PayloadIdFromUuidParsesCanonicalUuid) {
  const auto parsed = PayloadClient::PayloadIdFromUuid("00112233-4455-6677-8899-aabbccddeeff");
  ASSERT_TRUE(parsed.ok());
  EXPECT_EQ(parsed->value().size(), 16u);

  const std::string expected{"\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xAA\xBB\xCC\xDD\xEE\xFF", 16};
  EXPECT_EQ(parsed->value(), expected);
}

TEST(PayloadClient, PayloadIdFromUuidAcceptsHexWithoutDashes) {
  const auto parsed = PayloadClient::PayloadIdFromUuid("00112233445566778899aabbccddeeff");
  ASSERT_TRUE(parsed.ok());
  EXPECT_EQ(parsed->value().size(), 16u);
}

TEST(PayloadClient, PayloadIdFromUuidAllZeros) {
  const auto parsed = PayloadClient::PayloadIdFromUuid("00000000-0000-0000-0000-000000000000");
  ASSERT_TRUE(parsed.ok());
  EXPECT_EQ(parsed->value().size(), 16u);
  EXPECT_EQ(parsed->value(), std::string(16, '\0'));
}

TEST(PayloadClient, PayloadIdFromUuidUppercaseHex) {
  const auto parsed = PayloadClient::PayloadIdFromUuid("AABBCCDD-EEFF-0011-2233-445566778899");
  ASSERT_TRUE(parsed.ok());
  EXPECT_EQ(parsed->value().size(), 16u);
}

TEST(PayloadClient, PayloadIdFromUuidMixedCaseProducesSameBytes) {
  // Uppercase and lowercase hex must decode to identical byte sequences.
  const auto lower = PayloadClient::PayloadIdFromUuid("aabbccddeeff00112233445566778899");
  const auto upper = PayloadClient::PayloadIdFromUuid("AABBCCDDEEFF00112233445566778899");
  ASSERT_TRUE(lower.ok());
  ASSERT_TRUE(upper.ok());
  EXPECT_EQ(lower->value(), upper->value());
}

TEST(PayloadClient, PayloadIdFromUuidRejectsInvalidCharacters) {
  const auto parsed = PayloadClient::PayloadIdFromUuid("00112233-4455-6677-8899-aabbccddeefg");
  EXPECT_FALSE(parsed.ok());
  EXPECT_TRUE(parsed.status().IsInvalid());
}

TEST(PayloadClient, PayloadIdFromUuidRejectsInvalidLength) {
  const auto parsed = PayloadClient::PayloadIdFromUuid("00112233-4455-6677-8899-aabbccddee");
  EXPECT_FALSE(parsed.ok());
  EXPECT_TRUE(parsed.status().IsInvalid());
}

TEST(PayloadClient, PayloadIdFromUuidRejectsTooLong) {
  // 33 hex characters after stripping dashes — one too many.
  const auto parsed = PayloadClient::PayloadIdFromUuid("00112233445566778899aabbccddeeff00");
  EXPECT_FALSE(parsed.ok());
  EXPECT_TRUE(parsed.status().IsInvalid());
}

TEST(PayloadClient, PayloadIdFromUuidRejectsEmpty) {
  const auto parsed = PayloadClient::PayloadIdFromUuid("");
  EXPECT_FALSE(parsed.ok());
  EXPECT_TRUE(parsed.status().IsInvalid());
}

TEST(PayloadClient, PayloadIdFromUuidRejectsOnlyDashes) {
  // All dashes strip to zero hex characters.
  const auto parsed = PayloadClient::PayloadIdFromUuid("----");
  EXPECT_FALSE(parsed.ok());
  EXPECT_TRUE(parsed.status().IsInvalid());
}

TEST(PayloadClient, ValidatePayloadIdEnforcesSixteenBytes) {
  const auto ok = PayloadClient::ValidatePayloadId(MakePayloadIdOfSize(16));
  EXPECT_TRUE(ok.ok());

  const auto short_id = PayloadClient::ValidatePayloadId(MakePayloadIdOfSize(15));
  EXPECT_FALSE(short_id.ok());
  EXPECT_TRUE(short_id.IsInvalid());

  const auto long_id = PayloadClient::ValidatePayloadId(MakePayloadIdOfSize(17));
  EXPECT_FALSE(long_id.ok());
  EXPECT_TRUE(long_id.IsInvalid());
}

TEST(PayloadClient, ValidatePayloadIdRejectsEmpty) {
  const auto empty = PayloadClient::ValidatePayloadId(MakePayloadIdOfSize(0));
  EXPECT_FALSE(empty.ok());
  EXPECT_TRUE(empty.IsInvalid());
}

TEST(PayloadClient, ValidatePayloadIdDoesNotInspectContent) {
  // Validation is size-only; content is treated as opaque bytes.
  payload::manager::v1::PayloadID id;
  id.set_value(std::string(16, '\0'));
  EXPECT_TRUE(PayloadClient::ValidatePayloadId(id).ok());

  id.set_value(std::string(16, '\xFF'));
  EXPECT_TRUE(PayloadClient::ValidatePayloadId(id).ok());
}

TEST(PayloadClient, RpcHelpersRejectInvalidPayloadIdBeforeGrpcCall) {
  auto          channel = grpc::CreateChannel("dns:///127.0.0.1:1", grpc::InsecureChannelCredentials());
  PayloadClient client(channel);

  const auto invalid_id = MakePayloadIdOfSize(8);

  const auto commit = client.CommitPayload(invalid_id);
  EXPECT_FALSE(commit.ok());
  EXPECT_TRUE(commit.IsInvalid());

  const auto resolve = client.Resolve(invalid_id);
  EXPECT_FALSE(resolve.ok());
  EXPECT_TRUE(resolve.status().IsInvalid());

  const auto acquire = client.AcquireReadableBuffer(invalid_id);
  EXPECT_FALSE(acquire.ok());
  EXPECT_TRUE(acquire.status().IsInvalid());
}

TEST(PayloadClient, RpcHelpersRejectEmptyPayloadIdBeforeGrpcCall) {
  auto          channel = grpc::CreateChannel("dns:///127.0.0.1:1", grpc::InsecureChannelCredentials());
  PayloadClient client(channel);

  const auto empty_id = MakePayloadIdOfSize(0);

  EXPECT_TRUE(client.CommitPayload(empty_id).IsInvalid());
  EXPECT_TRUE(client.Resolve(empty_id).status().IsInvalid());
  EXPECT_TRUE(client.AcquireReadableBuffer(empty_id).status().IsInvalid());
}

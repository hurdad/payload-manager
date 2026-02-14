#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "client/cpp/payload_client.h"
#include "payload/manager/v1.hpp"

namespace {

std::string UuidToHex(const std::string& uuid_bytes) {
  std::ostringstream os;
  for (size_t i = 0; i < uuid_bytes.size(); ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) {
      os << '-';
    }
    os << std::hex << std::setw(2) << std::setfill('0')
       << static_cast<int>(static_cast<unsigned char>(uuid_bytes[i]));
  }
  return os.str();
}

}  // namespace

int main(int argc, char** argv) {
  const std::string target = argc > 1 ? argv[1] : "localhost:50051";

  payload::manager::client::PayloadClient client(
      grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));

  constexpr uint64_t kPayloadSize = 64;
  auto writable = client.AllocateWritableBuffer(kPayloadSize, payload::manager::v1::TIER_RAM);
  if (!writable.ok()) {
    std::cerr << "AllocateWritableBuffer failed: " << writable.status().ToString() << '\n';
    return 1;
  }

  auto writable_payload = writable.ValueOrDie();
  for (uint64_t i = 0; i < kPayloadSize; ++i) {
    writable_payload.buffer->mutable_data()[i] = static_cast<uint8_t>(i & 0xFFu);
  }

  const std::string uuid = writable_payload.descriptor.id().value();
  auto commit_status = client.CommitPayload(UuidToHex(uuid));
  if (!commit_status.ok()) {
    std::cerr << "CommitPayload failed: " << commit_status.ToString() << '\n';
    return 1;
  }

  auto readable = client.AcquireReadableBuffer(UuidToHex(uuid));
  if (!readable.ok()) {
    std::cerr << "AcquireReadableBuffer failed: " << readable.status().ToString() << '\n';
    return 1;
  }

  auto readable_payload = readable.ValueOrDie();
  std::cout << "Committed and acquired payload UUID=" << UuidToHex(uuid)
            << ", size=" << readable_payload.buffer->size() << " bytes\n";

  const int preview_len = static_cast<int>(std::min<int64_t>(8, readable_payload.buffer->size()));
  std::cout << "First " << preview_len << " bytes:";
  for (int i = 0; i < preview_len; ++i) {
    std::cout << ' ' << static_cast<int>(readable_payload.buffer->data()[i]);
  }
  std::cout << '\n';

  auto release_status = client.Release(readable_payload.lease_id);
  if (!release_status.ok()) {
    std::cerr << "Release failed: " << release_status.ToString() << '\n';
    return 1;
  }

  return 0;
}

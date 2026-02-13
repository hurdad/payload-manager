#pragma once

#include <array>
#include <string>
#include <random>

#include "payload/manager/v1/id.pb.h"

namespace payload::util {

/*
  UUID helpers

  PayloadID uses raw 16 byte RFC4122 UUID.
*/

using UUID = std::array<uint8_t, 16>;

UUID GenerateUUID();

std::string ToString(const UUID& id);
UUID FromString(const std::string& str);

// protobuf helpers
payload::manager::v1::PayloadID ToProto(const UUID& id);
UUID FromProto(const payload::manager::v1::PayloadID& id);

} // namespace payload::util

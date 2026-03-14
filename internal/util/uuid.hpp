#pragma once

#include <array>
#include <random>
#include <string>

#include "payload/manager/core/v1/id.pb.h"
#include "payload/manager/v1.hpp"

namespace payload::util {

/*
  UUID helpers

  PayloadID uses raw 16 byte RFC4122 UUID.
*/

using UUID = std::array<uint8_t, 16>;

UUID GenerateUUID();

std::string ToString(const UUID& id);
UUID        FromString(const std::string& str);

// protobuf helpers
payload::manager::v1::PayloadID ToProto(const UUID& id);
payload::manager::v1::LeaseID   ToLeaseProto(const UUID& id);
UUID                            FromProto(const payload::manager::v1::PayloadID& id);
UUID                            FromProto(const payload::manager::v1::LeaseID& id);

// Returns a human-readable hex UUID string for a PayloadID.
// If value() is exactly 16 bytes it is decoded as binary UUID; otherwise
// the raw value() is returned as-is (for synthetic test IDs).
std::string PayloadIdToHex(const payload::manager::v1::PayloadID& id);

} // namespace payload::util

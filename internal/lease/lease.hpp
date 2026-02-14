#pragma once

#include <chrono>
#include <string>

#include "payload/manager/core/v1/id.pb.h"
#include "payload/manager/core/v1/placement.pb.h"
#include "payload/manager/v1.hpp"

namespace payload::lease {

struct Lease {
  std::string                             lease_id;
  payload::manager::v1::PayloadID         payload_id;
  payload::manager::v1::PayloadDescriptor payload_descriptor;
  std::chrono::system_clock::time_point   expires_at;
};

} // namespace payload::lease

#pragma once

#include <string>
#include <chrono>
#include "payload/manager/v1/id.pb.h"
#include "payload/manager/v1/placement.pb.h"
#include "google/protobuf/timestamp.pb.h"

namespace payload::lease {

struct Lease {
  std::string lease_id;
  payload::manager::v1::PayloadID payload_id;
  payload::manager::v1::Placement placement;

  std::chrono::system_clock::time_point expires_at;
};

}

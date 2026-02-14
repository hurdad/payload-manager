#pragma once

#include "payload/manager/core/v1/id.pb.h"
#include "payload/manager/core/v1/types.pb.h"
#include "payload/manager/v1_compat.hpp"

namespace payload::spill {

/*
  A scheduled durability request.

  Represents making a payload durable in a target tier.
*/
struct SpillTask {
  payload::manager::v1::PayloadID id;

  payload::manager::v1::Tier target_tier;

  bool fsync = false;
  bool wait_for_leases = false;
};

}

#pragma once

#include "payload/manager/core/v1/types.pb.h"
#include "payload/manager/v1_compat.hpp"

namespace payload::core {

/*
  Stateless helper for tier ordering decisions.
*/
class PlacementEngine {
public:
    static bool IsHigherTier(payload::manager::v1::Tier a,
                             payload::manager::v1::Tier b);

    static payload::manager::v1::Tier NextLowerTier(payload::manager::v1::Tier t);
};

}

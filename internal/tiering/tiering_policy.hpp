#pragma once

#include <optional>
#include "payload/manager/core/v1/id.pb.h"
#include "pressure_state.hpp"
#include "payload/manager/v1_compat.hpp"

namespace payload::metadata { class MetadataCache; }

namespace payload::tiering {

/*
  Determines which payload should move tiers.
*/
class TieringPolicy {
public:
  explicit TieringPolicy(std::shared_ptr<payload::metadata::MetadataCache> cache);

  std::optional<payload::manager::v1::PayloadID>
  ChooseRamEviction(const PressureState& state);

  std::optional<payload::manager::v1::PayloadID>
  ChooseGpuEviction(const PressureState& state);

private:
  std::shared_ptr<payload::metadata::MetadataCache> cache_;
};

}

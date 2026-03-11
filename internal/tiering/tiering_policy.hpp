#pragma once

#include <functional>
#include <optional>

#include "payload/manager/core/v1/id.pb.h"
#include "payload/manager/v1.hpp"
#include "pressure_state.hpp"

namespace payload::metadata {
class MetadataCache;
}

namespace payload::tiering {

/*
  Determines which payload should move tiers.
*/
class TieringPolicy {
 public:
  // is_evictable: returns true if the given payload may be chosen as a victim.
  TieringPolicy(std::shared_ptr<payload::metadata::MetadataCache>           cache,
                std::function<bool(const payload::manager::v1::PayloadID&)> is_evictable = {});

  std::optional<payload::manager::v1::PayloadID> ChooseRamEviction(const PressureState& state);

  std::optional<payload::manager::v1::PayloadID> ChooseGpuEviction(const PressureState& state);

 private:
  std::shared_ptr<payload::metadata::MetadataCache>           cache_;
  std::function<bool(const payload::manager::v1::PayloadID&)> is_evictable_;
};

} // namespace payload::tiering

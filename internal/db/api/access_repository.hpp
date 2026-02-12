#pragma once

#include <vector>

#include "internal/db/api/types.hpp"

namespace payload::manager::internal::db::api {

class AccessRepository {
 public:
  virtual ~AccessRepository() = default;

  virtual void RecordAccess(const AccessEvent& event) = 0;
  virtual std::vector<AccessEvent> RecentAccesses(const Uuid& uuid,
                                                  const Pagination& pagination) const = 0;
};

}  // namespace payload::manager::internal::db::api

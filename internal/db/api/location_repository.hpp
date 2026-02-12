#pragma once

#include <optional>
#include <vector>

#include "internal/db/api/types.hpp"

namespace payload::manager::internal::db::api {

class LocationRepository {
 public:
  virtual ~LocationRepository() = default;

  virtual void Upsert(const Uuid& uuid, const model::Location& location) = 0;
  virtual std::optional<model::Location> FindByUuid(const Uuid& uuid) const = 0;
  virtual std::vector<Uuid> ListByTier(model::Tier tier, const Pagination& pagination) const = 0;
  virtual bool Delete(const Uuid& uuid) = 0;
};

}  // namespace payload::manager::internal::db::api

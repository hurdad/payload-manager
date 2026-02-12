#pragma once

#include <optional>
#include <vector>

#include "internal/db/api/types.hpp"

namespace payload::manager::internal::db::api {

class PayloadRepository {
 public:
  virtual ~PayloadRepository() = default;

  virtual void Upsert(const model::Payload& payload) = 0;
  virtual std::optional<model::Payload> FindByUuid(const Uuid& uuid) const = 0;
  virtual std::vector<model::Payload> List(const PayloadFilter& filter,
                                           const Pagination& pagination) const = 0;
  virtual bool Delete(const Uuid& uuid) = 0;
};

}  // namespace payload::manager::internal::db::api

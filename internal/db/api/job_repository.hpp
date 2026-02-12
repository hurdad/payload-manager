#pragma once

#include <optional>
#include <vector>

#include "internal/db/api/types.hpp"

namespace payload::manager::internal::db::api {

class JobRepository {
 public:
  virtual ~JobRepository() = default;

  virtual void Upsert(const JobRecord& job) = 0;
  virtual std::optional<JobRecord> FindById(const std::string& id) const = 0;
  virtual std::vector<JobRecord> ListByStatus(const std::string& status,
                                              const Pagination& pagination) const = 0;
};

}  // namespace payload::manager::internal::db::api

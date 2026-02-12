#pragma once

#include <memory>

#include "internal/db/api/access_repository.hpp"
#include "internal/db/api/connection.hpp"
#include "internal/db/api/job_repository.hpp"
#include "internal/db/api/lineage_repository.hpp"
#include "internal/db/api/location_repository.hpp"
#include "internal/db/api/payload_repository.hpp"
#include "internal/db/api/transaction.hpp"

namespace payload::manager::internal::db::api {

class Database {
 public:
  virtual ~Database() = default;

  virtual Connection& ConnectionHandle() = 0;
  virtual std::unique_ptr<Transaction> BeginTransaction() = 0;

  virtual PayloadRepository& Payloads() = 0;
  virtual LocationRepository& Locations() = 0;
  virtual LineageRepository& Lineage() = 0;
  virtual AccessRepository& Accesses() = 0;
  virtual JobRepository& Jobs() = 0;
};

}  // namespace payload::manager::internal::db::api

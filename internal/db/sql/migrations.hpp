#pragma once

#include <string>
#include <vector>

namespace payload::db::sql {

/*
  Backend-agnostic migration execution.

  Each backend implements ExecuteSQL().
*/

class MigrationExecutor {
 public:
  virtual ~MigrationExecutor() = default;

  virtual void ExecuteSQL(const std::string& sql) = 0;
};

/*
  Runs migrations in order.
  Files are provided by backend-specific loader.
*/

void RunMigrations(MigrationExecutor& executor, const std::vector<std::string>& ordered_sql);

} // namespace payload::db::sql

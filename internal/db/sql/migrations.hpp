#pragma once

#include <string>
#include <vector>

namespace payload::db::sql {

/*
  Backend-agnostic migration execution.

  Each backend implements ExecuteSQL().

  NOTE: a sketch, not working code. RunMigrations is declared here and defined
  nowhere, so calling it is a link error; nothing does. It is kept as the shape
  a versioned runner would take, next to the payload_schema_migrations table
  BootstrapSchema already creates and never writes to.

  The schema is currently created by db::postgres::BootstrapSchema, which
  replays idempotent DDL at startup. Finishing this becomes worthwhile at the
  first change that cannot express — a dropped column, a split table, a data
  backfill, anything order-dependent. At that point BootstrapSchema's body
  becomes 0001_baseline.sql, this gains an implementation, and the version
  column starts being written and checked.

  Until then, do not add .sql files for it to run. A set of numbered migrations
  that nothing loads used to live under internal/db/migrations; they drifted
  out of step with the real schema and were deleted for it.
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

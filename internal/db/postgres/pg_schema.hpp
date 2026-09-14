#pragma once

// Postgres schema bootstrap.
//
// One definition of the schema, shared by the service and by the tests that
// run against a real database. It lived inline in factory::Build, the
// repository-parity test carried its own hand-written copy, and a set of
// numbered .sql files sat under internal/db/migrations that nothing loaded —
// three descriptions of the same tables, free to drift. The .sql files are
// gone; this is the only one left.
//
// They did. The test's copy never gained min_residency_tier or
// require_durable, typed ids as TEXT where the service uses UUID, and left
// `offset` unquoted, which is a syntax error in Postgres. Because every
// statement shares one transaction, that error rolled the whole schema back
// and surfaced much later as `relation "payload" does not exist`. None of it
// was caught, because nothing ran the suite against Postgres until CI did.
//
// Idempotent: every statement is CREATE ... IF NOT EXISTS, ADD COLUMN IF NOT
// EXISTS, or a guarded DO block, so it is safe on a fresh database and on one
// created by an older build.

#include <string>

namespace payload::db::postgres {

// Create or upgrade the schema at `conninfo`, then verify the expected
// columns are readable. Throws pqxx exceptions on failure.
void BootstrapSchema(const std::string& conninfo);

} // namespace payload::db::postgres

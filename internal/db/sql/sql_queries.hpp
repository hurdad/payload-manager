#pragma once

namespace payload::db::sql {

/*
  Canonical SQL used by all backends.

  IMPORTANT:
  These are written in SQLite-compatible SQL subset
  so they work in both engines.
*/

static constexpr const char* INSERT_PAYLOAD =
    "INSERT INTO payload(id,tier,state,size_bytes,version,expires_at_ms)"
    " VALUES(?,?,?,?,?,?);";

static constexpr const char* SELECT_PAYLOAD =
    "SELECT id,tier,state,size_bytes,version,expires_at_ms"
    " FROM payload WHERE id=?;";

static constexpr const char* UPDATE_PAYLOAD =
    "UPDATE payload SET tier=?,state=?,size_bytes=?,version=?,expires_at_ms=?"
    " WHERE id=?;";

static constexpr const char* DELETE_PAYLOAD =
    "DELETE FROM payload WHERE id=?;";

// metadata

static constexpr const char* UPSERT_METADATA =
    "INSERT INTO payload_metadata(id,json,schema,updated_at_ms)"
    " VALUES(?,?,?,?)"
    " ON CONFLICT(id) DO UPDATE SET"
    " json=excluded.json,"
    " schema=excluded.schema,"
    " updated_at_ms=excluded.updated_at_ms;";

static constexpr const char* SELECT_METADATA =
    "SELECT id,json,schema,updated_at_ms"
    " FROM payload_metadata WHERE id=?;";

// lineage

static constexpr const char* INSERT_LINEAGE =
    "INSERT INTO payload_lineage(parent_id,child_id,operation,role,parameters,created_at_ms)"
    " VALUES(?,?,?,?,?,?);";

static constexpr const char* SELECT_PARENTS =
    "SELECT parent_id,child_id,operation,role,parameters,created_at_ms"
    " FROM payload_lineage WHERE child_id=?;";

static constexpr const char* SELECT_CHILDREN =
    "SELECT parent_id,child_id,operation,role,parameters,created_at_ms"
    " FROM payload_lineage WHERE parent_id=?;";

}

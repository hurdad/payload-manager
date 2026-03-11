#pragma once

#include <string>

namespace payload::db::model {

/*
  Immutable audit history entry for a payload.

  Append-only; records are never updated.
*/

struct MetadataEventRecord {
  std::string id;      // payload UUID
  std::string data;    // opaque event payload (bytes stored as-is)
  std::string schema;  // schema identifier
  std::string source;  // event producer
  std::string version; // schema version
  uint64_t    ts_ms = 0; // event timestamp (epoch ms)
};

} // namespace payload::db::model

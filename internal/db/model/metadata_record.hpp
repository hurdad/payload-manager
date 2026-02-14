#pragma once

#include <string>

namespace payload::db::model {

/*
  Current metadata snapshot.

  Stored as JSON text for portability:
    postgres -> jsonb
    sqlite   -> text
    memory   -> string
*/

struct MetadataRecord {
  std::string id; // payload UUID

  // opaque metadata blob
  std::string json;

  // schema identifier (sigmf.core@1, rf.capture.v2, etc)
  std::string schema;

  // monotonic update time (epoch ms)
  uint64_t updated_at_ms = 0;
};

} // namespace payload::db::model

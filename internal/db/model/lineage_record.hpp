#pragma once

#include <string>

namespace payload::db::model {

/*
  Generic lineage edge.

  This intentionally avoids domain meaning.
  The payload manager stores relationships only.

  parent ---> child
*/

struct LineageRecord {
  std::string parent_id;
  std::string child_id;

  std::string operation; // "fft", "demod", "classifier", etc
  std::string role;      // "input", "reference", "training", etc

  // optional opaque params (JSON, protobuf, CBOR, etc)
  std::string parameters;

  // event timestamp (epoch ms)
  uint64_t created_at_ms = 0;
};

} // namespace payload::db::model

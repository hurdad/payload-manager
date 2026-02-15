#include "internal/metadata/metadata_cache.hpp"

#include <cassert>
#include <iostream>

namespace {

using payload::manager::v1::PayloadID;
using payload::manager::v1::PayloadMetadata;
using payload::metadata::MetadataCache;

PayloadID MakePayloadID(const std::string& value) {
  PayloadID id;
  id.set_value(value);
  return id;
}

void TestPutAndGetRoundTripsMetadata() {
  MetadataCache cache;
  const auto    id = MakePayloadID("payload-1");

  PayloadMetadata metadata;
  *metadata.mutable_id() = id;
  metadata.set_data("{\"status\":\"ok\"}");
  metadata.set_schema("schema.v1");

  cache.Put(id, metadata);

  const auto cached = cache.Get(id);
  assert(cached.has_value());
  assert(cached->id().value() == "payload-1");
  assert(cached->data() == "{\"status\":\"ok\"}");
  assert(cached->schema() == "schema.v1");
}

void TestMergeKeepsExistingFieldsWhenUpdateIsEmpty() {
  MetadataCache cache;
  const auto    id = MakePayloadID("payload-merge");

  PayloadMetadata initial;
  *initial.mutable_id() = id;
  initial.set_data("initial-data");
  initial.set_schema("schema.v1");
  cache.Put(id, initial);

  PayloadMetadata update;
  cache.Merge(id, update);

  const auto cached = cache.Get(id);
  assert(cached.has_value());
  assert(cached->id().value() == "payload-merge");
  assert(cached->data() == "initial-data");
  assert(cached->schema() == "schema.v1");
}

void TestMergeOnMissingEntrySeedsIdAndProvidedFields() {
  MetadataCache cache;
  const auto    id = MakePayloadID("payload-new");

  PayloadMetadata update;
  update.set_schema("schema.v2");

  cache.Merge(id, update);

  const auto cached = cache.Get(id);
  assert(cached.has_value());
  assert(cached->id().value() == "payload-new");
  assert(cached->data().empty());
  assert(cached->schema() == "schema.v2");
}

void TestRemoveErasesEntry() {
  MetadataCache cache;
  const auto    id = MakePayloadID("payload-remove");

  PayloadMetadata metadata;
  *metadata.mutable_id() = id;
  metadata.set_data("value");
  cache.Put(id, metadata);

  cache.Remove(id);

  assert(!cache.Get(id).has_value());
}

} // namespace

int main() {
  TestPutAndGetRoundTripsMetadata();
  TestMergeKeepsExistingFieldsWhenUpdateIsEmpty();
  TestMergeOnMissingEntrySeedsIdAndProvidedFields();
  TestRemoveErasesEntry();

  std::cout << "payload_manager_unit_metadata_cache: pass\n";
  return 0;
}

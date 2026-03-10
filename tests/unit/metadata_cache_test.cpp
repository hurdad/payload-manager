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

void TestListIdsReturnsAllCachedPayloadIds() {
  MetadataCache cache;

  PayloadMetadata first;
  auto            id1 = MakePayloadID("payload-ids-1");
  *first.mutable_id() = id1;
  cache.Put(id1, first);

  PayloadMetadata second;
  auto            id2  = MakePayloadID("payload-ids-2");
  *second.mutable_id() = id2;
  cache.Put(id2, second);

  const auto ids = cache.ListIds();
  assert(ids.size() == 2);

  bool found_first  = false;
  bool found_second = false;
  for (const auto& id : ids) {
    if (id.value() == "payload-ids-1") found_first = true;
    if (id.value() == "payload-ids-2") found_second = true;
  }

  assert(found_first);
  assert(found_second);
}

void TestLeastRecentlyUsedTracksAccessOrder() {
  MetadataCache cache;

  PayloadMetadata first;
  auto            id1 = MakePayloadID("payload-lru-1");
  *first.mutable_id() = id1;
  cache.Put(id1, first);

  PayloadMetadata second;
  auto            id2  = MakePayloadID("payload-lru-2");
  *second.mutable_id() = id2;
  cache.Put(id2, second);

  auto lru = cache.GetLeastRecentlyUsedId();
  assert(lru.has_value());
  assert(lru->value() == "payload-lru-1");

  // Access first item; second should now become LRU.
  (void)cache.Get(id1);
  lru = cache.GetLeastRecentlyUsedId();
  assert(lru.has_value());
  assert(lru->value() == "payload-lru-2");
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

void TestRemoveUpdatesLeastRecentlyUsed() {
  MetadataCache cache;

  PayloadMetadata first;
  auto            id1 = MakePayloadID("payload-remove-lru-1");
  *first.mutable_id() = id1;
  cache.Put(id1, first);

  PayloadMetadata second;
  auto            id2  = MakePayloadID("payload-remove-lru-2");
  *second.mutable_id() = id2;
  cache.Put(id2, second);

  cache.Remove(id1);

  const auto lru = cache.GetLeastRecentlyUsedId();
  assert(lru.has_value());
  assert(lru->value() == "payload-remove-lru-2");
}

} // namespace

int main() {
  TestPutAndGetRoundTripsMetadata();
  TestMergeKeepsExistingFieldsWhenUpdateIsEmpty();
  TestMergeOnMissingEntrySeedsIdAndProvidedFields();
  TestListIdsReturnsAllCachedPayloadIds();
  TestLeastRecentlyUsedTracksAccessOrder();
  TestRemoveErasesEntry();
  TestRemoveUpdatesLeastRecentlyUsed();

  std::cout << "payload_manager_unit_metadata_cache: pass\n";
  return 0;
}

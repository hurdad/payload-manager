#include "internal/metadata/metadata_cache.hpp"

#include <gtest/gtest.h>

namespace {

using payload::manager::v1::PayloadID;
using payload::manager::v1::PayloadMetadata;
using payload::metadata::MetadataCache;

PayloadID MakePayloadID(const std::string& value) {
  PayloadID id;
  id.set_value(value);
  return id;
}

} // namespace

TEST(MetadataCache, PutAndGetRoundTripsMetadata) {
  MetadataCache cache;
  const auto    id = MakePayloadID("payload-1");

  PayloadMetadata metadata;
  *metadata.mutable_id() = id;
  metadata.set_data("{\"status\":\"ok\"}");
  metadata.set_schema("schema.v1");

  cache.Put(id, metadata);

  const auto cached = cache.Get(id);
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(cached->id().value(), "payload-1");
  EXPECT_EQ(cached->data(), "{\"status\":\"ok\"}");
  EXPECT_EQ(cached->schema(), "schema.v1");
}

TEST(MetadataCache, MergeKeepsExistingFieldsWhenUpdateIsEmpty) {
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
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(cached->id().value(), "payload-merge");
  EXPECT_EQ(cached->data(), "initial-data");
  EXPECT_EQ(cached->schema(), "schema.v1");
}

TEST(MetadataCache, MergeOnMissingEntrySeedsIdAndProvidedFields) {
  MetadataCache cache;
  const auto    id = MakePayloadID("payload-new");

  PayloadMetadata update;
  update.set_schema("schema.v2");

  cache.Merge(id, update);

  const auto cached = cache.Get(id);
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(cached->id().value(), "payload-new");
  EXPECT_TRUE(cached->data().empty());
  EXPECT_EQ(cached->schema(), "schema.v2");
}

TEST(MetadataCache, ListIdsReturnsAllCachedPayloadIds) {
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
  EXPECT_EQ(ids.size(), 2u);

  bool found_first  = false;
  bool found_second = false;
  for (const auto& id : ids) {
    if (id.value() == "payload-ids-1") found_first = true;
    if (id.value() == "payload-ids-2") found_second = true;
  }

  EXPECT_TRUE(found_first);
  EXPECT_TRUE(found_second);
}

TEST(MetadataCache, LeastRecentlyUsedTracksAccessOrder) {
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
  ASSERT_TRUE(lru.has_value());
  EXPECT_EQ(lru->value(), "payload-lru-1");

  // Access first item; second should now become LRU.
  (void)cache.Get(id1);
  lru = cache.GetLeastRecentlyUsedId();
  ASSERT_TRUE(lru.has_value());
  EXPECT_EQ(lru->value(), "payload-lru-2");
}

TEST(MetadataCache, RemoveErasesEntry) {
  MetadataCache cache;
  const auto    id = MakePayloadID("payload-remove");

  PayloadMetadata metadata;
  *metadata.mutable_id() = id;
  metadata.set_data("value");
  cache.Put(id, metadata);

  cache.Remove(id);

  EXPECT_FALSE(cache.Get(id).has_value());
}

TEST(MetadataCache, RemoveUpdatesLeastRecentlyUsed) {
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
  ASSERT_TRUE(lru.has_value());
  EXPECT_EQ(lru->value(), "payload-remove-lru-2");
}

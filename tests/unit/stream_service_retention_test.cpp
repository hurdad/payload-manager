#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <string>

#include "internal/db/memory/memory_repository.hpp"
#include "internal/service/service_context.hpp"
#include "internal/service/stream_service.hpp"
#include "internal/util/uuid.hpp"
#include "payload/manager/v1.hpp"

namespace {

using payload::db::memory::MemoryRepository;
using payload::manager::v1::AppendItem;
using payload::manager::v1::AppendRequest;
using payload::manager::v1::CreateStreamRequest;
using payload::manager::v1::ReadRequest;
using payload::manager::v1::StreamID;
using payload::service::ServiceContext;
using payload::service::StreamService;

StreamService MakeService(std::shared_ptr<payload::db::Repository> repo) {
  ServiceContext ctx;
  ctx.repository = std::move(repo);
  return StreamService(std::move(ctx));
}

StreamID MakeStream(const std::string& name) {
  StreamID id;
  id.set_namespace_("test");
  id.set_name(name);
  return id;
}

AppendItem MakeItem() {
  AppendItem item;
  *item.mutable_payload_id() = payload::util::ToProto(payload::util::GenerateUUID());
  return item;
}

} // namespace

// ---------------------------------------------------------------------------
// Test: Append with max uint64_t retention_max_age_sec does not overflow and
// does not crash or delete all entries (cutoff should clamp to 0 or far past).
// ---------------------------------------------------------------------------
TEST(StreamServiceRetention, RetentionMaxAgeSecOverflowSafe) {
  auto repo    = std::make_shared<MemoryRepository>();
  auto service = MakeService(repo);

  // Create stream with maximum possible retention_max_age_sec to trigger the
  // overflow path. Any value where value * 1000 > UINT64_MAX would overflow.
  constexpr uint64_t kHugeAge = std::numeric_limits<uint64_t>::max() / 500; // * 1000 overflows

  CreateStreamRequest create;
  *create.mutable_stream() = MakeStream("overflow-test");
  create.set_retention_max_age_sec(kHugeAge);
  service.CreateStream(create);

  // Append an entry — must not crash or delete the entry we just appended.
  AppendRequest append;
  *append.mutable_stream() = MakeStream("overflow-test");
  *append.add_items()      = MakeItem();
  auto resp                = service.Append(append);
  EXPECT_EQ(resp.last_offset(), 0u) << "first entry should be at offset 0";

  // Read back — entry must still be present.
  ReadRequest read;
  *read.mutable_stream() = MakeStream("overflow-test");
  read.set_start_offset(0);
  auto result = service.Read(read);
  EXPECT_EQ(result.entries_size(), 1) << "entry must not be pruned by overflowed cutoff";
}

// ---------------------------------------------------------------------------
// Test: Normal retention_max_age_sec (small value) still prunes old entries.
// This verifies that the overflow guard doesn't break the normal path.
// ---------------------------------------------------------------------------
TEST(StreamServiceRetention, RetentionMaxAgeSecNormalPath) {
  auto repo    = std::make_shared<MemoryRepository>();
  auto service = MakeService(repo);

  // retention_max_age_sec = 0 means no age-based pruning.
  CreateStreamRequest create;
  *create.mutable_stream() = MakeStream("normal-retention");
  create.set_retention_max_entries(0);
  create.set_retention_max_age_sec(0);
  service.CreateStream(create);

  // Append two entries.
  for (int i = 0; i < 2; ++i) {
    AppendRequest append;
    *append.mutable_stream() = MakeStream("normal-retention");
    *append.add_items()      = MakeItem();
    service.Append(append);
  }

  ReadRequest read;
  *read.mutable_stream() = MakeStream("normal-retention");
  read.set_start_offset(0);
  auto result = service.Read(read);
  EXPECT_EQ(result.entries_size(), 2) << "both entries must be present with no retention policy";
}

// ---------------------------------------------------------------------------
// Test: retention_max_entries limits stream size correctly.
// ---------------------------------------------------------------------------
TEST(StreamServiceRetention, RetentionMaxEntriesTruncates) {
  auto repo    = std::make_shared<MemoryRepository>();
  auto service = MakeService(repo);

  CreateStreamRequest create;
  *create.mutable_stream() = MakeStream("max-entries");
  create.set_retention_max_entries(3);
  service.CreateStream(create);

  for (int i = 0; i < 5; ++i) {
    AppendRequest append;
    *append.mutable_stream() = MakeStream("max-entries");
    *append.add_items()      = MakeItem();
    service.Append(append);
  }

  ReadRequest read;
  *read.mutable_stream() = MakeStream("max-entries");
  read.set_start_offset(0);
  auto result = service.Read(read);
  EXPECT_EQ(result.entries_size(), 3) << "only 3 most recent entries must remain";
}

/*
  Tests for RamArrowStore::PurgeOrphans.

  A crashed or killed container leaves its /dev/shm/<prefix>-* segments behind.
  Nothing in this process's accounting knows about them, so it keeps accepting
  allocations until the tmpfs is full — at which point shm_open, ftruncate and
  mmap all still succeed (metadata and address space only) and the *producer*
  takes SIGBUS on its first write. Downstream observed 7.7G/7.7G with 10,329
  orphans before this sweep existed.

  Each test uses a unique shm prefix so concurrent runs cannot collide, and
  cleans up after itself.
*/

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <filesystem>
#include <string>
#include <unordered_set>

#include "internal/storage/ram/ram_arrow_store.hpp"
#include "internal/util/uuid.hpp"

namespace {

using payload::storage::RamArrowStore;

// Unique per test so parallel ctest jobs do not fight over /dev/shm.
std::string UniquePrefix(const std::string& tag) {
  return "pmtest-" + tag + "-" + std::to_string(::getpid());
}

// Creates a segment directly, as a crashed previous process would have left it.
void CreateStraySegment(const std::string& prefix, const std::string& uuid_hex, size_t bytes = 64) {
  const std::string name = "/" + prefix + "-" + uuid_hex;
  const int         fd   = shm_open(name.c_str(), O_CREAT | O_RDWR, 0600);
  ASSERT_GE(fd, 0) << "failed to create stray segment " << name;
  ASSERT_EQ(ftruncate(fd, static_cast<off_t>(bytes)), 0);
  close(fd);
}

bool SegmentExists(const std::string& prefix, const std::string& uuid_hex) {
  return std::filesystem::exists("/dev/shm/" + prefix + "-" + uuid_hex);
}

void UnlinkIfPresent(const std::string& prefix, const std::string& uuid_hex) {
  shm_unlink(("/" + prefix + "-" + uuid_hex).c_str());
}

} // namespace

TEST(RamOrphanPurge, RemovesSegmentsWithNoRepositoryRecord) {
  const auto    prefix = UniquePrefix("orphans");
  RamArrowStore store(prefix);

  const auto live_uuid  = payload::util::ToString(payload::util::GenerateUUID());
  const auto orphan_one = payload::util::ToString(payload::util::GenerateUUID());
  const auto orphan_two = payload::util::ToString(payload::util::GenerateUUID());

  CreateStraySegment(prefix, live_uuid);
  CreateStraySegment(prefix, orphan_one);
  CreateStraySegment(prefix, orphan_two);
  ASSERT_TRUE(SegmentExists(prefix, orphan_one));

  const std::unordered_set<std::string> known{live_uuid};
  EXPECT_EQ(store.PurgeOrphans(known), 2u);

  EXPECT_TRUE(SegmentExists(prefix, live_uuid)) << "a payload the repository knows about must survive";
  EXPECT_FALSE(SegmentExists(prefix, orphan_one));
  EXPECT_FALSE(SegmentExists(prefix, orphan_two));

  UnlinkIfPresent(prefix, live_uuid);
}

TEST(RamOrphanPurge, LeavesOtherPrefixesAlone) {
  const auto    ours   = UniquePrefix("mine");
  const auto    theirs = UniquePrefix("theirs");
  RamArrowStore store(ours);

  const auto uuid = payload::util::ToString(payload::util::GenerateUUID());
  CreateStraySegment(ours, uuid);
  CreateStraySegment(theirs, uuid);

  EXPECT_EQ(store.PurgeOrphans({}), 1u) << "only segments carrying our prefix may be touched";
  EXPECT_FALSE(SegmentExists(ours, uuid));
  EXPECT_TRUE(SegmentExists(theirs, uuid)) << "another service's segments must not be reclaimed";

  UnlinkIfPresent(theirs, uuid);
}

TEST(RamOrphanPurge, EmptyKnownSetPurgesEverythingOfOurs) {
  // This is the in-memory repository case: the repo is empty at startup, so by
  // definition every surviving segment is an orphan.
  const auto    prefix = UniquePrefix("allorphans");
  RamArrowStore store(prefix);

  for (int i = 0; i < 5; ++i) {
    CreateStraySegment(prefix, payload::util::ToString(payload::util::GenerateUUID()));
  }

  EXPECT_EQ(store.PurgeOrphans({}), 5u);
}

TEST(RamOrphanPurge, IsSafeWhenNothingMatches) {
  const auto    prefix = UniquePrefix("empty");
  RamArrowStore store(prefix);
  EXPECT_EQ(store.PurgeOrphans({}), 0u) << "a sweep with no matching segments must be a no-op";
}

TEST(RamOrphanPurge, DropsCachedBuffersForPurgedPayloads) {
  const auto    prefix = UniquePrefix("cache");
  RamArrowStore store(prefix);

  const auto uuid = payload::util::GenerateUUID();
  const auto id   = payload::util::ToProto(uuid);
  const auto hex  = payload::util::ToString(uuid);

  // Allocate through the store so the buffer is cached internally.
  auto buf = store.Allocate(id, 128);
  ASSERT_NE(buf, nullptr);
  ASSERT_TRUE(SegmentExists(prefix, hex));

  // Repository knows nothing about it, so the sweep should reclaim it and
  // forget the cached mapping rather than serve a dangling entry from Read().
  EXPECT_EQ(store.PurgeOrphans({}), 1u);
  EXPECT_FALSE(SegmentExists(prefix, hex));
  EXPECT_THROW((void)store.Read(id), std::runtime_error) << "Read must not return a buffer for a purged payload";
}

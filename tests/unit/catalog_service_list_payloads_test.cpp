/*
  Unit tests for CatalogService::ListPayloads pagination.

  Covers:
    - total_count reflects all matching records regardless of page size
    - next_page_token is absent on the last page, present otherwise
    - offset navigation: page 0, page 1, last page
    - tier_filter is applied before counting and paginating
    - page_size clamping: 0 → server default (50), >500 → 500
    - invalid page_token falls back to offset 0
*/

#include "internal/service/catalog_service.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <unordered_set>

#include "internal/db/memory/memory_repository.hpp"
#include "internal/db/model/payload_record.hpp"
#include "internal/service/service_context.hpp"
#include "internal/util/uuid.hpp"
#include "payload/manager/v1.hpp"

namespace {

using payload::manager::v1::ListPayloadsRequest;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_RAM;
using payload::manager::v1::TIER_UNSPECIFIED;
using payload::manager::v1::PAYLOAD_STATE_ACTIVE;

payload::service::ServiceContext MakeCtx() {
  payload::service::ServiceContext ctx;
  ctx.repository = std::make_shared<payload::db::memory::MemoryRepository>();
  return ctx;
}

// Insert n payload records with the given tier into the repository.
// created_at_ms is staggered so ORDER BY created_at_ms DESC is deterministic.
void InsertPayloads(const payload::service::ServiceContext& ctx,
                    int n,
                    payload::manager::v1::Tier tier = TIER_RAM) {
  auto base_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());

  for (int i = 0; i < n; ++i) {
    payload::db::model::PayloadRecord r;
    r.id             = payload::util::GenerateUUID();
    r.tier           = tier;
    r.state          = PAYLOAD_STATE_ACTIVE;
    r.size_bytes     = 64;
    r.version        = 1;
    r.created_at_ms  = base_ms + static_cast<uint64_t>(i); // ascending so newest last

    auto tx = ctx.repository->Begin();
    ctx.repository->InsertPayload(*tx, r);
    tx->Commit();
  }
}

ListPayloadsRequest MakeReq(int page_size = 0,
                            const std::string& page_token = "",
                            payload::manager::v1::Tier tier = TIER_UNSPECIFIED) {
  ListPayloadsRequest req;
  req.set_page_size(page_size);
  req.set_page_token(page_token);
  req.set_tier_filter(tier);
  return req;
}

} // namespace

// ---------------------------------------------------------------------------
// Basic count and single-page response
// ---------------------------------------------------------------------------

TEST(ListPayloads, TotalCountMatchesInserted) {
  auto ctx = MakeCtx();
  InsertPayloads(ctx, 7);

  payload::service::CatalogService svc(ctx);
  const auto resp = svc.ListPayloads(MakeReq(50));

  EXPECT_EQ(resp.total_count(), 7);
  EXPECT_EQ(resp.payloads_size(), 7);
  EXPECT_TRUE(resp.next_page_token().empty()) << "single page: no next token";
}

TEST(ListPayloads, EmptyRepositoryReturnsZeroCount) {
  auto ctx = MakeCtx();
  payload::service::CatalogService svc(ctx);

  const auto resp = svc.ListPayloads(MakeReq(50));
  EXPECT_EQ(resp.total_count(), 0);
  EXPECT_EQ(resp.payloads_size(), 0);
  EXPECT_TRUE(resp.next_page_token().empty());
}

// ---------------------------------------------------------------------------
// next_page_token presence
// ---------------------------------------------------------------------------

TEST(ListPayloads, NextPageTokenPresentWhenMorePagesExist) {
  auto ctx = MakeCtx();
  InsertPayloads(ctx, 10);

  payload::service::CatalogService svc(ctx);
  const auto resp = svc.ListPayloads(MakeReq(3)); // page_size=3, 10 total → 4 pages

  EXPECT_EQ(resp.total_count(), 10);
  EXPECT_EQ(resp.payloads_size(), 3);
  EXPECT_FALSE(resp.next_page_token().empty()) << "more pages remain";
}

TEST(ListPayloads, NextPageTokenAbsentOnLastPage) {
  auto ctx = MakeCtx();
  InsertPayloads(ctx, 6);

  payload::service::CatalogService svc(ctx);
  // First page: offset=0, size=4 → next_page_token="4"
  const auto page1 = svc.ListPayloads(MakeReq(4));
  ASSERT_FALSE(page1.next_page_token().empty());

  // Second page: offset=4, size=4 → only 2 rows remain → last page
  const auto page2 = svc.ListPayloads(MakeReq(4, page1.next_page_token()));
  EXPECT_EQ(page2.total_count(), 6);
  EXPECT_EQ(page2.payloads_size(), 2);
  EXPECT_TRUE(page2.next_page_token().empty()) << "last page: no next token";
}

TEST(ListPayloads, ExactlyOnePageHasNoNextToken) {
  auto ctx = MakeCtx();
  InsertPayloads(ctx, 5);

  payload::service::CatalogService svc(ctx);
  const auto resp = svc.ListPayloads(MakeReq(5)); // exactly fits

  EXPECT_EQ(resp.total_count(), 5);
  EXPECT_EQ(resp.payloads_size(), 5);
  EXPECT_TRUE(resp.next_page_token().empty());
}

// ---------------------------------------------------------------------------
// Offset navigation
// ---------------------------------------------------------------------------

TEST(ListPayloads, PageTwoContainsCorrectRecords) {
  auto ctx = MakeCtx();
  InsertPayloads(ctx, 9);

  payload::service::CatalogService svc(ctx);
  const auto page1 = svc.ListPayloads(MakeReq(4));
  ASSERT_EQ(page1.payloads_size(), 4);
  ASSERT_FALSE(page1.next_page_token().empty());

  const auto page2 = svc.ListPayloads(MakeReq(4, page1.next_page_token()));
  ASSERT_EQ(page2.payloads_size(), 4);
  ASSERT_FALSE(page2.next_page_token().empty());

  const auto page3 = svc.ListPayloads(MakeReq(4, page2.next_page_token()));
  EXPECT_EQ(page3.payloads_size(), 1); // 9 - 8 = 1 remaining
  EXPECT_TRUE(page3.next_page_token().empty());

  // Validate no UUID overlap across pages
  std::unordered_set<std::string> seen;
  for (const auto& p : page1.payloads()) seen.insert(p.id().value());
  for (const auto& p : page2.payloads()) EXPECT_TRUE(seen.insert(p.id().value()).second) << "duplicate UUID on page 2";
  for (const auto& p : page3.payloads()) EXPECT_TRUE(seen.insert(p.id().value()).second) << "duplicate UUID on page 3";
  EXPECT_EQ(static_cast<int>(seen.size()), 9);
}

TEST(ListPayloads, TotalCountConsistentAcrossPages) {
  auto ctx = MakeCtx();
  InsertPayloads(ctx, 12);

  payload::service::CatalogService svc(ctx);
  const auto page1 = svc.ListPayloads(MakeReq(5));
  const auto page2 = svc.ListPayloads(MakeReq(5, page1.next_page_token()));
  const auto page3 = svc.ListPayloads(MakeReq(5, page2.next_page_token()));

  EXPECT_EQ(page1.total_count(), 12);
  EXPECT_EQ(page2.total_count(), 12);
  EXPECT_EQ(page3.total_count(), 12);
}

// ---------------------------------------------------------------------------
// Tier filter with pagination
// ---------------------------------------------------------------------------

TEST(ListPayloads, TierFilterAppliedBeforePagination) {
  auto ctx = MakeCtx();
  InsertPayloads(ctx, 6, TIER_RAM);
  InsertPayloads(ctx, 4, TIER_DISK);

  payload::service::CatalogService svc(ctx);

  const auto ram_resp = svc.ListPayloads(MakeReq(50, "", TIER_RAM));
  EXPECT_EQ(ram_resp.total_count(), 6);
  EXPECT_EQ(ram_resp.payloads_size(), 6);

  const auto disk_resp = svc.ListPayloads(MakeReq(50, "", TIER_DISK));
  EXPECT_EQ(disk_resp.total_count(), 4);
  EXPECT_EQ(disk_resp.payloads_size(), 4);

  const auto all_resp = svc.ListPayloads(MakeReq(50));
  EXPECT_EQ(all_resp.total_count(), 10);
}

TEST(ListPayloads, TierFilterPaginationNextToken) {
  auto ctx = MakeCtx();
  InsertPayloads(ctx, 5, TIER_RAM);
  InsertPayloads(ctx, 5, TIER_DISK);

  payload::service::CatalogService svc(ctx);

  // Page through only RAM payloads, 2 at a time
  const auto p1 = svc.ListPayloads(MakeReq(2, "", TIER_RAM));
  EXPECT_EQ(p1.total_count(), 5);
  EXPECT_EQ(p1.payloads_size(), 2);
  ASSERT_FALSE(p1.next_page_token().empty());
  for (const auto& p : p1.payloads()) EXPECT_EQ(p.tier(), TIER_RAM);

  const auto p2 = svc.ListPayloads(MakeReq(2, p1.next_page_token(), TIER_RAM));
  EXPECT_EQ(p2.payloads_size(), 2);
  ASSERT_FALSE(p2.next_page_token().empty());

  const auto p3 = svc.ListPayloads(MakeReq(2, p2.next_page_token(), TIER_RAM));
  EXPECT_EQ(p3.payloads_size(), 1);
  EXPECT_TRUE(p3.next_page_token().empty());
}

// ---------------------------------------------------------------------------
// page_size clamping
// ---------------------------------------------------------------------------

TEST(ListPayloads, PageSizeZeroUsesServerDefault) {
  auto ctx = MakeCtx();
  InsertPayloads(ctx, 10);

  payload::service::CatalogService svc(ctx);
  // page_size=0 → server default (50) → all 10 fit on one page
  const auto resp = svc.ListPayloads(MakeReq(0));
  EXPECT_EQ(resp.payloads_size(), 10);
  EXPECT_TRUE(resp.next_page_token().empty());
}

TEST(ListPayloads, PageSizeAbove500ClampedTo500) {
  auto ctx = MakeCtx();
  InsertPayloads(ctx, 10);

  payload::service::CatalogService svc(ctx);
  const auto resp = svc.ListPayloads(MakeReq(9999));
  // Clamped to 500, but only 10 records — all returned, no next page
  EXPECT_EQ(resp.payloads_size(), 10);
  EXPECT_TRUE(resp.next_page_token().empty());
}

// ---------------------------------------------------------------------------
// Invalid / garbage page_token
// ---------------------------------------------------------------------------

TEST(ListPayloads, InvalidPageTokenFallsBackToFirstPage) {
  auto ctx = MakeCtx();
  InsertPayloads(ctx, 5);

  payload::service::CatalogService svc(ctx);
  const auto resp = svc.ListPayloads(MakeReq(50, "not-a-number"));
  // Should not throw; treats as offset=0
  EXPECT_EQ(resp.total_count(), 5);
  EXPECT_EQ(resp.payloads_size(), 5);
}

TEST(ListPayloads, NegativePageTokenFallsBackToFirstPage) {
  auto ctx = MakeCtx();
  InsertPayloads(ctx, 5);

  payload::service::CatalogService svc(ctx);
  const auto resp = svc.ListPayloads(MakeReq(50, "-10"));
  EXPECT_EQ(resp.total_count(), 5);
  EXPECT_EQ(resp.payloads_size(), 5);
}

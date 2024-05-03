/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public License v3.
 */

#include "backend/common/page_manager.h"

#include "backend/common/file.h"
#include "backend/common/page.h"
#include "backend/common/page_id.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend {
namespace {

using ::testing::FieldsAre;
using ::testing::IsOkAndHolds;
using ::testing::Return;
using ::testing::StatusIs;

using Page = RawPage<>;
using TestPagePool = PagePool<InMemoryFile<kFileSystemPageSize>>;
using TestPageManager = PageManager<TestPagePool>;

TEST(PageManager, AllocatedPagesAreDistinct) {
  TestPageManager manager;
  ASSERT_OK_AND_ASSIGN((auto [id1, page1]), manager.New<Page>());
  ASSERT_OK_AND_ASSIGN((auto [id2, page2]), manager.New<Page>());
  EXPECT_NE(id1, id2);
  EXPECT_NE(&page1, &page2);
}

TEST(PageManager, AllocationsCanReturnPageIdsDirectly) {
  TestPageManager manager;
  ASSERT_OK_AND_ASSIGN(PageId id1, manager.New<Page>());
  ASSERT_OK_AND_ASSIGN(PageId id2, manager.New<Page>());
  EXPECT_NE(id1, id2);
}

TEST(PageManager, AllocationsCanReturnPageReferencesDirectly) {
  TestPageManager manager;
  ASSERT_OK_AND_ASSIGN(Page & page1, manager.New<Page>());
  ASSERT_OK_AND_ASSIGN(Page & page2, manager.New<Page>());
  EXPECT_NE(&page1, &page2);
}

TEST(PageManager, PageIdsAreResolvedToCorrespondingPages) {
  TestPageManager manager;
  ASSERT_OK_AND_ASSIGN((auto [id1, page1]), manager.New<Page>());
  ASSERT_OK_AND_ASSIGN((auto [id2, page2]), manager.New<Page>());

  // This assumes that pages are not evicted during the test.
  ASSERT_OK_AND_ASSIGN(Page & reload1, manager.Get<Page>(id1));
  ASSERT_OK_AND_ASSIGN(Page & reload2, manager.Get<Page>(id2));

  EXPECT_EQ(&page1, &reload1);
  EXPECT_EQ(&page2, &reload2);
}

// A mock version of a page pool to test PageManager internals.
class MockPagePool {
 public:
  template <typename Page>
  StatusOrRef<Page> Get(PageId id) {
    return mock_->Get(id);
  }

  auto MarkAsDirty(PageId id) { return mock_->MarkAsDirty(id); }
  auto Flush() { return mock_->Flush(); }
  auto Close() { return mock_->Close(); }

  auto& GetMock() { return *mock_; }

 private:
  class Mock {
   public:
    MOCK_METHOD(StatusOrRef<Page>, Get, (PageId id));
    MOCK_METHOD(void, MarkAsDirty, (PageId id));
    MOCK_METHOD(absl::Status, Flush, ());
    MOCK_METHOD(absl::Status, Close, ());
  };

  // Mock is wrapped in unique_ptr to allow move semantics.
  std::unique_ptr<Mock> mock_{std::make_unique<Mock>()};
};

TEST(PageManager, NewPageProducesFreshIdAndLoadsMatchingPage) {
  MockPagePool pool;
  auto& mock = pool.GetMock();
  PageManager<MockPagePool> manager(std::move(pool));

  Page page0;
  Page page1;
  EXPECT_CALL(mock, Get(0)).WillOnce(Return(StatusOrRef<Page>(page0)));
  EXPECT_CALL(mock, Get(1)).WillOnce(Return(StatusOrRef<Page>(page1)));

  EXPECT_THAT(manager.New<Page>(),
              IsOkAndHolds(FieldsAre(0, testing::Address(&page0))));
  EXPECT_THAT(manager.New<Page>(),
              IsOkAndHolds(FieldsAre(1, testing::Address(&page1))));
}

TEST(PageManager, StartingOffsetOfPageManagerIsUsed) {
  MockPagePool pool;
  auto& mock = pool.GetMock();
  PageManager<MockPagePool> manager(std::move(pool), /*next=*/42);

  Page page42;
  Page page43;
  EXPECT_CALL(mock, Get(42)).WillOnce(Return(StatusOrRef<Page>(page42)));
  EXPECT_CALL(mock, Get(43)).WillOnce(Return(StatusOrRef<Page>(page43)));

  EXPECT_THAT(manager.New<Page>(),
              IsOkAndHolds(FieldsAre(42, testing::Address(&page42))));
  EXPECT_THAT(manager.New<Page>(),
              IsOkAndHolds(FieldsAre(43, testing::Address(&page43))));
}

TEST(PageManager, PageLookupFailureIsForwardedInNew) {
  MockPagePool pool;
  auto& mock = pool.GetMock();
  PageManager<MockPagePool> manager(std::move(pool), 12);

  EXPECT_CALL(mock, Get(12)).WillOnce(Return(absl::InternalError("test")));
  EXPECT_THAT(manager.New<Page>(),
              StatusIs(absl::StatusCode::kInternal, "test"));
}

TEST(PageManager, GetIsForwarded) {
  MockPagePool pool;
  auto& mock = pool.GetMock();
  PageManager<MockPagePool> manager(std::move(pool));

  Page page;
  EXPECT_CALL(mock, Get(2)).WillOnce(Return(StatusOrRef<Page>(page)));
  ASSERT_OK_AND_ASSIGN(Page * ptr, manager.Get<Page>(2));
  EXPECT_EQ(ptr, &page);

  auto error = absl::InternalError("test");
  EXPECT_CALL(mock, Get(5)).WillOnce(Return(error));
  EXPECT_THAT(manager.Get<Page>(5), error);
}

TEST(PageManager, MarkAsDirtyIsForwarded) {
  MockPagePool pool;
  auto& mock = pool.GetMock();
  PageManager<MockPagePool> manager(std::move(pool));

  EXPECT_CALL(mock, MarkAsDirty(2));
  manager.MarkAsDirty(2);
}

TEST(PageManager, FlushIsForwarded) {
  MockPagePool pool;
  auto& mock = pool.GetMock();
  PageManager<MockPagePool> manager(std::move(pool));

  auto error = absl::InternalError("test");
  EXPECT_CALL(mock, Flush()).WillOnce(Return(error));
  EXPECT_THAT(manager.Flush(), error);
}

TEST(PageManager, CloseIsForwarded) {
  MockPagePool pool;
  auto& mock = pool.GetMock();
  PageManager<MockPagePool> manager(std::move(pool));

  auto error = absl::InternalError("test");
  EXPECT_CALL(mock, Close()).WillOnce(Return(error));
  EXPECT_THAT(manager.Close(), error);
}

}  // namespace
}  // namespace carmen::backend

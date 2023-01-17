#include "backend/common/page_manager.h"

#include "backend/common/file.h"
#include "backend/common/page.h"
#include "backend/common/page_id.h"
#include "common/status_test_util.h"
#include "gtest/gtest.h"

namespace carmen::backend {
namespace {

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

}  // namespace
}  // namespace carmen::backend

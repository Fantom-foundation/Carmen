#include "backend/store/file/page_pool.h"

#include <filesystem>
#include <sstream>

#include "backend/store/file/file.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using TestPool = PagePool<int, InMemoryFile, 40>;

TEST(PagePoolTest, PoolSizeCanBeDefined) {
  TestPool pool_a(12);
  EXPECT_EQ(12, pool_a.GetPoolSize());
  TestPool pool_b(4);
  EXPECT_EQ(4, pool_b.GetPoolSize());
}

TEST(PagePoolTest, PagesCanBeFetched) {
  TestPool pool(2);
  auto& page_12 = pool.Get(12);
  auto& page_14 = pool.Get(14);
  EXPECT_NE(&page_12, &page_14);
}

TEST(PagePoolTest, FreshFetchedPagesAreZeroInitialized) {
  TestPool pool(2);
  auto& page_12 = pool.Get(12);
  for (int i = 0; i < 4; i++) {
    EXPECT_EQ(0, page_12[i]);
  }
}

TEST(PagePoolTest, PagesAreEvictedAndReloadedCorrectly) {
  constexpr int kNumSteps = 4;
  static_assert(TestPool::Page::kNumElementsPerPage >= 2);
  TestPool pool(2);

  // Write data to kNumSteps pages;
  for (int i = 0; i < kNumSteps; i++) {
    auto& page = pool.Get(i);
    page[0] = i;
    page[1] = i + 1;
    pool.MarkAsDirty(i);
  }

  // Fetch those kNumSteps pages and check the content
  for (int i = 0; i < kNumSteps; i++) {
    auto& page = pool.Get(i);
    EXPECT_EQ(i, page[0]);
    EXPECT_EQ(i + 1, page[1]);
  }
}

}  // namespace
}  // namespace carmen::backend::store

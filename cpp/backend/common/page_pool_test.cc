#include "backend/common/page_pool.h"

#include <filesystem>
#include <sstream>

#include "backend/common/file.h"
#include "backend/common/page.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend {
namespace {

using ::testing::_;
using ::testing::NiceMock;
using ::testing::Sequence;

using Page = ArrayPage<int, 40>;
using TestPool = PagePool<Page, InMemoryFile>;
using TestPoolListener = PagePoolListener<Page>;

TEST(PagePoolTest, TypeProperties) {
  EXPECT_TRUE(std::is_move_constructible_v<TestPool>);
  EXPECT_TRUE(std::is_move_assignable_v<TestPool>);
}

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

class MockListener : public TestPoolListener {
 public:
  MOCK_METHOD(void, AfterLoad, (PageId id, const Page& page), (override));
  MOCK_METHOD(void, BeforeEvict, (PageId id, const Page& page, bool is_dirty),
              (override));
};

TEST(PagePoolTest, ListenersAreNotifiedOnLoad) {
  TestPool pool(1);  // single slot pool
  auto listener = std::make_unique<NiceMock<MockListener>>();
  MockListener& mock = *listener.get();
  pool.AddListener(std::move(listener));

  // We expect to be notified about loaded pages in order.
  Sequence s;
  EXPECT_CALL(mock, AfterLoad(0, _)).InSequence(s);
  EXPECT_CALL(mock, AfterLoad(1, _)).InSequence(s);
  EXPECT_CALL(mock, AfterLoad(0, _)).InSequence(s);

  // Loads page 0 into pool, no eviction.
  pool.Get(0);

  // Loads page 1 into pool, evicts page 0, which is not dirty.
  pool.Get(1);

  // Loads page 0 into pool, evicts page 1, which is not dirty.
  pool.Get(0);
}

TEST(PagePoolTest, ListenersAreNotifiedOnEviction) {
  TestPool pool(1);  // single slot pool
  auto listener = std::make_unique<NiceMock<MockListener>>();
  MockListener& mock = *listener.get();
  pool.AddListener(std::move(listener));

  // We expect to be notified on the eviction of pages 0 and 1 in order.
  Sequence s;
  EXPECT_CALL(mock, BeforeEvict(0, _, false)).InSequence(s);
  EXPECT_CALL(mock, BeforeEvict(1, _, false)).InSequence(s);

  // Loads page 0 into pool, no eviction.
  pool.Get(0);

  // Loads page 1 into pool, evicts page 0, which is not dirty.
  pool.Get(1);

  // Loads page 0 into pool, evicts page 1, which is not dirty.
  pool.Get(0);
}

}  // namespace
}  // namespace carmen::backend

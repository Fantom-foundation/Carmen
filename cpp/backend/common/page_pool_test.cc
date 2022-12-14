#include "backend/common/page_pool.h"

#include <filesystem>
#include <optional>
#include <sstream>

#include "absl/status/status.h"
#include "backend/common/file.h"
#include "backend/common/page.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend {
namespace {

using ::testing::_;
using ::testing::InSequence;
using ::testing::NiceMock;
using ::testing::Return;
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

template <carmen::backend::Page P>
class MockFile {
 public:
  using page_type = P;
  MOCK_METHOD(std::size_t, GetNumPages, ());
  MOCK_METHOD(void, LoadPage, (PageId id, P& dest));
  MOCK_METHOD(void, StorePage, (PageId id, const P& src));
  MOCK_METHOD(absl::Status, Flush, ());
  MOCK_METHOD(absl::Status, Close, ());
};

TEST(MockFileTest, IsFile) { EXPECT_TRUE(File<MockFile<Page>>); }

TEST(PagePoolTest, FlushWritesDirtyPages) {
  auto file = std::make_unique<MockFile<Page>>();
  auto& mock = *file;
  PagePool<Page, MockFile> pool(std::move(file), 2);

  EXPECT_CALL(mock, LoadPage(10, _));
  EXPECT_CALL(mock, LoadPage(20, _));
  EXPECT_CALL(mock, StorePage(10, _));
  EXPECT_CALL(mock, StorePage(20, _));

  pool.Get(10);
  pool.Get(20);
  pool.MarkAsDirty(10);
  pool.MarkAsDirty(20);

  ASSERT_OK(pool.Flush());
}

TEST(PagePoolTest, FlushResetsPageState) {
  auto file = std::make_unique<MockFile<Page>>();
  auto& mock = *file;
  PagePool<Page, MockFile> pool(std::move(file), 2);

  EXPECT_CALL(mock, LoadPage(10, _));
  EXPECT_CALL(mock, StorePage(10, _));

  pool.Get(10);
  pool.MarkAsDirty(10);

  ASSERT_OK(pool.Flush());
  ASSERT_OK(pool.Flush());  // < not written a second time
}

TEST(PagePoolTest, CleanPagesAreNotFlushed) {
  auto file = std::make_unique<MockFile<Page>>();
  auto& mock = *file;
  PagePool<Page, MockFile> pool(std::move(file), 2);

  EXPECT_CALL(mock, LoadPage(10, _));
  EXPECT_CALL(mock, LoadPage(20, _));
  EXPECT_CALL(mock, StorePage(20, _));

  pool.Get(10);
  pool.Get(20);
  pool.MarkAsDirty(20);

  ASSERT_OK(pool.Flush());
}

TEST(PagePoolTest, ClosingPoolFlushesPagesAndClosesFile) {
  auto file = std::make_unique<MockFile<Page>>();
  auto& mock = *file;
  PagePool<Page, MockFile> pool(std::move(file), 2);

  EXPECT_CALL(mock, LoadPage(10, _));
  EXPECT_CALL(mock, LoadPage(20, _));
  EXPECT_CALL(mock, StorePage(20, _));
  EXPECT_CALL(mock, Close());

  pool.Get(10);
  pool.Get(20);
  pool.MarkAsDirty(20);

  ASSERT_OK(pool.Close());
}

class MockEvictionPolicy {
 public:
  MockEvictionPolicy(std::size_t = 0) {}
  MOCK_METHOD(void, Read, (std::size_t));
  MOCK_METHOD(void, Written, (std::size_t));
  MOCK_METHOD(void, Removed, (std::size_t));
  MOCK_METHOD(std::optional<std::size_t>, GetPageToEvict, ());
};

TEST(MockEvictionPolicy, IsEvictionPolicy) {
  EXPECT_TRUE(EvictionPolicy<MockEvictionPolicy>);
}

TEST(PagePoolTest, EvictionPolicyIsInformedAboutRead) {
  PagePool<Page, InMemoryFile, MockEvictionPolicy> pool(2);
  auto& mock = pool.GetEvictionPolicy();

  // This assumes that unused pages are used in order.
  Sequence s;
  EXPECT_CALL(mock, Read(0)).InSequence(s);
  EXPECT_CALL(mock, Read(1)).InSequence(s);
  EXPECT_CALL(mock, Read(0)).InSequence(s);

  pool.Get(10);
  pool.Get(20);
  pool.Get(10);
}

TEST(PagePoolTest, EvictionPolicyIsInformedAboutWrite) {
  PagePool<Page, InMemoryFile, MockEvictionPolicy> pool(2);
  auto& mock = pool.GetEvictionPolicy();

  // This assumes that unused pages are used in order.
  {
    InSequence s;
    EXPECT_CALL(mock, Read(0));
    EXPECT_CALL(mock, Written(0));
    EXPECT_CALL(mock, Read(1));
    EXPECT_CALL(mock, Written(1));
  }

  pool.Get(10);
  pool.MarkAsDirty(10);
  pool.Get(20);
  pool.MarkAsDirty(20);
}

TEST(PagePoolTest, OnEvictionPolicyIsConsultedAndInformed) {
  PagePool<Page, InMemoryFile, MockEvictionPolicy> pool(2);
  auto& mock = pool.GetEvictionPolicy();

  // This assumes that unused pages are used in order.
  {
    InSequence s;
    EXPECT_CALL(mock, Read(0));
    EXPECT_CALL(mock, Read(1));
    EXPECT_CALL(mock, GetPageToEvict()).WillOnce(Return(1));
    EXPECT_CALL(mock, Removed(1));
    EXPECT_CALL(mock, Read(1));
    EXPECT_CALL(mock, GetPageToEvict()).WillOnce(Return(0));
    EXPECT_CALL(mock, Removed(0));
    EXPECT_CALL(mock, Read(0));
  }

  pool.Get(10);
  pool.Get(20);
  pool.Get(30);
  pool.Get(40);
}

TEST(PagePoolTest, OnFallBackEvictionPolicyIsInformed) {
  PagePool<Page, InMemoryFile, MockEvictionPolicy> pool(2);
  auto& mock = pool.GetEvictionPolicy();

  // This assumes that unused pages are used in order.
  {
    InSequence s;
    EXPECT_CALL(mock, Read(0));
    EXPECT_CALL(mock, Read(1));
    EXPECT_CALL(mock, GetPageToEvict()).WillOnce(Return(std::nullopt));
    EXPECT_CALL(mock, Removed(_));
    EXPECT_CALL(mock, Read(_));
  }

  pool.Get(10);
  pool.Get(20);
  pool.Get(30);
}

}  // namespace
}  // namespace carmen::backend

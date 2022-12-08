#include "backend/index/memory/index.h"

#include "backend/index/test_util.h"
#include "common/status_test_util.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::IsOkAndHolds;
using ::testing::Pair;

using TestIndex = InMemoryIndex<int, int>;

// Instantiates common index tests for the InMemory index type.
INSTANTIATE_TYPED_TEST_SUITE_P(InMemory, IndexTest, TestIndex);

TEST(InMemoryIndexTest, SnapshotShieldsMutations) {
  TestIndex index;

  EXPECT_THAT(index.GetOrAdd(10), IsOkAndHolds(std::pair{0, true}));
  EXPECT_THAT(index.GetOrAdd(12), IsOkAndHolds(std::pair{1, true}));
  auto snapshot = index.CreateSnapshot();

  EXPECT_THAT(index.GetOrAdd(14), IsOkAndHolds(std::pair{2, true}));

  TestIndex restored(*snapshot);
  EXPECT_THAT(restored.Get(10), 0);
  EXPECT_THAT(restored.Get(12), 1);
  EXPECT_THAT(restored.GetOrAdd(14), IsOkAndHolds(std::pair{2, true}));
}

TEST(InMemoryIndexTest, SnapshotRecoveryHasSameHash) {
  TestIndex index;
  ASSERT_OK(index.GetOrAdd(10));
  ASSERT_OK_AND_ASSIGN(auto hash, index.GetHash());
  auto snapshot = index.CreateSnapshot();

  TestIndex restored(*snapshot);
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
}

TEST(InMemoryIndexTest, LargeSnapshotRecoveryWorks) {
  constexpr const int kNumElements = 100000;

  TestIndex index;
  for (int i = 0; i < kNumElements; i++) {
    EXPECT_THAT(index.GetOrAdd(i + 10), IsOkAndHolds(std::pair{i, true}));
  }
  ASSERT_OK_AND_ASSIGN(auto hash, index.GetHash());
  auto snapshot = index.CreateSnapshot();

  TestIndex restored(*snapshot);
  for (int i = 0; i < kNumElements; i++) {
    EXPECT_THAT(index.Get(i + 10), IsOkAndHolds(i));
  }
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
}

}  // namespace
}  // namespace carmen::backend::index

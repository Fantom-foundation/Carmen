#include "backend/index/memory/index.h"

#include "backend/index/test_util.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::Pair;
using TestIndex = InMemoryIndex<int, int>;

// Instantiates common index tests for the InMemory index type.
INSTANTIATE_TYPED_TEST_SUITE_P(InMemory, IndexTest, TestIndex);

TEST(InMemoryIndexTest, SnapshotShieldsMutations) {
  TestIndex index;

  EXPECT_THAT(index.GetOrAdd(10), Pair(0, true));
  EXPECT_THAT(index.GetOrAdd(12), Pair(1, true));
  auto snapshot = index.CreateSnapshot();

  EXPECT_THAT(index.GetOrAdd(14), Pair(2, true));

  TestIndex restored(*snapshot);
  EXPECT_THAT(restored.Get(10), 0);
  EXPECT_THAT(restored.Get(12), 1);
  EXPECT_THAT(restored.GetOrAdd(14), Pair(2, true));
}

TEST(InMemoryIndexTest, SnapshotRecoveryHasSameHash) {
  TestIndex index;
  index.GetOrAdd(10);
  auto hash = index.GetHash();
  auto snapshot = index.CreateSnapshot();

  TestIndex restored(*snapshot);
  EXPECT_EQ(restored.GetHash(), hash);
}

TEST(InMemoryIndexTest, LargeSnapshotRecoveryWorks) {
  constexpr const int kNumElements = 100000;

  TestIndex index;
  for (int i = 0; i < kNumElements; i++) {
    EXPECT_THAT(index.GetOrAdd(i + 10), Pair(i, true));
  }
  auto hash = index.GetHash();
  auto snapshot = index.CreateSnapshot();

  TestIndex restored(*snapshot);
  for (int i = 0; i < kNumElements; i++) {
    EXPECT_EQ(index.Get(i + 10), i);
  }
  EXPECT_EQ(restored.GetHash(), hash);
}

}  // namespace
}  // namespace carmen::backend::index

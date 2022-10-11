#include "backend/index/memory/linear_hash_index.h"

#include "backend/index/test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::Optional;

using TestIndex = InMemoryLinearHashIndex<int, int, 16>;

// Instantiates common index tests for the InMemory index type.
INSTANTIATE_TYPED_TEST_SUITE_P(InMemory, IndexTest, TestIndex);

TEST(LinearHashingIndexTest, LoadTest) {
  constexpr int N = 1000;
  TestIndex index;
  for (int i = 0; i < N; i++) {
    EXPECT_EQ((std::pair{i, true}), index.GetOrAdd(i));
  }
  for (int i = 0; i < N; i++) {
    EXPECT_EQ((std::pair{i, false}), index.GetOrAdd(i));
  }
  for (int i = 0; i < N; i++) {
    EXPECT_EQ(i, index.Get(i));
  }
}

}  // namespace
}  // namespace carmen::backend::index

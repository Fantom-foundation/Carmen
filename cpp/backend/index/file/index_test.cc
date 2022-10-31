#include "backend/index/file/index.h"

#include "backend/common/file.h"
#include "backend/index/test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using testing::Pair;

using TestIndex = FileIndex<int, int, InMemoryFile, 128>;

// Instantiates common index tests for the FileIndex index type.
INSTANTIATE_TYPED_TEST_SUITE_P(File, IndexTest, TestIndex);

TEST(FileIndexTest, FillTest) {
  constexpr int N = 1000;
  TestIndex index;
  for (int i = 0; i < N; i++) {
    EXPECT_EQ((std::pair{i, true}), index.GetOrAdd(i));
    for (int j = 0; j < N; j++) {
      if (j <= i) {
        ASSERT_EQ(index.Get(j), j) << "Inserted: " << i << "\n";
      } else {
        ASSERT_EQ(index.Get(j), std::nullopt) << "Inserted: " << i << "\n";
      }
    }
  }
}

TEST(FileIndexTest, FillTest_SmallPages) {
  using Index = FileIndex<std::uint32_t, std::uint32_t, InMemoryFile, 64>;
  constexpr int N = 1000;
  Index index;
  for (std::uint32_t i = 0; i < N; i++) {
    EXPECT_EQ((std::pair{i, true}), index.GetOrAdd(i));
    for (std::uint32_t j = 0; j <= i; j++) {
      ASSERT_EQ(index.Get(j), j) << "Inserted: " << i << "\n";
    }
  }
}

TEST(FileIndexTest, FillTest_LargePages) {
  using Index = FileIndex<std::uint32_t, std::uint32_t, InMemoryFile, 1 << 14>;
  constexpr int N = 1000;
  Index index;
  for (std::uint32_t i = 0; i < N; i++) {
    EXPECT_EQ((std::pair{i, true}), index.GetOrAdd(i));
    for (std::uint32_t j = 0; j <= i; j++) {
      ASSERT_EQ(index.Get(j), j) << "Inserted: " << i << "\n";
    }
  }
}

TEST(FileIndexTest, LastInsertedElementIsPresent) {
  // The last element being missing was observed as a bug during development.
  // This test is present to prevent this issue from being re-introduced.
  constexpr int N = 1000000;
  TestIndex index;
  for (int i = 0; i < N; i++) {
    EXPECT_EQ((std::pair{i, true}), index.GetOrAdd(i));
    ASSERT_EQ(index.Get(i), i);
  }
}

TEST(FileIndexTest, StoreCanBeSavedAndRestored) {
  using Index = FileIndex<int, int, SingleFile>;
  const int kNumElements = 100000;
  TempDir dir;
  Hash hash;
  {
    Index index(dir.GetPath());
    for (int i = 0; i < kNumElements; i++) {
      EXPECT_THAT(index.GetOrAdd(i + 5), Pair(i, true));
    }
    hash = index.GetHash();
  }
  {
    Index restored(dir.GetPath());
    EXPECT_EQ(hash, restored.GetHash());
    for (int i = 0; i < kNumElements; i++) {
      EXPECT_EQ(restored.Get(i + 5), i);
    }
  }
}

}  // namespace
}  // namespace carmen::backend::index

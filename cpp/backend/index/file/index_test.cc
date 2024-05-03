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

#include "backend/index/file/index.h"

#include "backend/common/file.h"
#include "backend/index/index_test_suite.h"
#include "backend/structure.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::_;
using ::testing::IsOkAndHolds;
using ::testing::Pair;
using ::testing::StatusIs;

using TestIndex = FileIndex<int, int, InMemoryFile, 128>;

// Instantiates common index tests for the FileIndex index type.
INSTANTIATE_TYPED_TEST_SUITE_P(File, IndexTest, TestIndex);

TEST(FileIndexTest, FillTest) {
  constexpr int N = 1000;
  Context ctx;
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto index, TestIndex::Open(ctx, dir.GetPath()));
  for (int i = 0; i < N; i++) {
    EXPECT_THAT(index.GetOrAdd(i), IsOkAndHolds(Pair(i, true)));
    for (int j = 0; j < N; j++) {
      if (j <= i) {
        EXPECT_THAT(index.Get(j), IsOkAndHolds(j)) << "Inserted: " << i << "\n";
      } else {
        EXPECT_THAT(index.Get(j), StatusIs(absl::StatusCode::kNotFound, _))
            << "Inserted: " << i << "\n";
      }
    }
  }
}

TEST(FileIndexTest, FillTest_SmallPages) {
  using Index = FileIndex<std::uint32_t, std::uint32_t, InMemoryFile, 64>;
  constexpr int N = 1000;
  Context ctx;
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto index, Index::Open(ctx, dir.GetPath()));
  for (std::uint32_t i = 0; i < N; i++) {
    EXPECT_THAT(index.GetOrAdd(i), IsOkAndHolds(Pair(i, true)));
    for (std::uint32_t j = 0; j <= i; j++) {
      EXPECT_THAT(index.Get(j), IsOkAndHolds(j)) << "Inserted: " << i << "\n";
    }
  }
}

TEST(FileIndexTest, FillTest_LargePages) {
  using Index = FileIndex<std::uint32_t, std::uint32_t, InMemoryFile, 1 << 14>;
  constexpr int N = 1000;
  Context ctx;
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto index, Index::Open(ctx, dir.GetPath()));
  for (std::uint32_t i = 0; i < N; i++) {
    EXPECT_THAT(index.GetOrAdd(i), IsOkAndHolds(Pair(i, true)));
    for (std::uint32_t j = 0; j <= i; j++) {
      EXPECT_THAT(index.Get(j), IsOkAndHolds(j)) << "Inserted: " << i << "\n";
    }
  }
}

TEST(FileIndexTest, LastInsertedElementIsPresent) {
  // The last element being missing was observed as a bug during development.
  // This test is present to prevent this issue from being re-introduced.
  constexpr int N = 1000000;
  Context ctx;
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto index, TestIndex::Open(ctx, dir.GetPath()));
  for (int i = 0; i < N; i++) {
    EXPECT_THAT(index.GetOrAdd(i), IsOkAndHolds(Pair(i, true)));
    EXPECT_THAT(index.Get(i), IsOkAndHolds(i));
  }
}

TEST(FileIndexTest, StoreCanBeSavedAndRestored) {
  using Index = FileIndex<int, int, SingleFile>;
  const int kNumElements = 100000;
  TempDir dir;
  Context ctx;
  Hash hash;
  {
    ASSERT_OK_AND_ASSIGN(auto index, Index::Open(ctx, dir.GetPath()));
    for (int i = 0; i < kNumElements; i++) {
      EXPECT_THAT(index.GetOrAdd(i + 5), IsOkAndHolds(Pair(i, true)));
    }
    ASSERT_OK_AND_ASSIGN(hash, index.GetHash());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto restored, Index::Open(ctx, dir.GetPath()));
    EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
    for (int i = 0; i < kNumElements; i++) {
      EXPECT_THAT(restored.Get(i + 5), IsOkAndHolds(i));
    }
  }
}

}  // namespace
}  // namespace carmen::backend::index

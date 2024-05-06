// Copyright (c) 2024 Fantom Foundation
// 
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
// 
// Change Date: 2028-4-16
// 
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#include "backend/index/memory/index.h"

#include "backend/index/index_test_suite.h"
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

  EXPECT_THAT(index.GetOrAdd(10), IsOkAndHolds(Pair(0, true)));
  EXPECT_THAT(index.GetOrAdd(12), IsOkAndHolds(Pair(1, true)));
  auto snapshot = index.CreateSnapshot();

  EXPECT_THAT(index.GetOrAdd(14), IsOkAndHolds(Pair(2, true)));

  TestIndex restored(*snapshot);
  EXPECT_THAT(restored.Get(10), 0);
  EXPECT_THAT(restored.Get(12), 1);
  EXPECT_THAT(restored.GetOrAdd(14), IsOkAndHolds(Pair(2, true)));
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
    EXPECT_THAT(index.GetOrAdd(i + 10), IsOkAndHolds(Pair(i, true)));
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

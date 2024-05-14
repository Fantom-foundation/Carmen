// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#include "backend/index/memory/linear_hash_index.h"

#include "backend/index/index_test_suite.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::IsOkAndHolds;

using TestIndex = InMemoryLinearHashIndex<int, int, 16>;

// Instantiates common index tests for the InMemory index type.
INSTANTIATE_TYPED_TEST_SUITE_P(InMemory, IndexTest, TestIndex);

TEST(LinearHashingIndexTest, LoadTest) {
  constexpr int N = 1000;
  TestIndex index;
  for (int i = 0; i < N; i++) {
    EXPECT_THAT(index.GetOrAdd(i), IsOkAndHolds(std::pair{i, true}));
  }
  for (int i = 0; i < N; i++) {
    EXPECT_THAT(index.GetOrAdd(i), IsOkAndHolds(std::pair{i, false}));
  }
  for (int i = 0; i < N; i++) {
    EXPECT_THAT(index.Get(i), IsOkAndHolds(i));
  }
}

}  // namespace
}  // namespace carmen::backend::index

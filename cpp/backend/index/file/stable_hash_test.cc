/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "backend/index/file/stable_hash.h"

#include "absl/container/flat_hash_set.h"
#include "backend/common/page.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

TEST(StableHash, HashHasLimitedCollisionsForIntegers) {
  // Check the number of collisions for the first N integers.
  constexpr int N = 1000000;
  StableHash<int> hash;
  int collisions = 0;
  absl::flat_hash_set<std::size_t> seen;
  for (int i = 0; i < N; i++) {
    if (!seen.insert(hash(i)).second) {
      collisions++;
    }
  }
  EXPECT_EQ(collisions, 0);  // < no collisions
}

TEST(StableHash, HashHasLimitedCollisionsForPairsOfIntegers) {
  // Check the number of collisions for the integers in N^2.
  constexpr int N = 1000;
  StableHash<std::pair<int, int>> hash;
  int collisions = 0;
  absl::flat_hash_set<std::size_t> seen;
  for (int i = 0; i < N; i++) {
    for (int j = 0; j < N; j++) {
      if (!seen.insert(hash({i, j})).second) {
        collisions++;
      }
    }
  }
  EXPECT_EQ(collisions, 0);  // < no collisions
}

TEST(StableHash, HashHasLimitedCollisionsForArraysOfIntegers) {
  // Check the number of collisions for the integers in N^3.
  constexpr int N = 100;
  StableHash<std::array<int, 3>> hash;
  int collisions = 0;
  absl::flat_hash_set<std::size_t> seen;
  for (int i = 0; i < N; i++) {
    for (int j = 0; j < N; j++) {
      for (int k = 0; k < N; k++) {
        if (!seen.insert(hash({i, j, k})).second) {
          collisions++;
        }
      }
    }
  }
  EXPECT_EQ(collisions, 0);  // < no collisions
}

}  // namespace
}  // namespace carmen::backend::index

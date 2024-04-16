/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "backend/index/memory/linear_hash_map.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::IsNull;
using ::testing::Optional;
using ::testing::Pair;
using ::testing::Pointee;

using TestIndex = LinearHashMap<int, int, 16>;

TEST(LinearHashMapTest, ElementsCanBeInserted) {
  TestIndex index;
  EXPECT_THAT(index.Insert({1, 2}), Pair(Pointee(Pair(1, 2)), true));
  EXPECT_THAT(index.Insert({3, 4}), Pair(Pointee(Pair(3, 4)), true));
}

TEST(LinearHashMapTest, IfElementsArePresentThisIsIndicated) {
  TestIndex index;
  EXPECT_THAT(index.Insert({1, 2}), Pair(Pointee(Pair(1, 2)), true));
  EXPECT_THAT(index.Insert({1, 2}), Pair(Pointee(Pair(1, 2)), false));
}

TEST(LinearHashMapTest, InsertDoesNotUpdatePresentElements) {
  TestIndex index;
  EXPECT_THAT(index.Insert({1, 2}), Pair(Pointee(Pair(1, 2)), true));
  EXPECT_THAT(index.Insert({1, 3}), Pair(Pointee(Pair(1, 2)), false));
}

TEST(LinearHashMapTest, EntriesCanBeUpdated) {
  TestIndex index;
  EXPECT_THAT(index.InsertOrAssign(1, 2), Pair(Pointee(Pair(1, 2)), true));
  EXPECT_THAT(index.InsertOrAssign(1, 3), Pair(Pointee(Pair(1, 3)), false));
}

TEST(LinearHashMapTest, FindLocatesElement) {
  TestIndex index;
  EXPECT_THAT(index.Find(1), IsNull());
  EXPECT_THAT(index.InsertOrAssign(1, 2), Pair(Pointee(Pair(1, 2)), true));
  EXPECT_THAT(index.Find(1), Pointee(Pair(1, 2)));
}

TEST(LinearHashMapTest, SubscriptLocatesElements) {
  TestIndex index;
  EXPECT_THAT(index.Insert({1, 2}), Pair(Pointee(Pair(1, 2)), true));
  EXPECT_EQ(index[1], 2);
}

TEST(LinearHashMapTest, SubscriptInitializesValus) {
  TestIndex index;
  EXPECT_EQ(index[1], 0);
  EXPECT_THAT(index.Insert({1, 2}), Pair(Pointee(Pair(1, 0)), false));
}

TEST(LinearHashMapTest, SubscriptCanUpdateValue) {
  TestIndex index;
  EXPECT_EQ(index[1], 0);
  index[1] = 2;
  EXPECT_THAT(index.Find(1), Pointee(Pair(1, 2)));
  EXPECT_EQ(index[1], 2);
}

TEST(LinearHashMapTest, SizeCountsNumberOfKeysAccurately) {
  TestIndex index;
  EXPECT_EQ(0, index.Size());
  EXPECT_THAT(index.Insert({1, 2}), Pair(Pointee(Pair(1, 2)), true));
  EXPECT_EQ(1, index.Size());
  EXPECT_THAT(index.Insert({3, 4}), Pair(Pointee(Pair(3, 4)), true));
  EXPECT_EQ(2, index.Size());
  EXPECT_THAT(index.Insert({1, 5}), Pair(Pointee(Pair(1, 2)), false));
  EXPECT_EQ(2, index.Size());
  EXPECT_THAT(index.InsertOrAssign(1, 6), Pair(Pointee(Pair(1, 6)), false));
}

TEST(LinearHashMapTest, GrowTestWithPageSize2) {
  LinearHashMap<int, int, 2> index;
  for (int i = 0; i < 1000; i++) {
    EXPECT_THAT(index.Insert({i, i}), Pair(Pointee(Pair(i, i)), true));
    EXPECT_EQ(i + 1, index.Size());
    for (int j = 0; j <= i; j++) {
      EXPECT_THAT(index[j], j);
    }
  }
}

TEST(LinearHashMapTest, GrowTestWithPageSize20) {
  LinearHashMap<int, int, 20> index;
  for (int i = 0; i < 1000; i++) {
    EXPECT_THAT(index.Insert({i, i}), Pair(Pointee(Pair(i, i)), true));
    for (int j = 0; j <= i; j++) {
      EXPECT_THAT(index[j], j);
    }
  }
}

}  // namespace
}  // namespace carmen::backend::index

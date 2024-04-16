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

#include "backend/common/cache/lru_cache.h"

#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::ElementsAre;
using ::testing::IsNull;
using ::testing::Pointee;

using TestCache = LeastRecentlyUsedCache<int, int>;

TEST(LeastRecentlyUsedCacheTest, MissingElementsReturnNullptr) {
  TestCache cache(2);
  EXPECT_THAT(cache.Get(0), IsNull());
  EXPECT_THAT(cache.Get(1), IsNull());
}

TEST(LeastRecentlyUsedCacheTest, ElementsCanBeSetAndRetrieved) {
  TestCache cache(2);
  cache.Set(0, 1);
  EXPECT_THAT(cache.Get(0), Pointee(1));
}

TEST(LeastRecentlyUsedCacheTest, KeysAreDifferentiated) {
  TestCache cache(2);
  cache.Set(0, 1);
  cache.Set(1, 2);
  EXPECT_THAT(cache.Get(0), Pointee(1));
  EXPECT_THAT(cache.Get(1), Pointee(2));
}

TEST(LeastRecentlyUsedCacheTest, KeysCanBeUpdated) {
  TestCache cache(2);
  EXPECT_THAT(cache.Get(0), IsNull());
  cache.Set(0, 1);
  EXPECT_THAT(cache.Get(0), Pointee(1));
  cache.Set(0, 2);
  EXPECT_THAT(cache.Get(0), Pointee(2));
}

TEST(LeastRecentlyUsedCacheTest, SizeLimitIsEnforced) {
  TestCache cache(2);
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre());
  cache.Set(0, 1);
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre(0));
  cache.Set(1, 2);
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre(1, 0));
  cache.Set(2, 3);
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre(2, 1));
  cache.Set(0, 4);
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre(0, 2));
}

TEST(LeastRecentlyUsedCacheTest, GetIsATouch) {
  TestCache cache(3);
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre());
  cache.Set(1, 1);
  cache.Set(2, 2);
  cache.Set(3, 3);
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre(3, 2, 1));

  // Get missing.
  EXPECT_EQ(cache.Get(0), nullptr);
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre(3, 2, 1));

  // Get first in LRU order.
  EXPECT_THAT(cache.Get(3), Pointee(3));
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre(3, 2, 1));

  // Get middle in LRU order.
  EXPECT_THAT(cache.Get(2), Pointee(2));
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre(2, 3, 1));

  // Get last in LRU order.
  EXPECT_THAT(cache.Get(1), Pointee(1));
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre(1, 2, 3));
}

TEST(LeastRecentlyUsedCacheTest, SetIsATouch) {
  TestCache cache(3);
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre());
  cache.Set(1, 1);
  cache.Set(2, 2);
  cache.Set(3, 3);
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre(3, 2, 1));

  // Set first in LRU order.
  cache.Set(3, 0);
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre(3, 2, 1));

  // Get middle in LRU order.
  cache.Set(2, 0);
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre(2, 3, 1));

  // Get last in LRU order.
  cache.Set(1, 0);
  EXPECT_THAT(cache.GetOrderedKeysForTesting(), ElementsAre(1, 2, 3));
}

}  // namespace
}  // namespace carmen::backend::index

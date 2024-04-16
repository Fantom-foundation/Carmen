/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3 
 */

#include "common/heterogenous_map.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

TEST(HeterogenousMapTest, Insert) {
  HeterogenousMap map;
  EXPECT_EQ(map.Get<int>(), 0);
  map.Set<int>(12);
  EXPECT_THAT(map.Get<int>(), 12);
}

TEST(HeterogenousMapTest, EntriesCanBeUpdated) {
  HeterogenousMap map;
  EXPECT_EQ(map.Get<int>(), 0);
  map.Set<int>(12);
  EXPECT_THAT(map.Get<int>(), 12);
  map.Set<int>(14);
  EXPECT_THAT(map.Get<int>(), 14);
}

TEST(HeterogenousMapTest, CanContainMultipleTypes) {
  HeterogenousMap map;
  EXPECT_EQ(map.Get<int>(), 0);
  EXPECT_EQ(map.Get<std::string>(), "");

  map.Set<int>(10);
  EXPECT_EQ(map.Get<int>(), 10);
  EXPECT_EQ(map.Get<std::string>(), "");

  map.Set<std::string>("hello");
  EXPECT_EQ(map.Get<int>(), 10);
  EXPECT_EQ(map.Get<std::string>(), "hello");
}

TEST(HeterogenousMapTest, ValuesCanBeRetrievedFromConstantMap) {
  HeterogenousMap map;
  const HeterogenousMap& const_map = map;

  EXPECT_EQ(const_map.Get<int>(), 0);
  map.Set<int>(12);
  EXPECT_THAT(const_map.Get<int>(), 12);
  map.Set<int>(14);
  EXPECT_THAT(const_map.Get<int>(), 14);
}

TEST(HeterogenousMapTest, RetrievedReferencesAreMutable) {
  HeterogenousMap map;

  int& value = map.Get<int>();
  value = 10;
  EXPECT_EQ(map.Get<int>(), 10);
  value = 12;
  EXPECT_EQ(map.Get<int>(), 12);
}

TEST(HeterogenousMapTest, ContainsDetectsSets) {
  HeterogenousMap map;
  EXPECT_FALSE(map.Contains<int>());
  map.Set<int>(12);
  EXPECT_TRUE(map.Contains<int>());
}

TEST(HeterogenousMapTest, ContainsDetectsElementsCreatedByGet) {
  HeterogenousMap map;
  EXPECT_FALSE(map.Contains<int>());
  map.Get<int>();
  EXPECT_TRUE(map.Contains<int>());
}

TEST(HeterogenousMapTest, ElementsCanBeRemoved) {
  HeterogenousMap map;
  EXPECT_FALSE(map.Contains<int>());
  map.Get<int>();
  EXPECT_TRUE(map.Contains<int>());
  map.Reset<int>();
  EXPECT_FALSE(map.Contains<int>());
}

}  // namespace
}  // namespace carmen

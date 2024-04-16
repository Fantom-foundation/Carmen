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

#include "backend/common/btree/entry.h"

#include <cstdint>

#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::btree {
namespace {

TEST(Entry, SizeOfKeyOnly) {
  EXPECT_EQ(sizeof(Entry<std::uint8_t>), sizeof(std::uint8_t));
  EXPECT_EQ(sizeof(Entry<std::uint16_t>), sizeof(std::uint16_t));
  EXPECT_EQ(sizeof(Entry<std::uint32_t>), sizeof(std::uint32_t));
  EXPECT_EQ(sizeof(Entry<std::uint64_t>), sizeof(std::uint64_t));
}

TEST(Entry, SizeOfKeyValuePair) {
  EXPECT_EQ((sizeof(Entry<std::uint8_t, std::uint8_t>)), 2);
  EXPECT_EQ((sizeof(Entry<std::uint16_t, std::uint8_t>)), 3);
  EXPECT_EQ((sizeof(Entry<std::uint16_t, std::uint16_t>)), 4);
  EXPECT_EQ((sizeof(Entry<std::uint32_t, std::uint8_t>)), 5);
  EXPECT_EQ((sizeof(Entry<std::uint8_t, std::uint32_t>)), 5);

  EXPECT_EQ((sizeof(Entry<Address, Value>)), 20 + 32);
}

TEST(Entry, EntriesAreTrivial) {
  EXPECT_TRUE((Trivial<Entry<int>>));
  EXPECT_TRUE((Trivial<Entry<int, int>>));
  EXPECT_TRUE((Trivial<Entry<Address, Value>>));
}

}  // namespace
}  // namespace carmen::backend::btree

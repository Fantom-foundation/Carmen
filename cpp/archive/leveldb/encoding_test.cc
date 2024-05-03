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

#include "archive/leveldb/encoding.h"

#include <array>
#include <span>

#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::archive::leveldb {
namespace {

using ::testing::ElementsAre;

TEST(Encoding, IntegersAreEncodedInBigEndianFormat) {
  std::array<char, 4> trg;
  Write(0x12345678, trg);
  EXPECT_THAT(trg, ElementsAre(0x12, 0x34, 0x56, 0x78));
}

TEST(Encoding, EncodedIntegersCanBeDecoded) {
  std::array<char, 4> trg;
  for (std::uint32_t i = 0; i < 1000; i++) {
    Write(i, trg);
    EXPECT_EQ(ReadUint32(trg), i);
  }
}

TEST(Encoding, EncodedTrivialValuesCanBeDecoded) {
  std::array<char, sizeof(Value)> trg;
  Value value{1, 2, 3, 4};
  Write(value, trg);
  EXPECT_EQ(Read<Value>(std::span<char, sizeof(Value)>(trg)), value);
  EXPECT_EQ(Read<Value>(std::span<const char, sizeof(Value)>(trg)), value);
}

}  // namespace
}  // namespace carmen::archive::leveldb

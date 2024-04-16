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

#include "archive/leveldb/keys.h"

#include <span>

#include "common/type.h"
#include "gtest/gtest.h"

namespace carmen::archive::leveldb {
namespace {

TEST(Keys, BlockIdIsEncodedUsingBigEndian) {
  BlockId id = 0x12345678;
  auto key = GetBlockKey(id);
  auto span = std::span(key).subspan(1);
  EXPECT_EQ(span[0], 0x12);
  EXPECT_EQ(span[1], 0x34);
  EXPECT_EQ(span[2], 0x56);
  EXPECT_EQ(span[3], 0x78);
}

TEST(Keys, StorageKeyEncodesValuesCorrectly) {
  Address addr{1, 2, 3, 4, 5};
  ReincarnationNumber r = 0x12345678;
  Key key{6, 7, 8, 9};
  BlockId b = 0x12345678;
  auto res = GetStorageKey(addr, r, key, b);

  EXPECT_EQ(res[0], '6');

  Address restored_addr;
  restored_addr.SetBytes(std::as_bytes(std::span<char, 20>(&res[1], 20)));
  EXPECT_EQ(addr, restored_addr);

  // The reincarnation number is encoded using big-endian order.
  auto r_span = std::span(res).subspan(1 + 20);
  EXPECT_EQ(r_span[0], 0x12);
  EXPECT_EQ(r_span[1], 0x34);
  EXPECT_EQ(r_span[2], 0x56);
  EXPECT_EQ(r_span[3], 0x78);

  Key restored_key;
  restored_key.SetBytes(
      std::as_bytes(std::span<char, 32>(&res[1 + 20 + 4], 32)));
  EXPECT_EQ(key, restored_key);

  auto span = std::span(res).subspan(1 + 20 + 4 + 32);
  EXPECT_EQ(span[0], 0x12);
  EXPECT_EQ(span[1], 0x34);
  EXPECT_EQ(span[2], 0x56);
  EXPECT_EQ(span[3], 0x78);
}

TEST(Keys, BlockIdCanBeExtractedFromBlockKey) {
  for (BlockId i = 1; i < (BlockId(1) << 31); i <<= 1) {
    auto key = GetBlockKey(i);
    EXPECT_EQ(GetBlockFromKey(key), i);
  }
}

TEST(Keys, BlockIdCanBeExtractedFromPropertyKey) {
  Address addr{};
  for (BlockId i = 1; i < (BlockId(1) << 31); i <<= 1) {
    auto key = GetBalanceKey(addr, i);
    EXPECT_EQ(GetBlockFromKey(key), i);
  }
}

TEST(Keys, AccountPrefixCanBeExtractedFromPropertyKey) {
  Address addr{1, 2, 3, 4};
  auto key = GetBalanceKey(addr, 12);
  auto span = GetAccountPrefix(key);
  EXPECT_EQ(span.data(), key.data());
  EXPECT_EQ(span.size(), 1 + sizeof(Address));
}

TEST(Keys, BlockIdCanBeExtractedFromStorageKey) {
  Address addr{};
  Key slot{};
  for (BlockId i = 1; i < (BlockId(1) << 31); i <<= 1) {
    auto key = GetStorageKey(addr, 12, slot, i);
    EXPECT_EQ(GetBlockFromKey(key), i);
  }
}

TEST(Keys, SlotKeyBeExtractedFromStorageKey) {
  Address addr{};
  Key slot{1, 2, 3, 4};
  auto key = GetStorageKey(addr, 12, slot, 0);
  EXPECT_EQ(GetSlotKey(key), slot);
}

}  // namespace
}  // namespace carmen::archive::leveldb

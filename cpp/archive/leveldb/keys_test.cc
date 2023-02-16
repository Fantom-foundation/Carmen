#include "archive/leveldb/keys.h"

#include <span>

#include "common/status_test_util.h"
#include "common/type.h"
#include "gtest/gtest.h"

namespace carmen::archive::leveldb {
namespace {

TEST(Keys, BlockIdIsEncodedUsingBigEndian) {
  BlockId id = 0x12345678;
  auto key = GetBlockKey(id);
  auto span = std::span(key).subspan(1);
  EXPECT_EQ(span[0], 0);
  EXPECT_EQ(span[1], 0);
  EXPECT_EQ(span[2], 0);
  EXPECT_EQ(span[3], 0);
  EXPECT_EQ(span[4], 0x12);
  EXPECT_EQ(span[5], 0x34);
  EXPECT_EQ(span[6], 0x56);
  EXPECT_EQ(span[7], 0x78);
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
  EXPECT_EQ(span[0], 0);
  EXPECT_EQ(span[1], 0);
  EXPECT_EQ(span[2], 0);
  EXPECT_EQ(span[3], 0);
  EXPECT_EQ(span[4], 0x12);
  EXPECT_EQ(span[5], 0x34);
  EXPECT_EQ(span[6], 0x56);
  EXPECT_EQ(span[7], 0x78);
}

TEST(Keys, BlockIdCanBeExtractedFromBlockKey) {
  for (BlockId i = 1; i < (BlockId(1) << 31); i <<= 1) {
    auto key = GetBlockKey(i);
    EXPECT_EQ(GetBlockId(key), i);
  }
}

TEST(Keys, BlockIdCanBeExtractedFromPropertyKey) {
  Address addr{};
  for (BlockId i = 1; i < (BlockId(1) << 31); i <<= 1) {
    auto key = GetBalanceKey(addr, i);
    EXPECT_EQ(GetBlockId(key), i);
  }
}

TEST(Keys, BlockIdCanBeExtractedFromStorageKey) {
  Address addr{};
  Key slot{};
  for (BlockId i = 1; i < (BlockId(1) << 31); i <<= 1) {
    auto key = GetStorageKey(addr, 12, slot, i);
    EXPECT_EQ(GetBlockId(key), i);
  }
}

TEST(AccountState, ReincarnationNumberCanBeEncodedAndDecoded) {
  AccountState state;
  for (ReincarnationNumber i = 1; i < (ReincarnationNumber(1) << 31); i <<= 1) {
    state.reincarnation_number = i;
    auto encoded = state.Encode();
    AccountState restored;
    restored.SetBytes(std::as_bytes(std::span(encoded)));
    EXPECT_EQ(state.reincarnation_number, restored.reincarnation_number);
  }
}

}  // namespace
}  // namespace carmen::archive::leveldb

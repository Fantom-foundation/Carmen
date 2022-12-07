#include "backend/index/memory/index.h"

#include "common/status_test_util.h"
#include "common/test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::StrEq;

TEST(IndexTest, KnownAddresssIndexHashes) {
  InMemoryIndex<Address, int> index;

  ASSERT_OK_AND_ASSIGN(auto hash, index.GetHash());
  EXPECT_THAT(Print(hash),
              StrEq("0x00000000000000000000000000000000000000000000000000000000"
                    "00000000"));

  index.GetOrAdd(Address{0x01});
  ASSERT_OK_AND_ASSIGN(hash, index.GetHash());
  EXPECT_THAT(Print(hash),
              StrEq("0xff9226e320b1deb7fabecff9ac800cd8eb1e3fb7709c003e2effcce3"
                    "7eec68ed"));

  index.GetOrAdd(Address{0x02});
  ASSERT_OK_AND_ASSIGN(hash, index.GetHash());
  EXPECT_THAT(Print(hash),
              StrEq("0xc28553369c52e217564d3f5a783e2643186064498d1b3071568408d4"
                    "9eae6cbe"));
}

TEST(IndexTest, KnownKeyIndexHashes) {
  InMemoryIndex<Key, int> index;

  ASSERT_OK_AND_ASSIGN(auto hash, index.GetHash());
  EXPECT_THAT(Print(hash),
              StrEq("0x00000000000000000000000000000000000000000000000000000000"
                    "00000000"));

  index.GetOrAdd(Key{0x01});
  ASSERT_OK_AND_ASSIGN(hash, index.GetHash());
  EXPECT_THAT(Print(hash),
              StrEq("0xcb592844121d926f1ca3ad4e1d6fb9d8e260ed6e3216361f7732e975"
                    "a0e8bbf6"));

  index.GetOrAdd(Key{0x02});
  ASSERT_OK_AND_ASSIGN(hash, index.GetHash());
  EXPECT_THAT(Print(hash),
              StrEq("0x975d8dfa71d715cead145c4b80c474d210471dbc7ff614e9dab53887"
                    "d61bc957"));
}

}  // namespace
}  // namespace carmen::backend::index

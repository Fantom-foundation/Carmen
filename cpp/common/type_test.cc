#include "common/type.h"

#include "absl/container/flat_hash_set.h"
#include "common/test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::StrEq;
using ::testing::StrNe;

TEST(ByteValueTest, CanBePrinted) {
  ByteValue<2> container{0x12, 0xab};
  EXPECT_THAT(Print(container), StrEq("0x12ab"));
}

TEST(ByteValueTest, CanBeEmpty) {
  ByteValue<0> container;
  EXPECT_THAT(Print(container), StrEq("0x"));
}

TEST(ByteValueTest, CanBeInitializedEmpty) {
  ByteValue<1> container;
  EXPECT_THAT(Print(container), StrEq("0x00"));
}

TEST(ByteValueTest, CannotHoldMoreValues) {
  ByteValue<2> container{0x12, 0xab, 0xef};
  EXPECT_THAT(Print(container), StrNe("0x12abef"));
}

TEST(ByteValueTest, CanBeUsedInFlatHashSet) {
  ByteValue<2> a{0x12, 0x14};
  ByteValue<2> b{0x16, 0xf5};
  absl::flat_hash_set<ByteValue<2>> set;
  EXPECT_FALSE(set.contains(a));
  EXPECT_FALSE(set.contains(b));
  set.insert(a);
  EXPECT_TRUE(set.contains(a));
  EXPECT_FALSE(set.contains(b));
}

TEST(HashTest, SizeIsCompact) { EXPECT_EQ(kHashLength, sizeof(Hash)); }

TEST(HashTest, TypeProperties) {
  EXPECT_TRUE(std::is_trivially_copyable_v<Hash>);
  EXPECT_TRUE(std::is_trivially_move_assignable_v<Hash>);
  EXPECT_TRUE(std::equality_comparable<Hash>);
  EXPECT_TRUE(std::is_default_constructible_v<Hash>);
}

TEST(HashTest, CanBeUsedInFlatHashSet) {
  Hash a{0x12, 0x14};
  Hash b{0x16, 0xf5};
  absl::flat_hash_set<Hash> set;
  EXPECT_FALSE(set.contains(a));
  EXPECT_FALSE(set.contains(b));
  set.insert(a);
  EXPECT_TRUE(set.contains(a));
  EXPECT_FALSE(set.contains(b));
}

TEST(AddressTest, SizeIsCompact) { EXPECT_EQ(kAddressLength, sizeof(Address)); }

TEST(AddressTest, TypeProperties) {
  EXPECT_TRUE(std::is_trivially_copyable_v<Address>);
  EXPECT_TRUE(std::is_trivially_move_assignable_v<Address>);
  EXPECT_TRUE(std::equality_comparable<Address>);
  EXPECT_TRUE(std::is_default_constructible_v<Address>);
}

TEST(KeyTest, SizeIsCompact) { EXPECT_EQ(kKeyLength, sizeof(Key)); }

TEST(KeyTest, TypeProperties) {
  EXPECT_TRUE(std::is_trivially_copyable_v<Key>);
  EXPECT_TRUE(std::is_trivially_move_assignable_v<Key>);
  EXPECT_TRUE(std::equality_comparable<Key>);
  EXPECT_TRUE(std::is_default_constructible_v<Key>);
}

TEST(ValueTest, SizeIsCompact) { EXPECT_EQ(kValueLength, sizeof(Value)); }

TEST(ValueTest, TypeProperties) {
  EXPECT_TRUE(std::is_trivially_copyable_v<Value>);
  EXPECT_TRUE(std::is_trivially_move_assignable_v<Value>);
  EXPECT_TRUE(std::equality_comparable<Value>);
  EXPECT_TRUE(std::is_default_constructible_v<Value>);
}

}  // namespace
}  // namespace carmen

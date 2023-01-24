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

TEST(ByteValueTest, DefaultValueIsZero) {
  EXPECT_EQ(ByteValue<0>{}, (ByteValue<0>{}));
  EXPECT_EQ(ByteValue<1>{}, (ByteValue<1>{0x00}));
  EXPECT_EQ(ByteValue<2>{}, (ByteValue<2>{0x00, 0x00}));
  EXPECT_EQ(ByteValue<3>{}, (ByteValue<3>{0x00, 0x00, 0x00}));
}

TEST(ByteValueTest, AreComarable) {
  using Value = ByteValue<2>;
  EXPECT_EQ(Value{0x01}, Value{0x01});
  EXPECT_NE(Value{0x01}, Value{0x02});
  EXPECT_LT(Value{0x01}, Value{0x02});
  EXPECT_LE(Value{0x01}, Value{0x02});
  EXPECT_GT(Value{0x02}, Value{0x01});
  EXPECT_GE(Value{0x02}, Value{0x01});
}

TEST(ByteValueTest, AreLexicographicallySorted) {
  EXPECT_LT((ByteValue<3>{0x01, 0x02}), (ByteValue<3>{0x01, 0x03}));
  EXPECT_LT((ByteValue<3>{0x01, 0x02}), (ByteValue<3>{0x02, 0x01}));
  EXPECT_LT((ByteValue<3>{0x01}), (ByteValue<3>{0x01, 0x02}));
  EXPECT_EQ((ByteValue<3>{0x01}), (ByteValue<3>{0x01, 0x00}));
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
  EXPECT_TRUE(Trivial<Address>);
  EXPECT_TRUE(std::is_trivially_copyable_v<Address>);
  EXPECT_TRUE(std::is_trivially_move_assignable_v<Address>);
  EXPECT_TRUE(std::equality_comparable<Address>);
  EXPECT_TRUE(std::is_default_constructible_v<Address>);
}

TEST(KeyTest, SizeIsCompact) { EXPECT_EQ(kKeyLength, sizeof(Key)); }

TEST(KeyTest, TypeProperties) {
  EXPECT_TRUE(Trivial<Key>);
  EXPECT_TRUE(std::is_trivially_copyable_v<Key>);
  EXPECT_TRUE(std::is_trivially_move_assignable_v<Key>);
  EXPECT_TRUE(std::equality_comparable<Key>);
  EXPECT_TRUE(std::is_default_constructible_v<Key>);
}

TEST(ValueTest, SizeIsCompact) { EXPECT_EQ(kValueLength, sizeof(Value)); }

TEST(ValueTest, TypeProperties) {
  EXPECT_TRUE(Trivial<Value>);
  EXPECT_TRUE(std::is_trivially_copyable_v<Value>);
  EXPECT_TRUE(std::is_trivially_move_assignable_v<Value>);
  EXPECT_TRUE(std::equality_comparable<Value>);
  EXPECT_TRUE(std::is_default_constructible_v<Value>);
}

TEST(BalanceTest, SizeIsCompact) { EXPECT_EQ(kBalanceLength, sizeof(Balance)); }

TEST(BalanceTest, TypeProperties) {
  EXPECT_TRUE(Trivial<Balance>);
  EXPECT_TRUE(std::is_trivially_copyable_v<Balance>);
  EXPECT_TRUE(std::is_trivially_move_assignable_v<Balance>);
  EXPECT_TRUE(std::equality_comparable<Balance>);
  EXPECT_TRUE(std::is_default_constructible_v<Balance>);
}

TEST(NonceTest, SizeIsCompact) { EXPECT_EQ(kNonceLength, sizeof(Nonce)); }

TEST(NonceTest, TypeProperties) {
  EXPECT_TRUE(Trivial<Nonce>);
  EXPECT_TRUE(std::is_trivially_copyable_v<Nonce>);
  EXPECT_TRUE(std::is_trivially_move_assignable_v<Nonce>);
  EXPECT_TRUE(std::equality_comparable<Nonce>);
  EXPECT_TRUE(std::is_default_constructible_v<Nonce>);
}

}  // namespace
}  // namespace carmen

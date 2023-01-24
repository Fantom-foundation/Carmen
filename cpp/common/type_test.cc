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

TEST(ByteValueTest, ValuesCanBeAccessedUsingSubscripts) {
  ByteValue<3> a{};
  a[0] = 1;
  a[1] = 2;
  a[2] = 3;
  const ByteValue<3> b = a;
  EXPECT_EQ(b[0], 1);
  EXPECT_EQ(b[1], 2);
  EXPECT_EQ(b[2], 3);
}

TEST(ByteValueTest, CanBeConvertedToByteSpans) {
  ByteValue<23> a{};
  std::span<const std::byte> span_a = a;
  std::span<const std::byte, 23> span_b = a;
  EXPECT_EQ(span_a.size(), span_b.size());
  EXPECT_EQ(span_a.data(), span_b.data());
  EXPECT_EQ(span_a.data(), reinterpret_cast<const std::byte*>(&a[0]));
}

TEST(ByteValueTest, ValuesCanBeUpdatedUsingSetByte) {
  ByteValue<3> a{};
  std::span<const std::byte, 3> span_fixed = a;
  std::span<const std::byte> span_variable = a;

  a[0] = 1;
  a[1] = 2;
  a[2] = 3;
  ByteValue<3> b;
  b.SetBytes(span_fixed);
  EXPECT_EQ(a, b);

  a[1] = 4;
  EXPECT_NE(a, b);
  b.SetBytes(span_variable);
  EXPECT_EQ(a, b);
}

TEST(ByteValueTest, ValuesCanBeUpdatedUsingDifferentLengthSpan) {
  ByteValue<3> a{};

  ByteValue<4> b{0x01, 0x02, 0x03, 0x04};
  a.SetBytes(b);
  EXPECT_EQ(a, (ByteValue<3>{0x01, 0x02, 0x03}));

  ByteValue<2> c{0x04, 0x05};
  a.SetBytes(c);
  EXPECT_EQ(a, (ByteValue<3>{0x04, 0x05, 0x00}));
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

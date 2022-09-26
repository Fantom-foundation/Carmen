#include "common/type.h"

#include <sstream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::StrEq;
using ::testing::StrNe;

template<typename T>
std::string Print(const T& value) {
    std::stringstream out;
    out << value;
    return out.str();
}

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

TEST(HashTest, SizeIsCompact) {
    EXPECT_EQ(HASH_LENGTH, sizeof(Hash));
}

TEST(HashTest, TypeProperties) {
    EXPECT_TRUE(std::is_trivially_copyable_v<Hash>);
    EXPECT_TRUE(std::is_trivially_move_assignable_v<Hash>);
    EXPECT_TRUE(std::equality_comparable<Hash>);
    EXPECT_TRUE(std::is_default_constructible_v<Hash>);
}

TEST(AddressTest, SizeIsCompact) {
    EXPECT_EQ(ADDRESS_LENGTH, sizeof(Address));
}

TEST(AddressTest, TypeProperties) {
    EXPECT_TRUE(std::is_trivially_copyable_v<Address>);
    EXPECT_TRUE(std::is_trivially_move_assignable_v<Address>);
    EXPECT_TRUE(std::equality_comparable<Address>);
    EXPECT_TRUE(std::is_default_constructible_v<Address>);
}

} // namespace
} // namespace carmen

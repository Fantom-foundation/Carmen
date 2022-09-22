#include "common/type.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <sstream>

namespace carmen {
namespace {

using ::testing::StrEq;

template<typename T>
std::string Print(const T& value) {
    std::stringstream out;
    out << value;
    return out.str();
}

TEST(HashTest, SizeIsCompact) { 
    // A simple test to ensure that hashes do require a fixed amount of memory.
    EXPECT_EQ(32, sizeof(Hash)); 
}

TEST(HashTest, TypeProperties) {
    EXPECT_TRUE(std::is_trivially_copyable_v<Hash>);
    EXPECT_TRUE(std::is_trivially_move_assignable_v<Hash>);
}

TEST(HashTest, CanBePrinted) {
    // Test to ensure that hashes are printable in hex format with "0x" prefix
    Hash hash((std::array<std::uint8_t, 32>{0x12, 0xab}));
    EXPECT_THAT(Print(hash), StrEq("0x12ab000000000000000000000000000000000000000000000000000000000000"));
}

TEST(HashTest, CanBeCompared) {
    // Test to ensure that hashes can be compared
    Hash hashA((std::array<std::uint8_t, 32>{0x12, 0xab}));
    Hash hashB((std::array<std::uint8_t, 32>{0x12, 0xab}));
    Hash hashC((std::array<std::uint8_t, 32>{0x01, 0xab}));
    EXPECT_EQ(hashA, hashB);
    EXPECT_NE(hashA, hashC);
}

TEST(HashTest, AllZerosBinary) {
    // Test to ensure that hashes can contain only zero binary values
    std::array<std::uint8_t, 32> data;
    data.fill(0);
    Hash hash((data));
    EXPECT_THAT(Print(hash), StrEq("0x0000000000000000000000000000000000000000000000000000000000000000"));
}

TEST(HashTest, AllOnesBinary) {
    // Test to ensure that hashes can contain only one values
    std::array<std::uint8_t, 32> data;
    data.fill(255);
    Hash hash((data));
    EXPECT_THAT(Print(hash), StrEq("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"));
}

} // namespace
} // namespace carmen
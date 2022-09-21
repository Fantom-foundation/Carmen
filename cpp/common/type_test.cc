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

TEST(DISABLED_HashTest, CanBePrinted) { 
    Hash hash;
    EXPECT_THAT(Print(hash), StrEq("abc"));
}

} // namespace
} // namespace carmen
#include "common/type.h"

#include <sstream>

#include "gmock/gmock.h"

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
    EXPECT_EQ(32, sizeof(Hash));
}

TEST(HashTest, TypeProperties) {
    EXPECT_TRUE(std::is_trivially_copyable_v<Hash>);
    EXPECT_TRUE(std::is_trivially_move_assignable_v<Hash>);
    EXPECT_TRUE(std::equality_comparable<Hash>);
    EXPECT_TRUE(std::is_default_constructible_v<Hash>);
}

} // namespace
} // namespace carmen

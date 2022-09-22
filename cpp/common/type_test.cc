#include "common/type.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <sstream>

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

TEST(HexContainerTest, CanBePrinted) {
  // Test to ensure that hex containers are printable in hex format.
  HexContainer<2> container{0x12, 0xab};

  EXPECT_THAT(Print(container), StrEq("0x12ab"));
}

TEST(HexContainerTest, CanBeCompared) {
  // Test to ensure that hex containers can be compared.
  HexContainer<2> containerA{0x12, 0xab};
  HexContainer<2> containerB{0x12, 0xab};
  HexContainer<2> containerC{0x01, 0xab};

  EXPECT_EQ(Print(containerA), Print(containerB));
  EXPECT_NE(Print(containerA), Print(containerC));
}

TEST(HexContainerTest, CanBeEmpty) {
  // Test to ensure that container can be empty.
  HexContainer<0> container{};

  EXPECT_THAT(Print(container), StrEq("0x"));
}

TEST(HexContainerTest, CanBeInitializedEmpty) {
  // Test to ensure that container can be initialized empty.
  HexContainer<1> container{};

  EXPECT_THAT(Print(container), StrEq("0x00"));
}

TEST(HexContainerTest, CannotHoldMoreValues) {
  // Test to ensure that container cannot be fed with more values.
  HexContainer<2> container{0x12, 0xab, 0xef};

  EXPECT_THAT(Print(container), StrNe("0x12abef"));
}

TEST(HashTest, SizeIsCompact) { 
    // A simple test to ensure that hashes do require a fixed amount of memory.
    EXPECT_EQ(32, sizeof(Hash)); 
}

TEST(HashTest, TypeProperties) {
    EXPECT_TRUE(std::is_trivially_copyable_v<Hash>);
    EXPECT_TRUE(std::is_trivially_move_assignable_v<Hash>);
}

} // namespace
} // namespace carmen

#include "common/hex_util.h"

#include <ostream>

#include "gmock/gmock.h"

#include "common/type.h"

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

TEST(HexUtilTest, ContainsAllHexValues) {
  std::array<uint8_t, 8> values = {0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
                                           0xcd, 0xef};
  std::stringstream out;
  hex_util::WriteTo(values, out);
  EXPECT_THAT(out.str(), StrEq("0x0123456789abcdef"));
}

TEST(HexUtilTest, CanBePrinted) {
  ByteValue<2> container{0x12, 0xab};
  EXPECT_THAT(Print(container), StrEq("0x12ab"));
}

TEST(HexUtilTest, CanBeEmpty) {
  ByteValue<0> container{};
  EXPECT_THAT(Print(container), StrEq("0x"));
}

TEST(HexUtilTest, CanBeInitializedEmpty) {
  ByteValue<1> container{};
  EXPECT_THAT(Print(container), StrEq("0x00"));
}

TEST(HexUtilTest, CannotHoldMoreValues) {
  ByteValue<2> container{0x12, 0xab, 0xef};
  EXPECT_THAT(Print(container), StrNe("0x12abef"));
}

} // namespace
} // namespace carmen

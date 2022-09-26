#include "common/hex_util.h"

#include <ostream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::StrEq;

TEST(HexUtilTest, ContainsAllHexValues) {
  std::array<uint8_t, 8> values = {0x01, 0x23, 0x45, 0x67,
                                   0x89, 0xab, 0xcd, 0xef};
  std::stringstream out;
  hex_util::WriteTo(out, values);
  EXPECT_THAT(out.str(), StrEq("0x0123456789abcdef"));
}

}  // namespace
}  // namespace carmen

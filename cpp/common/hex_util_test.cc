#include "common/hex_util.h"

#include "gmock/gmock.h"

namespace carmen {
namespace {

using ::testing::StrEq;

TEST(HashUtilTest, ContainsAllHexValues) {
    // Test to ensure that all values are correctly converted to hex values
    const std::array<uint8_t, 8> values = {0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
                                             0xcd, 0xef};

    std::string result = hex_util::ToString(values);

    EXPECT_THAT(result, StrEq("0x0123456789abcdef"));
}
} // namespace
} // namespace carmen

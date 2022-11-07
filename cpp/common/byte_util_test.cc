#include "common/byte_util.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::StartsWith;
using ::testing::ElementsAreArray;


TEST(ByteUtil, ConvertToBytes) {
  int value = 42;
  auto bytes = AsBytes(value);
  EXPECT_THAT(bytes, testing::ElementsAreArray({std::byte{0}, std::byte{0}, std::byte{0}, std::byte{42}}));
}

}  // namespace
}  // namespace carmen

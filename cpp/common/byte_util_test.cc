/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "common/byte_util.h"

#include "absl/status/statusor.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::ElementsAreArray;
using ::testing::StartsWith;
using ::testing::StatusIs;

TEST(ByteUtil, ConvertToBytes) {
  std::uint32_t value = 42;
  auto bytes = AsBytes(value);
  ASSERT_EQ(bytes.size(), sizeof(std::uint32_t));
  if (std::endian::native == std::endian::little) {
    EXPECT_THAT(bytes, testing::ElementsAreArray({std::byte{42}, std::byte{0},
                                                  std::byte{0}, std::byte{0}}));
  } else {
    EXPECT_THAT(bytes,
                testing::ElementsAreArray(
                    {std::byte{0}, std::byte{0}, std::byte{0}, std::byte{42}}));
  }
}

TEST(ByteUtil, ConvertToChars) {
  std::uint32_t value = 42;
  auto chars = AsChars(value);
  ASSERT_EQ(chars.size(), sizeof(std::uint32_t));
  if (std::endian::native == std::endian::little) {
    EXPECT_THAT(chars, testing::ElementsAreArray(
                           {char{42}, char{0}, char{0}, char{0}}));
  } else {
    EXPECT_THAT(chars, testing::ElementsAreArray(
                           {char{0}, char{0}, char{0}, char{42}}));
  }
}

TEST(ByteUtil, ConvertFromBytes) {
  absl::StatusOr<std::uint32_t> result;
  if (std::endian::native == std::endian::little) {
    result = FromBytes<std::uint32_t>(
        {{std::byte{42}, std::byte{0}, std::byte{0}, std::byte{0}}});
  } else {
    result = FromBytes<std::uint32_t>(
        {{std::byte{0}, std::byte{0}, std::byte{0}, std::byte{42}}});
  }
  ASSERT_EQ(*result, std::uint32_t{42});
}

TEST(ByteUtil, ConvertFromBytesWrongSize) {
  auto result =
      FromBytes<std::uint32_t>({{std::byte{42}, std::byte{0}, std::byte{0},
                                 std::byte{0}, std::byte{0}}});
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInternal,
                               StartsWith("Invalid data size")));
}

TEST(ByteUtil, ConvertFromChars) {
  absl::StatusOr<std::uint32_t> result;
  if (std::endian::native == std::endian::little) {
    result = FromChars<std::uint32_t>({{char{42}, char{0}, char{0}, char{0}}});
  } else {
    result = FromChars<std::uint32_t>({{char{0}, char{0}, char{0}, char{42}}});
  }
  ASSERT_EQ(*result, std::uint32_t{42});
}

TEST(ByteUtil, ConvertFromCharsWrongSize) {
  auto result = FromChars<std::uint32_t>(
      {{char{42}, char{0}, char{0}, char{0}, char{0}}});
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInternal,
                               StartsWith("Invalid data size")));
}

}  // namespace
}  // namespace carmen

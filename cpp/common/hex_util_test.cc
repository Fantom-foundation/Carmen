/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3 
 */

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

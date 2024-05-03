/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public License v3.
 */

#include "common/variant_util.h"

#include <string>
#include <variant>

#include "gtest/gtest.h"

namespace carmen {
namespace {

TEST(Match, CanBeUsedInStaticVistitor) {
  // Demonstrates how to use match{..} to implement a static visitor.
  std::variant<int, bool, std::string> data = false;
  auto res = std::visit(
      match{[](int) -> std::string { return "int"; },
            [](bool) -> std::string { return "bool"; },
            [](const std::string&) -> std::string { return "std::string"; }},
      data);
  EXPECT_EQ(res, "bool");
}

}  // namespace
}  // namespace carmen

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

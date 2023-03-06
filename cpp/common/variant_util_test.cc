#include "common/variant_util.h"

#include <string>
#include <type_traits>
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

TEST(Variant, ProducesVariantWithoutRepetition) {
  static_assert(std::is_same_v<Variant<>, std::variant<>>);
  static_assert(std::is_same_v<Variant<int>, std::variant<int>>);
  static_assert(std::is_same_v<Variant<int, int>, std::variant<int>>);
  static_assert(
      std::is_same_v<Variant<int, bool, int>, std::variant<int, bool>>);
  static_assert(std::is_same_v<Variant<int, bool, int, float>,
                               std::variant<int, bool, float>>);
}

}  // namespace
}  // namespace carmen

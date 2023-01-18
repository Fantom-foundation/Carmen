#pragma once

#include "absl/strings/str_cat.h"
#include "common/macro_utils.h"
#include "common/status_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

// A few additional gtest expectations and assertions.
#define EXPECT_OK(expr) EXPECT_THAT((expr), ::testing::IsOk())
#define ASSERT_OK(expr) ASSERT_THAT((expr), ::testing::IsOk())

// The implementation of ASSERT_OK_AND_ASSIGN below, more compact as if it would
// be if it would be written inline.
#define INTERNAL_ASSERT_OK_AND_ASSIGN_IMPL(lhs, expr, var) \
  auto var = (expr);                                       \
  ASSERT_THAT(var, ::testing::IsOk());                     \
  lhs = std::move(*var)

// The ASSERT_OK_AND_ASSIGN macro can be used in test cases where the OK status
// of a StatusOr value should be asserted and the status part stripped.
//
// Example: instead of writing
//
//    auto status_or_value = <expr>;
//    ASSERT_OK(status_or_value);
//    lhs = *status_or_value;
//
// this macro can be used to write
//
//    ASSERT_OK_AND_ASSIGN(lhs, <expr>);
//
// instead. The macro also works with declarations, such that one could write
//
//    ASSERT_OK_AND_ASSIGN(auto x, <expr>);
//
// to declare and initialize a new variable x in the current scope. The variable
// X will be of the value type stored inside the StatusOr type.
#define ASSERT_OK_AND_ASSIGN(lhs, expr)                                 \
  INTERNAL_ASSERT_OK_AND_ASSIGN_IMPL(REMOVE_OPTIONAL_PARENTHESIS(lhs),  \
                                     REMOVE_OPTIONAL_PARENTHESIS(expr), \
                                     CONCAT(_status_, __LINE__))

namespace testing {

namespace internal {

absl::StatusCode GetCode(const absl::Status& status) { return status.code(); }

template <typename T>
absl::StatusCode GetCode(const absl::StatusOr<T>& status) {
  return GetCode(status.status());
}

absl::string_view GetMessage(const absl::Status& status) {
  return status.message();
}

template <typename T>
absl::string_view GetMessage(const absl::StatusOr<T>& status) {
  return GetMessage(status.status());
}

template <typename T>
T GetValue(const absl::StatusOr<T>& status) {
  return *status;
}

}  // namespace internal

// Defines a IsOk matcher for matching Status or StatusOr using EXPECT_THAT.
// Example use:
//   EXPECT_THAT(<expr>, IsOk());
MATCHER(IsOk, absl::StrCat(negation ? "isn't" : "is", " OK status")) {
  *result_listener << "where status code is "
                   << ::testing::internal::GetCode(arg);
  return arg.ok();
}

// Defines a StatusIs matcher for matching the value Status or StatusOr values.
// Example uses:
//   EXPECT_THAT(<expr>, StatusIs(absl::StatusCode::kInvalidArgument,_));
//   EXPECT_THAT(<expr>, StatusIs(_,StrEq("File does not exist.")));
MATCHER_P2(StatusIs, code, msg,
           absl::StrCat(
               "status code ",
               ::testing::DescribeMatcher<absl::StatusCode>(code, negation),
               " and message ",
               ::testing::DescribeMatcher<absl::string_view>(msg, negation))) {
  return ExplainMatchResult(code, ::testing::internal::GetCode(arg),
                            result_listener) &&
         ExplainMatchResult(msg, ::testing::internal::GetMessage(arg),
                            result_listener);
}

// Defines a IsOkAndHolds matcher for matching StatusOr with ok status and
// value using EXPECT_THAT.
// Example use:
//   EXPECT_THAT(<expr>, IsOkAndHolds(<value>));
MATCHER_P(
    IsOkAndHolds, value,
    absl::StrCat(
        "OK status and value ",
        ::testing::DescribeMatcher<typename std::decay_t<arg_type>::value_type>(
            value, negation))) {
  return ExplainMatchResult(absl::StatusCode::kOk,
                            ::testing::internal::GetCode(arg),
                            result_listener) &&
         ExplainMatchResult(value, ::testing::internal::GetValue(arg),
                            result_listener);
}

}  // namespace testing

namespace absl {

namespace internal {

// A concept identifying types that can be written to an output stream.
template <typename T>
concept StreamableToOutputStream = requires(const T a) {
  { std::declval<std::ostream&>() << a } -> std::same_as<std::ostream&>;
};

}  // namespace internal

// Allows StatusOr instances to be printed to output streams. This is most
// useful in unit tests where values compared in EXPECT_THAT(..) statements
// are printed by the gunit infrastructure using the << operatore.
template <typename T>
std::ostream& operator<<(std::ostream& out, const StatusOr<T>& status) {
  if (!status.ok()) {
    return out << status.status();
  }
  out << "OK: ";
  // If supported, use the value's custom output format.
  if constexpr (internal::StreamableToOutputStream<T>) {
    return out << *status;
  }
  // If there is no output format defined, use gunit's universal printing
  // format.
  return out << testing::PrintToString<T>(*status);
}

}  // namespace absl

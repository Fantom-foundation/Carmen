#pragma once

#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

// This header provides a few utility macros for dealing with absl::Status and
// absl::StatusOr values. The main intention is to make code using Status codes
// more concise and readable.
//
// Additional utilities for writing unit tests using Status values can be found
// in status_util_test.h
//
// Additional links:
//  - Error code selection guides:
//  https://abseil.io/docs/cpp/guides/status-codes
//
// Note: this header may in the future be replaced by a solution provided by
// googletest.

// Concatenates the provided arguments into a single token.
#define CONCAT(a, b) CONCAT_INNER(a, b)
#define CONCAT_INNER(a, b) a##b

namespace testing::internal {

inline absl::Status GetStatus(absl::Status status) { return status; }

template <typename T>
absl::Status GetStatus(absl::StatusOr<T> status) {
  return status.status();
}

}  // namespace testing::internal

// The implementation of RETURN_IF_ERROR below, more compact as if it would be
// if it would be written inline.
#define INTERNAL_RETURN_IF_ERROR_IMPL(expr, var) \
  auto var = (expr);                             \
  if (ABSL_PREDICT_FALSE(!var.ok()))             \
  return ::testing::internal::GetStatus(std::move(var))

// A macro to evaluate a given expression returning a Status or a StatusOr
// value and returning an error if the expression failed. Thus, instead of
// writing
//
//   auto status = <expr>;
//   if (!status.ok()) return status;
//
// one can use this macro to write
//
//   RETURN_IF_ERROR(<expr>);
//
// Note, for this to work, the enclosing function must return a Status or a
// StatusOr.
#define RETURN_IF_ERROR(expr) \
  INTERNAL_RETURN_IF_ERROR_IMPL(expr, CONCAT(_status_, __LINE__))

// The implementation of ASSIGN_OR_RETURN below, more compact as if it would be
// if it would be written inline.
#define INTERNAL_ASSIGN_OR_RETURN_IMPL(lhs, expr, var)    \
  auto var = (expr);                                      \
  if (ABSL_PREDICT_FALSE(!var.ok())) return var.status(); \
  lhs = std::move(*var)

// A macro to evaluate a given expression returning a StatusOr value and
// returning from the current function scope if the value could not be obtained.
// Thus, instead of writing
//
//   auto status_or = <expr>;
//   if (!status_or.ok()) return status_or.status;
//   lhs = *status_or;
//
// one can use this macro to write
//
//   ASSIGN_OR_RETURN(lhs, <expr>);
//
// The <lhs> can also be a variable declaration.
//
// Note, for this to work, the enclosing function must return a Status or a
// StatusOr.
#define ASSIGN_OR_RETURN(lhs, expr) \
  INTERNAL_ASSIGN_OR_RETURN_IMPL(lhs, expr, CONCAT(_status_, __LINE__))

#pragma once

#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "cerrno"
#include "common/macro_utils.h"

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

// Get status based on status code. If `errno` error code is set, the error
// message will be appended to the status message.
inline absl::Status GetStatusWithSystemError(absl::StatusCode code,
                                             std::string_view message) {
  if (errno == 0) {
    return {code, message};
  }
  return {code, absl::StrCat(message, " Error: ", std::strerror(errno))};
}

// Wrapper around std::reference_wrapper that provides functions to access the
// wrapped value as reference or pointer.
template <typename T>
class ReferenceWrapper : public std::reference_wrapper<T> {
 public:
  using std::reference_wrapper<T>::reference_wrapper;
  // Returns a reference to the wrapped value.
  T& AsReference() const { return this->get(); }
  // Returns a pointer to the wrapped value.
  T* AsPointer() const { return &AsReference(); }
};

// Type definition for a StatusOr<T> that can be used with reference types.
template <typename T>
using StatusOrRef = absl::StatusOr<ReferenceWrapper<T>>;

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
#define ASSIGN_OR_RETURN(lhs, expr)                                 \
  INTERNAL_ASSIGN_OR_RETURN_IMPL(REMOVE_OPTIONAL_PARENTHESIS(lhs),  \
                                 REMOVE_OPTIONAL_PARENTHESIS(expr), \
                                 CONCAT(_status_, __LINE__))

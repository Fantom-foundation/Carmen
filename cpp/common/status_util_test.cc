/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "common/status_util.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "cerrno"
#include "common/status_test_util.h"
#include "gtest/gtest.h"

namespace {

using ::testing::_;
using ::testing::IsOk;
using ::testing::Not;
using ::testing::StartsWith;
using ::testing::StatusIs;
using ::testing::StrEq;

absl::Status Ok() { return absl::OkStatus(); }

absl::Status Fail() { return absl::UnknownError("fail"); }

absl::Status Process() { return Ok(); }

template <typename First, typename... Rest>
absl::Status Process(First&& op, Rest&&... rest) {
  RETURN_IF_ERROR(op());
  return Process(std::forward<Rest>(rest)...);
}

TEST(StatusMacroTest, ReturnIfErrorWorks) {
  EXPECT_TRUE(Process(&Ok, &Ok).ok());
  EXPECT_FALSE(Process(&Ok, &Fail).ok());
  EXPECT_FALSE(Process(&Fail, &Ok).ok());
}

template <typename A, typename B, typename C>
absl::Status DoAll(A&& a, B&& b, C&& c) {
  RETURN_IF_ERROR(a());
  RETURN_IF_ERROR(b());
  return c();
}

TEST(StatusMacroTest, MultipleReturnIfWorkInOneFunction) {
  EXPECT_TRUE(DoAll(&Ok, &Ok, &Ok).ok());
  EXPECT_FALSE(DoAll(&Fail, &Ok, &Ok).ok());
  EXPECT_FALSE(DoAll(&Ok, &Fail, &Ok).ok());
  EXPECT_FALSE(DoAll(&Ok, &Ok, &Fail).ok());
}

absl::StatusOr<int> Get(int i) { return i; }

absl::StatusOr<int> Fail(int) { return absl::InternalError("triggered fail"); }

TEST(StatusMacroTest, ReturnIfErrorWorksWithStatusAndStatusOr) {
  EXPECT_OK(Process(&Ok, [] { return Get(12); }));
  EXPECT_THAT(Process(&Ok, [] { return Fail(12); }), Not(IsOk()));
}

absl::StatusOr<int> IncWithAssignment(int x) {
  // A variable can be assigned as part of the macro.
  int y = -1;
  ASSIGN_OR_RETURN(y, Get(x));
  return y + 1;
}

absl::StatusOr<int> IncWithDeclaration(int x) {
  // A variable can be declared as part of the macro.
  ASSIGN_OR_RETURN(auto y, Get(x));
  return y + 1;
}

TEST(StatusMacroTest, AssignOrReturnWorks) {
  ASSERT_OK_AND_ASSIGN(int x, IncWithAssignment(10));
  EXPECT_EQ(x, 11);
  ASSERT_OK_AND_ASSIGN(x, IncWithDeclaration(15));
  EXPECT_EQ(x, 16);
}

template <typename Source>
absl::Status AssignAndReturnError(Source src) {
  ASSIGN_OR_RETURN(auto y, src());
  if (y > 0) {
    return absl::InternalError("y should be zero");
  }
  return absl::OkStatus();
}

TEST(StatusMacroTest, AssignOrReturnCanReturnPlainStatus) {
  EXPECT_OK(AssignAndReturnError([] { return Get(0); }));
  EXPECT_THAT(AssignAndReturnError([] { return Fail(0); }), Not(IsOk()));
  EXPECT_THAT(AssignAndReturnError([] { return Get(1); }),
              StatusIs(absl::StatusCode::kInternal, _));
}

TEST(ReferenceWraperTest, ReferenceAddressesAreEqual) {
  int x = 10;
  auto wrapper = ReferenceWrapper<int>(x);
  EXPECT_EQ(&x, &wrapper.AsReference());
}

TEST(ReferenceWraperTest, PointsToSameValue) {
  int x = 10;
  auto wrapper = ReferenceWrapper<int>(x);
  EXPECT_EQ(&x, wrapper.AsPointer());
}

TEST(StatusWithSystemErrorTest, HasNoSystemError) {
  // make sure the errno is set to zero
  auto status = GetStatusWithSystemError(absl::StatusCode::kInvalidArgument, 0,
                                         "Invalid arguments.");
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               StrEq("Invalid arguments.")));
}

TEST(StatusWithSystemErrorTest, HasSystemError) {
  auto status = GetStatusWithSystemError(absl::StatusCode::kInternal, ENOENT,
                                         "Internal error.");
  // assure that error message is appended.
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal,
                               StartsWith("Internal error. Error:")));
}

absl::StatusOr<std::pair<int, int>> CreatePair() { return std::pair{1, 2}; }

absl::StatusOr<int> AssignOrReturnWithDecomposition() {
  ASSIGN_OR_RETURN((auto [a, b]), CreatePair());
  return a + b;
}

TEST(StatusMacroTest, AssignCanHandleDecomposition) {
  EXPECT_THAT(AssignOrReturnWithDecomposition(), 3);
}
}  // namespace

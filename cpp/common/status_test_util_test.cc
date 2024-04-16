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

#include "common/status_test_util.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gtest/gtest.h"

namespace {

using ::testing::_;
using ::testing::IsOk;
using ::testing::IsOkAndHolds;
using ::testing::Not;
using ::testing::Pair;
using ::testing::PrintToString;
using ::testing::StatusIs;
using ::testing::StrEq;

TEST(StatusTestUtilTest, ExpectOkWorks) { EXPECT_OK(absl::OkStatus()); }

TEST(StatusTestUtilTest, AssertOkWorks) { ASSERT_OK(absl::OkStatus()); }

TEST(StatusTestUtilTest, IsOkMatcherWorksOnStatus) {
  auto ok = absl::OkStatus();
  auto err = absl::InvalidArgumentError("test");
  EXPECT_THAT(ok, IsOk());
  EXPECT_THAT(err, Not(IsOk()));
}

TEST(StatusTestUtilTest, IsOkMatcherWorksOnStatusOr) {
  absl::StatusOr<int> ok = 12;
  absl::StatusOr<int> err = absl::InvalidArgumentError("test");
  EXPECT_THAT(ok, IsOk());
  EXPECT_THAT(err, Not(IsOk()));
}

TEST(StatusTestUtilTest, StatusIsMatcherWorks) {
  auto ok = absl::OkStatus();
  auto err = absl::InvalidArgumentError("test");
  EXPECT_THAT(ok, StatusIs(absl::StatusCode::kOk, _));
  EXPECT_THAT(err, StatusIs(absl::StatusCode::kInvalidArgument, _));
  EXPECT_THAT(err, StatusIs(_, StrEq("test")));
}

TEST(StatusTestUtilTest, StatusIsMatcherWorksOnStatusOr) {
  absl::StatusOr<int> ok = 12;
  absl::StatusOr<int> err = absl::InvalidArgumentError("test");
  EXPECT_THAT(ok, StatusIs(absl::StatusCode::kOk, _));
  EXPECT_THAT(err, StatusIs(absl::StatusCode::kInvalidArgument, _));
  EXPECT_THAT(err, StatusIs(_, StrEq("test")));
}

TEST(StatusTestUtilTest, AssertOkAndAssingWorks) {
  ASSERT_OK_AND_ASSIGN(auto x, absl::StatusOr<int>(12));
  EXPECT_EQ(x, 12);
  ASSERT_OK_AND_ASSIGN(x, absl::StatusOr<int>(14));
  EXPECT_EQ(x, 14);
}

TEST(StatusTestUtilTest, AssertOkAndAssingWorksWithDecomposition) {
  ASSERT_OK_AND_ASSIGN((auto [a, b]),
                       (absl::StatusOr<std::pair<int, int>>({12, 14})));
  EXPECT_EQ(a, 12);
  EXPECT_EQ(b, 14);
}

TEST(StatusTestUtilTest, IsOkAndHoldsAcceptsMatcher) {
  absl::StatusOr<std::pair<int, char>> example(std::make_pair(12, 'a'));
  EXPECT_THAT(example, IsOkAndHolds(std::make_pair(12, 'a')));
  EXPECT_THAT(example, IsOkAndHolds(Pair(12, 'a')));
  EXPECT_THAT(example, IsOkAndHolds(Pair(12, _)));
}

TEST(StatusPrinting, StatusCanBePrinted) {
  absl::Status status = absl::OkStatus();
  EXPECT_THAT(PrintToString(status), StrEq("OK"));
  status = absl::InternalError("something went wrong");
  EXPECT_THAT(PrintToString(status), StrEq("INTERNAL: something went wrong"));
}

TEST(StatusPrinting, StatusOrCanBePrinted) {
  absl::StatusOr<int> status = 12;
  EXPECT_THAT(PrintToString(status), StrEq("OK: 12"));
  status = absl::InternalError("something went wrong");
  EXPECT_THAT(PrintToString(status), StrEq("INTERNAL: something went wrong"));
}

TEST(StatusPrinting, StatusOrNonPrintableCanBePrinted) {
  struct NonPrintable {
    char x;
  };
  absl::StatusOr<NonPrintable> status = NonPrintable{12};
  EXPECT_THAT(PrintToString(status), StrEq("OK: 1-byte object <0C>"));
  status = absl::InternalError("something went wrong");
  EXPECT_THAT(PrintToString(status), StrEq("INTERNAL: something went wrong"));
}

TEST(StatusPrinting, StatusOrRefCanBePrinted) {
  int value = 12;
  StatusOrRef<int> status = value;
  EXPECT_THAT(PrintToString(status), StrEq("OK: 12"));
  status = absl::InternalError("something went wrong");
  EXPECT_THAT(PrintToString(status), StrEq("INTERNAL: something went wrong"));
}

}  // namespace

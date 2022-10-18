#include "common/status_test_util.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gtest/gtest.h"

namespace {

using ::testing::_;
using ::testing::IsOk;
using ::testing::Not;
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

}  // namespace

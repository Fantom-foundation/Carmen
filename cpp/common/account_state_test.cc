#include "common/account_state.h"

#include "common/test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::StrEq;

TEST(AccountStateTest, ValueSize) { EXPECT_EQ(1, sizeof(AccountState)); }

TEST(AccountStateTest, IsTrivial) { EXPECT_TRUE(Trivial<AccountState>); }

TEST(AccountStateTest, IsPrintable) {
  EXPECT_THAT(Print(AccountState::kUnknown), StrEq("unknown"));
  EXPECT_THAT(Print(AccountState::kExists), StrEq("exists"));
  EXPECT_THAT(Print(AccountState(10)), StrEq("invalid"));
}

}  // namespace
}  // namespace carmen

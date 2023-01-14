#include "backend/depot/leveldb/depot.h"

#include "backend/depot/depot.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::depot {
namespace {

using ::testing::_;
using ::testing::ElementsAreArray;
using ::testing::IsOkAndHolds;
using ::testing::StatusIs;

using TestDepot = LevelDbDepot<unsigned long>;

TEST(LevelDbDepotTest, IsDepot) { EXPECT_TRUE(Depot<TestDepot>); }

TEST(LevelDbDepotTest, TestIsPersistent) {
  auto dir = TempDir();
  auto elements = std::array{std::byte{1}, std::byte{2}, std::byte{3}};
  Hash hash;

  {
    ASSERT_OK_AND_ASSIGN(auto depot, TestDepot::Open(dir.GetPath()));
    EXPECT_THAT(depot.Get(10), StatusIs(absl::StatusCode::kNotFound, _));
    EXPECT_THAT(depot.GetHash(), IsOkAndHolds(Hash{}));
    ASSERT_OK(depot.Set(10, elements));
    ASSERT_OK_AND_ASSIGN(hash, depot.GetHash());
  }

  {
    ASSERT_OK_AND_ASSIGN(auto depot, TestDepot::Open(dir.GetPath()));
    EXPECT_THAT(depot.Get(10), IsOkAndHolds(ElementsAreArray(elements)));
    EXPECT_THAT(depot.GetHash(), IsOkAndHolds(hash));
  }
}

}  // namespace
}  // namespace carmen::backend::depot

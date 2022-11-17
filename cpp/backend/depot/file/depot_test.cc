#include "backend/depot/file/depot.h"

#include "common/file_util.h"
#include "common/status_test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::depot {
namespace {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::StatusIs;

using Depot = FileDepot<unsigned long>;

TEST(FileDepotTest, TestIsPersistent) {
  auto dir = TempDir();
  auto elements = std::array{std::byte{1}, std::byte{2}, std::byte{3}};
  Hash hash;

  {
    ASSERT_OK_AND_ASSIGN(auto depot, Depot::Open(dir.GetPath()));
    EXPECT_THAT(depot.Get(10), StatusIs(absl::StatusCode::kNotFound, _));
    ASSERT_OK_AND_ASSIGN(auto empty_hash, depot.GetHash());
    EXPECT_EQ(empty_hash, Hash{});
    ASSERT_OK(depot.Set(10, elements));
    ASSERT_OK_AND_ASSIGN(hash, depot.GetHash());
    ASSERT_OK(depot.Flush());
    ASSERT_OK(depot.Close());
  }

  {
    ASSERT_OK_AND_ASSIGN(auto depot, Depot::Open(dir.GetPath()));
    ASSERT_OK_AND_ASSIGN(auto val, depot.Get(10));
    EXPECT_THAT(val, ElementsAreArray(elements));
    ASSERT_OK_AND_ASSIGN(auto new_hash, depot.GetHash());
    EXPECT_EQ(new_hash, hash);
  }
}

}  // namespace
}  // namespace carmen::backend::depot

#include "backend/depot/file/depot.h"

#include "backend/depot/depot.h"
#include "backend/depot/memory/depot.h"
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
using ::testing::IsOkAndHolds;
using ::testing::StatusIs;

using TestDepot = FileDepot<unsigned long>;
using ReferenceDepot = InMemoryDepot<unsigned long>;

TEST(FileDepotTest, IsDepot) { EXPECT_TRUE(Depot<TestDepot>); }

TEST(FileDepotTest, TestIsPersistent) {
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

TEST(FileDepotTest, FragmentedPathEqualsReference) {
  auto dir = TempDir();
  ASSERT_OK_AND_ASSIGN(
      auto depot, TestDepot::Open(dir.GetPath(), /*hash_branching_factor=*/32,
                                  /*hash_box_size=*/2));
  auto ref_depot =
      ReferenceDepot(/*hash_branching_factor=*/32, /*hash_box_size=*/2);

  // Box size is 2, so these elements will be in the same box.
  auto elements = std::array{std::byte{1}, std::byte{2}};
  ASSERT_OK(depot.Set(0, elements));
  ASSERT_OK(ref_depot.Set(0, elements));
  ASSERT_OK(depot.Set(1, elements));
  ASSERT_OK(ref_depot.Set(1, elements));

  // Assert hashes are same
  ASSERT_OK_AND_ASSIGN(auto hash, depot.GetHash());
  EXPECT_THAT(ref_depot.GetHash(), IsOkAndHolds(hash));

  // Update value of index 0, data will be appended to the end of the data file,
  // so it will get fragmented.
  auto updated_elements = std::array{std::byte{3}, std::byte{4}, std::byte{5}};
  ASSERT_OK(depot.Set(0, updated_elements));
  ASSERT_OK(ref_depot.Set(0, updated_elements));

  // Assert hashes are same, even after fragmentation
  ASSERT_OK_AND_ASSIGN(hash, depot.GetHash());
  EXPECT_THAT(ref_depot.GetHash(), IsOkAndHolds(hash));
}

}  // namespace
}  // namespace carmen::backend::depot

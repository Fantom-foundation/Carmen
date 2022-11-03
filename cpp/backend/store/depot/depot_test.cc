#include "backend/store/depot/memory/depot.h"

#include "backend/store/depot/depot_handler.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::StrEq;

// A test suite testing generic store implementations.
template <typename>
class DepotTest : public testing::Test {};

TYPED_TEST_SUITE_P(DepotTest);

TYPED_TEST_P(DepotTest, TypeProperties) {
  TypeParam wrapper;
  auto& depot = wrapper.GetDepot();
  EXPECT_TRUE(std::is_move_constructible_v<decltype(depot)>);
}

TYPED_TEST_P(DepotTest, DataCanBeAddedAndRetrieved) {
  TypeParam wrapper;
  auto& depot = wrapper.GetDepot();

  ASSERT_OK_AND_ASSIGN(auto val, depot.Get(10));
  EXPECT_THAT(val, IsEmpty());
  ASSERT_OK_AND_ASSIGN(val, depot.Get(100));
  EXPECT_THAT(val, IsEmpty());

  EXPECT_OK(depot.Set(10, std::array{std::byte{1}, std::byte{2}}));
  ASSERT_OK_AND_ASSIGN(val, depot.Get(10));
  EXPECT_THAT(val, ElementsAre(std::byte{1}, std::byte{2}));

  EXPECT_OK(
      depot.Set(100, std::array{std::byte{1}, std::byte{2}, std::byte{3}}));
  ASSERT_OK_AND_ASSIGN(val, depot.Get(100));
  EXPECT_THAT(val, ElementsAre(std::byte{1}, std::byte{2}, std::byte{3}));
}

TYPED_TEST_P(DepotTest, EntriesCanBeUpdated) {
  TypeParam wrapper;
  auto& depot = wrapper.GetDepot();

  EXPECT_OK(depot.Set(10, std::array{std::byte{1}, std::byte{2}}));
  ASSERT_OK_AND_ASSIGN(auto val, depot.Get(10));
  EXPECT_THAT(val, ElementsAre(std::byte{1}, std::byte{2}));

  EXPECT_OK(
      depot.Set(10, std::array{std::byte{1}, std::byte{2}, std::byte{3}}));
  ASSERT_OK_AND_ASSIGN(val, depot.Get(10));
  EXPECT_THAT(val, ElementsAre(std::byte{1}, std::byte{2}, std::byte{3}));
}

TYPED_TEST_P(DepotTest, EmptyDepotHasZeroHash) {
  TypeParam wrapper;
  auto& depot = wrapper.GetDepot();
  ASSERT_OK_AND_ASSIGN(auto hash, depot.GetHash());
  EXPECT_EQ(Hash{}, hash);
}

TYPED_TEST_P(DepotTest, NonEmptyDepotHasHash) {
  TypeParam wrapper;
  auto& depot = wrapper.GetDepot();

  ASSERT_OK_AND_ASSIGN(auto initial_hash, depot.GetHash());
  EXPECT_OK(depot.Set(10, std::array{std::byte{1}, std::byte{2}}));
  ASSERT_OK_AND_ASSIGN(auto new_hash, depot.GetHash());
  ASSERT_NE(initial_hash, new_hash);
}

REGISTER_TYPED_TEST_SUITE_P(DepotTest, TypeProperties,
                            DataCanBeAddedAndRetrieved, EntriesCanBeUpdated,
                            EmptyDepotHasZeroHash, NonEmptyDepotHasHash);

using DepotTypes = ::testing::Types<
    // Branching size 3, Size of box 2.
    DepotHandler<InMemoryDepot<unsigned int>, 32, 2>>;

INSTANTIATE_TYPED_TEST_SUITE_P(All, DepotTest, DepotTypes);

}  // namespace
}  // namespace carmen::backend::store

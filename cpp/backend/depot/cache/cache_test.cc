// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#include "backend/depot/cache/cache.h"

#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/depot/depot.h"
#include "backend/depot/depot_test_suite.h"
#include "backend/depot/memory/depot.h"
#include "backend/depot/test_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::depot {
namespace {

using ::testing::_;
using ::testing::ElementsAreArray;
using ::testing::IsOkAndHolds;
using ::testing::Return;
using ::testing::StatusIs;

using CachedDepot = Cached<InMemoryDepot<unsigned int>>;
using CachedDepotTypes = ::testing::Types<
    // Branching size 3, Size of box 1.
    DepotTestConfig<CachedDepot, 3, 1>,
    // Branching size 3, Size of box 2.
    DepotTestConfig<CachedDepot, 3, 2>,
    // Branching size 16, Size of box 8.
    DepotTestConfig<CachedDepot, 16, 8>,
    // Branching size 32, Size of box 16.
    DepotTestConfig<CachedDepot, 32, 16>>;

// Instantiates common depot tests for the Cached depot type.
INSTANTIATE_TYPED_TEST_SUITE_P(Cached, DepotTest, CachedDepotTypes);

TEST(CachedDepot, IsDepot) { EXPECT_TRUE(Depot<Cached<MockDepot<int>>>); }

TEST(CachedDepot, CachedKeysAreNotFetched) {
  MockDepot<int> wrapper;
  auto& mock = wrapper.GetMockDepot();
  Cached<MockDepot<int>> depot(std::move(wrapper));

  auto val = std::vector<std::byte>{std::byte{1}, std::byte{2}, std::byte{3}};

  // The underlying depot is only accessed once.
  EXPECT_CALL(mock, Get(10))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(val)));

  EXPECT_THAT(depot.Get(10), IsOkAndHolds(ElementsAreArray(val)));
  EXPECT_THAT(depot.Get(10), IsOkAndHolds(ElementsAreArray(val)));
}

TEST(CachedDepot, MissingEntriesAreCached) {
  MockDepot<int> wrapper;
  auto& mock = wrapper.GetMockDepot();
  Cached<MockDepot<int>> depot(std::move(wrapper));

  // The underlying depot is only accessed once.
  EXPECT_CALL(mock, Get(10)).WillOnce(Return(absl::NotFoundError("Not found")));

  auto result = depot.Get(10);
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kNotFound, _));
  result = depot.Get(10);
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kNotFound, _));
}

TEST(CachedDepot, HashesAreCached) {
  MockDepot<int> wrapper;
  auto& mock = wrapper.GetMockDepot();
  Cached<MockDepot<int>> depot(std::move(wrapper));

  // The underlying depot is only accessed once.
  Hash hash{0x01, 0x23};
  EXPECT_CALL(mock, GetHash()).WillOnce(Return(absl::StatusOr<Hash>(hash)));

  EXPECT_THAT(depot.GetHash(), IsOkAndHolds(hash));
  EXPECT_THAT(depot.GetHash(), IsOkAndHolds(hash));
}

TEST(CachedDepot, AddNewElementInvalidatesHash) {
  MockDepot<int> wrapper;
  auto& mock = wrapper.GetMockDepot();
  Cached<MockDepot<int>> depot(std::move(wrapper));

  auto val = std::vector<std::byte>{std::byte{1}, std::byte{2}, std::byte{3}};

  // The underlying depot is computing the hash twice.
  Hash hash_a{0x01, 0x23};
  Hash hash_b{0x45, 0x67};
  EXPECT_CALL(mock, GetHash())
      .WillOnce(Return(absl::StatusOr<Hash>(hash_a)))
      .WillOnce(Return(absl::StatusOr<Hash>(hash_b)));

  EXPECT_THAT(depot.GetHash(), IsOkAndHolds(hash_a));
  EXPECT_THAT(depot.GetHash(), IsOkAndHolds(hash_a));

  EXPECT_CALL(mock, Set(10, _)).WillOnce(Return(absl::OkStatus()));
  ASSERT_OK(depot.Set(10, val));

  EXPECT_THAT(depot.GetHash(), IsOkAndHolds(hash_b));
  EXPECT_THAT(depot.GetHash(), IsOkAndHolds(hash_b));
}

TEST(CachedDepot, CacheSizeLimitIsEnforced) {
  MockDepot<int> wrapper;
  auto& mock = wrapper.GetMockDepot();
  Cached<MockDepot<int>> depot(std::move(wrapper), /*max_entries=*/2);

  auto val = std::vector<std::byte>{std::byte{1}, std::byte{2}, std::byte{3}};

  EXPECT_CALL(mock, Get(0))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(val)));
  EXPECT_CALL(mock, Get(1))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(val)));

  ASSERT_OK(depot.Get(0));
  ASSERT_OK(depot.Get(1));

  // The cache is full, so the first element is evicted.
  EXPECT_CALL(mock, Get(2))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(val)));

  // elements 1 and 2 are in the cache, so 0 is evicted.
  ASSERT_OK(depot.Get(1));
  ASSERT_OK(depot.Get(2));

  // element 0 is evicted, so it is fetched from the underlying depot.
  EXPECT_CALL(mock, Get(0))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(val)));
  ASSERT_OK(depot.Get(0));
}

}  // namespace
}  // namespace carmen::backend::depot

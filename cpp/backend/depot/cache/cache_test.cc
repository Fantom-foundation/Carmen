#include "backend/depot/cache/cache.h"

#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/depot/depot.h"
#include "backend/depot/test_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::depot {
namespace {

using ::testing::_;
using ::testing::ElementsAreArray;
using ::testing::Return;
using ::testing::StatusIs;

TEST(CachedDepot, IsDepot) {
  EXPECT_TRUE(Depot<Cached<MockDepotWrapper<int>>>);
}

TEST(CachedDepot, CachedKeysAreNotFetched) {
  MockDepotWrapper<int> wrapper;
  auto& mock = wrapper.GetMockDepot();
  Cached<MockDepotWrapper<int>> depot(std::move(wrapper));

  auto val = std::vector<std::byte>{std::byte{1}, std::byte{2}, std::byte{3}};

  // The underlying depot is only accessed once.
  EXPECT_CALL(mock, Get(10))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(val)));

  ASSERT_OK_AND_ASSIGN(auto result, depot.Get(10));
  EXPECT_THAT(result, ElementsAreArray(val));
  ASSERT_OK_AND_ASSIGN(result, depot.Get(10));
  EXPECT_THAT(result, ElementsAreArray(val));
}

TEST(CachedDepot, MissingEntriesAreCached) {
  MockDepotWrapper<int> wrapper;
  auto& mock = wrapper.GetMockDepot();
  Cached<MockDepotWrapper<int>> depot(std::move(wrapper));

  // The underlying depot is only accessed once.
  EXPECT_CALL(mock, Get(10)).WillOnce(Return(absl::NotFoundError("Not found")));

  auto result = depot.Get(10);
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kNotFound, _));
  result = depot.Get(10);
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kNotFound, _));
}

TEST(CachedDepot, HashesAreCached) {
  MockDepotWrapper<int> wrapper;
  auto& mock = wrapper.GetMockDepot();
  Cached<MockDepotWrapper<int>> depot(std::move(wrapper));

  // The underlying depot is only accessed once.
  Hash hash{0x01, 0x23};
  EXPECT_CALL(mock, GetHash()).WillOnce(Return(absl::StatusOr<Hash>(hash)));

  ASSERT_OK_AND_ASSIGN(auto result, depot.GetHash());
  EXPECT_EQ(hash, result);
  ASSERT_OK_AND_ASSIGN(result, depot.GetHash());
  EXPECT_EQ(hash, result);
}

TEST(CachedDepot, AddNewElementInvalidatesHash) {
  MockDepotWrapper<int> wrapper;
  auto& mock = wrapper.GetMockDepot();
  Cached<MockDepotWrapper<int>> depot(std::move(wrapper));

  auto val = std::vector<std::byte>{std::byte{1}, std::byte{2}, std::byte{3}};

  // The underlying depot is computing the hash twice.
  Hash hash_a{0x01, 0x23};
  Hash hash_b{0x45, 0x67};
  EXPECT_CALL(mock, GetHash())
      .WillOnce(Return(absl::StatusOr<Hash>(hash_a)))
      .WillOnce(Return(absl::StatusOr<Hash>(hash_b)));

  ASSERT_OK_AND_ASSIGN(auto result, depot.GetHash());
  EXPECT_EQ(hash_a, result);
  ASSERT_OK_AND_ASSIGN(result, depot.GetHash());
  EXPECT_EQ(hash_a, result);

  EXPECT_CALL(mock, Set(10, _)).WillOnce(Return(absl::OkStatus()));
  ASSERT_OK(depot.Set(10, val));

  ASSERT_OK_AND_ASSIGN(result, depot.GetHash());
  EXPECT_EQ(hash_b, result);
  ASSERT_OK_AND_ASSIGN(result, depot.GetHash());
  EXPECT_EQ(hash_b, result);
}

TEST(CachedDepot, CacheSizeLimitIsEnforced) {
  MockDepotWrapper<int> wrapper;
  auto& mock = wrapper.GetMockDepot();
  Cached<MockDepotWrapper<int>> depot(std::move(wrapper), /*max_entries=*/2);

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

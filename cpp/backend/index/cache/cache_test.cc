#include "backend/index/cache/cache.h"

#include <utility>

#include "backend/index/memory/index.h"
#include "backend/index/test_util.h"
#include "common/status_test_util.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::_;
using ::testing::IsOkAndHolds;
using ::testing::Return;
using ::testing::StatusIs;

using TestIndex = InMemoryIndex<int, int>;
using CachedIndex = Cached<TestIndex>;

// Instantiates common index tests for the Cached index type.
INSTANTIATE_TYPED_TEST_SUITE_P(Cached, IndexTest, CachedIndex);

TEST(CachedIndex, CachedKeysAreNotFetched) {
  MockIndexWrapper<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndexWrapper<int, int>> index(std::move(wrapper));

  // The underlying index is only accessed once.
  EXPECT_CALL(mock, GetOrAdd(12))
      .WillOnce(
          Return(absl::StatusOr<std::pair<int, bool>>(std::pair{10, true})));

  EXPECT_THAT(index.GetOrAdd(12), IsOkAndHolds(std::pair(10, true)));
  EXPECT_THAT(index.GetOrAdd(12), IsOkAndHolds(std::pair(10, false)));
  EXPECT_THAT(index.GetOrAdd(12), IsOkAndHolds(std::pair(10, false)));
}

TEST(CachedIndex, MissingEntriesAreCached) {
  MockIndexWrapper<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndexWrapper<int, int>> index(std::move(wrapper));

  // The underlying index is only accessed once.
  EXPECT_CALL(mock, Get(12))
      .WillOnce(Return(absl::NotFoundError("Key not found")));

  EXPECT_THAT(index.Get(12), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_THAT(index.Get(12), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_THAT(index.Get(12), StatusIs(absl::StatusCode::kNotFound, _));
}

TEST(CachedIndex, HashesAreCached) {
  MockIndexWrapper<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndexWrapper<int, int>> index(std::move(wrapper));

  // The underlying index is only accessed once.
  Hash hash{0x01, 0x23};
  EXPECT_CALL(mock, GetHash()).WillOnce(Return(absl::StatusOr<Hash>(hash)));

  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash));
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash));
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash));
}

TEST(CachedIndex, AddNewElementInvalidatesHash) {
  MockIndexWrapper<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndexWrapper<int, int>> index(std::move(wrapper));

  // The underlying index is computing the hash twice.
  Hash hash_a{0x01, 0x23};
  Hash hash_b{0x45, 0x67};
  EXPECT_CALL(mock, GetHash())
      .WillOnce(Return(absl::StatusOr<Hash>(hash_a)))
      .WillOnce(Return(absl::StatusOr<Hash>(hash_b)));

  EXPECT_CALL(mock, GetOrAdd(12))
      .WillOnce(
          Return(absl::StatusOr<std::pair<int, bool>>(std::pair{10, true})));

  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash_a));
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash_a));
  ASSERT_OK_AND_ASSIGN(auto result, index.GetOrAdd(12));
  EXPECT_TRUE(result.second);
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash_b));
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash_b));
}

TEST(CachedIndex, GetExistingElementPreservesHash) {
  MockIndexWrapper<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndexWrapper<int, int>> index(std::move(wrapper));

  // The underlying index is only asked for a hash once.
  Hash hash_a{0x01, 0x23};
  EXPECT_CALL(mock, GetHash()).WillOnce(Return(absl::StatusOr<Hash>(hash_a)));

  EXPECT_CALL(mock, GetOrAdd(12))
      .WillOnce(
          Return(absl::StatusOr<std::pair<int, bool>>(std::pair{10, false})));

  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash_a));
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash_a));
  ASSERT_OK_AND_ASSIGN(auto result, index.GetOrAdd(12));
  EXPECT_FALSE(result.second);
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash_a));
}

TEST(CachedIndex, CacheSizeLimitIsEnforced) {
  MockIndexWrapper<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndexWrapper<int, int>> index(std::move(wrapper),
                                           /*max_entries=*/2);

  // The underlying index is only asked for a hash once.
  EXPECT_CALL(mock, GetOrAdd(0))
      .WillOnce(Return(std::pair{0, true}))
      .WillOnce(Return(std::pair{0, false}));
  EXPECT_CALL(mock, GetOrAdd(1))
      .WillOnce(
          Return(absl::StatusOr<std::pair<int, bool>>(std::pair{1, true})));
  EXPECT_CALL(mock, GetOrAdd(2))
      .WillOnce(
          Return(absl::StatusOr<std::pair<int, bool>>(std::pair{2, true})));

  EXPECT_THAT(index.GetOrAdd(0), IsOkAndHolds(std::pair(0, true)));
  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(std::pair(1, true)));

  // At this point keys 1 and 2 are in the cache, we can query them without
  // reload.
  EXPECT_THAT(index.GetOrAdd(0), IsOkAndHolds(std::pair(0, false)));
  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(std::pair(1, false)));
  EXPECT_THAT(index.GetOrAdd(0), IsOkAndHolds(std::pair(0, false)));
  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(std::pair(1, false)));

  // Asking for key=2 will kick out key 0.
  EXPECT_THAT(index.GetOrAdd(2), IsOkAndHolds(std::pair(2, true)));

  // At this point, key=0 is forgotten. This will trigger a second call.
  EXPECT_THAT(index.GetOrAdd(0), IsOkAndHolds(std::pair(0, false)));
}

}  // namespace
}  // namespace carmen::backend::index

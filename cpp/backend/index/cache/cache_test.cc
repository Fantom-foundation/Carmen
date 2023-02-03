#include "backend/index/cache/cache.h"

#include <utility>

#include "backend/index/test_util.h"
#include "common/status_test_util.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::_;
using ::testing::IsOkAndHolds;
using ::testing::Pair;
using ::testing::Return;
using ::testing::StatusIs;

TEST(CachedIndex, CachedKeysAreNotFetched) {
  MockIndex<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndex<int, int>> index(std::move(wrapper));

  // The underlying index is only accessed once.
  EXPECT_CALL(mock, GetOrAdd(12))
      .WillOnce(
          Return(absl::StatusOr<std::pair<int, bool>>(std::pair{10, true})));

  EXPECT_THAT(index.GetOrAdd(12), IsOkAndHolds(Pair(10, true)));
  EXPECT_THAT(index.GetOrAdd(12), IsOkAndHolds(Pair(10, false)));
  EXPECT_THAT(index.GetOrAdd(12), IsOkAndHolds(Pair(10, false)));
}

TEST(CachedIndex, MissingEntriesAreCached) {
  MockIndex<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndex<int, int>> index(std::move(wrapper));

  // The underlying index is only accessed once.
  EXPECT_CALL(mock, Get(12))
      .WillOnce(Return(absl::NotFoundError("Key not found")));

  EXPECT_THAT(index.Get(12), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_THAT(index.Get(12), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_THAT(index.Get(12), StatusIs(absl::StatusCode::kNotFound, _));
}

TEST(CachedIndex, ErrorStatusIsNotCached) {
  MockIndex<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndex<int, int>> index(std::move(wrapper));

  // The underlying index is accesses repeatedly because error status is not
  // cached.
  EXPECT_CALL(mock, Get(12))
      .Times(2)
      .WillRepeatedly(Return(absl::InternalError("Internal error")));

  EXPECT_THAT(index.Get(12), StatusIs(absl::StatusCode::kInternal, _));
  EXPECT_THAT(index.Get(12), StatusIs(absl::StatusCode::kInternal, _));
}

TEST(CachedIndex, HashesAreCached) {
  MockIndex<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndex<int, int>> index(std::move(wrapper));

  // The underlying index is only accessed once.
  Hash hash{0x01, 0x23};
  EXPECT_CALL(mock, GetHash()).WillOnce(Return(absl::StatusOr<Hash>(hash)));

  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash));
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash));
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash));
}

TEST(CachedIndex, AddNewElementInvalidatesHash) {
  MockIndex<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndex<int, int>> index(std::move(wrapper));

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
  EXPECT_THAT(index.GetOrAdd(12), IsOkAndHolds(Pair(_, true)));
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash_b));
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash_b));
}

TEST(CachedIndex, GetExistingElementPreservesHash) {
  MockIndex<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndex<int, int>> index(std::move(wrapper));

  // The underlying index is only asked for a hash once.
  Hash hash_a{0x01, 0x23};
  EXPECT_CALL(mock, GetHash()).WillOnce(Return(absl::StatusOr<Hash>(hash_a)));

  EXPECT_CALL(mock, GetOrAdd(12))
      .WillOnce(
          Return(absl::StatusOr<std::pair<int, bool>>(std::pair{10, false})));

  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash_a));
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash_a));
  EXPECT_THAT(index.GetOrAdd(12), IsOkAndHolds(Pair(_, false)));
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash_a));
}

TEST(CachedIndex, CacheSizeLimitIsEnforced) {
  MockIndex<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndex<int, int>> index(std::move(wrapper),
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

  EXPECT_THAT(index.GetOrAdd(0), IsOkAndHolds(Pair(0, true)));
  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(Pair(1, true)));

  // At this point keys 1 and 2 are in the cache, we can query them without
  // reload.
  EXPECT_THAT(index.GetOrAdd(0), IsOkAndHolds(Pair(0, false)));
  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(Pair(1, false)));
  EXPECT_THAT(index.GetOrAdd(0), IsOkAndHolds(Pair(0, false)));
  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(Pair(1, false)));

  // Asking for key=2 will kick out key 0.
  EXPECT_THAT(index.GetOrAdd(2), IsOkAndHolds(Pair(2, true)));

  // At this point, key=0 is forgotten. This will trigger a second call.
  EXPECT_THAT(index.GetOrAdd(0), IsOkAndHolds(Pair(0, false)));
}

}  // namespace
}  // namespace carmen::backend::index

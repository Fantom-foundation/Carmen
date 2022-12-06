#include "backend/index/cache/cache.h"

#include <utility>

#include "backend/index/memory/index.h"
#include "backend/index/test_util.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::Return;

using TestIndex = InMemoryIndex<int, int>;
using CachedIndex = Cached<TestIndex>;

// Instantiates common index tests for the Cached index type.
INSTANTIATE_TYPED_TEST_SUITE_P(Cached, IndexTest, CachedIndex);

TEST(CachedIndex, CachedKeysAreNotFetched) {
  MockIndexWrapper<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndexWrapper<int, int>> index(std::move(wrapper));

  // The underlying index is only accessed once.
  EXPECT_CALL(mock, GetOrAdd(12)).WillOnce(Return(std::pair{10, true}));

  EXPECT_EQ((std::pair{10, true}), index.GetOrAdd(12));
  EXPECT_EQ((std::pair{10, false}), index.GetOrAdd(12));
  EXPECT_EQ((std::pair{10, false}), index.GetOrAdd(12));
}

TEST(CachedIndex, MissingEntriesAreCached) {
  MockIndexWrapper<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndexWrapper<int, int>> index(std::move(wrapper));

  // The underlying index is only accessed once.
  EXPECT_CALL(mock, Get(12)).WillOnce(Return(std::nullopt));

  EXPECT_EQ(std::nullopt, index.Get(12));
  EXPECT_EQ(std::nullopt, index.Get(12));
  EXPECT_EQ(std::nullopt, index.Get(12));
}

TEST(CachedIndex, HashesAreCached) {
  MockIndexWrapper<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndexWrapper<int, int>> index(std::move(wrapper));

  // The underlying index is only accessed once.
  Hash hash{0x01, 0x23};
  EXPECT_CALL(mock, GetHash()).WillOnce(Return(absl::StatusOr<Hash>(hash)));

  EXPECT_EQ(hash, *index.GetHash());
  EXPECT_EQ(hash, *index.GetHash());
  EXPECT_EQ(hash, *index.GetHash());
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

  EXPECT_CALL(mock, GetOrAdd(12)).WillOnce(Return(std::pair{10, true}));

  EXPECT_EQ(hash_a, *index.GetHash());
  EXPECT_EQ(hash_a, *index.GetHash());
  EXPECT_TRUE(index.GetOrAdd(12).second);
  EXPECT_EQ(hash_b, *index.GetHash());
  EXPECT_EQ(hash_b, *index.GetHash());
}

TEST(CachedIndex, GetExistingElementPreservesHash) {
  MockIndexWrapper<int, int> wrapper;
  auto& mock = wrapper.GetMockIndex();
  Cached<MockIndexWrapper<int, int>> index(std::move(wrapper));

  // The underlying index is only asked for a hash once.
  Hash hash_a{0x01, 0x23};
  EXPECT_CALL(mock, GetHash()).WillOnce(Return(absl::StatusOr<Hash>(hash_a)));

  EXPECT_CALL(mock, GetOrAdd(12)).WillOnce(Return(std::pair{10, false}));

  EXPECT_EQ(hash_a, *index.GetHash());
  EXPECT_EQ(hash_a, *index.GetHash());
  EXPECT_FALSE(index.GetOrAdd(12).second);
  EXPECT_EQ(hash_a, *index.GetHash());
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
  EXPECT_CALL(mock, GetOrAdd(1)).WillOnce(Return(std::pair{1, true}));
  EXPECT_CALL(mock, GetOrAdd(2)).WillOnce(Return(std::pair{2, true}));

  EXPECT_EQ((std::pair{0, true}), index.GetOrAdd(0));
  EXPECT_EQ((std::pair{1, true}), index.GetOrAdd(1));

  // At this point keys 1 and 2 are in the cache, we can query them without
  // reload.
  EXPECT_EQ((std::pair{0, false}), index.GetOrAdd(0));
  EXPECT_EQ((std::pair{1, false}), index.GetOrAdd(1));
  EXPECT_EQ((std::pair{0, false}), index.GetOrAdd(0));
  EXPECT_EQ((std::pair{1, false}), index.GetOrAdd(1));

  // Asking for key=2 will kick out key 0.
  EXPECT_EQ((std::pair{2, true}), index.GetOrAdd(2));

  // At this point, key=0 is forgotten. This will trigger a second call.
  EXPECT_EQ((std::pair{0, false}), index.GetOrAdd(0));
}

}  // namespace
}  // namespace carmen::backend::index

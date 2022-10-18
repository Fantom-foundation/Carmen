#pragma once

#include <type_traits>

#include "backend/index/index.h"
#include "common/hash.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {

using ::testing::Optional;

// Implements a generic test suite for index implementations checking basic
// properties like GetOrAdd, contains, and hashing functionality.
template <typename IndexHandler>
class IndexTest : public testing::Test {};

TYPED_TEST_SUITE_P(IndexTest);

TYPED_TEST_P(IndexTest, TypeProperties) {
  TypeParam wrapper;
  auto& index = wrapper.GetIndex();
  EXPECT_TRUE(std::is_move_constructible_v<decltype(index)>);
}

TYPED_TEST_P(IndexTest, IdentifiersAreAssignedInorder) {
  TypeParam wrapper;
  auto& index = wrapper.GetIndex();
  EXPECT_EQ(std::pair(0, true), index.GetOrAdd(1));
  EXPECT_EQ(std::pair(1, true), index.GetOrAdd(2));
  EXPECT_EQ(std::pair(2, true), index.GetOrAdd(3));
}

TYPED_TEST_P(IndexTest, SameKeyLeadsToSameIdentifier) {
  TypeParam wrapper;
  auto& index = wrapper.GetIndex();
  EXPECT_EQ(std::pair(0, true), index.GetOrAdd(1));
  EXPECT_EQ(std::pair(1, true), index.GetOrAdd(2));
  EXPECT_EQ(std::pair(0, false), index.GetOrAdd(1));
  EXPECT_EQ(std::pair(1, false), index.GetOrAdd(2));
}

TYPED_TEST_P(IndexTest, ContainsIdentifiesIndexedElements) {
  TypeParam wrapper;
  auto& index = wrapper.GetIndex();
  EXPECT_FALSE(index.Get(1));
  EXPECT_FALSE(index.Get(2));
  EXPECT_FALSE(index.Get(3));

  EXPECT_EQ(std::pair(0, true), index.GetOrAdd(1));
  EXPECT_TRUE(index.Get(1));
  EXPECT_FALSE(index.Get(2));
  EXPECT_FALSE(index.Get(3));

  EXPECT_EQ(std::pair(1, true), index.GetOrAdd(2));
  EXPECT_TRUE(index.Get(1));
  EXPECT_TRUE(index.Get(2));
  EXPECT_FALSE(index.Get(3));
}

TYPED_TEST_P(IndexTest, GetRetrievesPresentKeys) {
  TypeParam wrapper;
  auto& index = wrapper.GetIndex();
  EXPECT_EQ(index.Get(1), std::nullopt);
  EXPECT_EQ(index.Get(2), std::nullopt);
  auto id1 = index.GetOrAdd(1).first;
  EXPECT_THAT(index.Get(1), Optional(id1));
  EXPECT_EQ(index.Get(2), std::nullopt);
  auto id2 = index.GetOrAdd(2).first;
  EXPECT_THAT(index.Get(1), Optional(id1));
  EXPECT_THAT(index.Get(2), Optional(id2));
}

TYPED_TEST_P(IndexTest, EmptyIndexHasHashEqualsZero) {
  TypeParam wrapper;
  auto& index = wrapper.GetIndex();
  EXPECT_EQ(Hash{}, index.GetHash());
}

TYPED_TEST_P(IndexTest, IndexHashIsEqualToInsertionOrder) {
  Hash hash;
  TypeParam wrapper;
  auto& index = wrapper.GetIndex();
  EXPECT_EQ(hash, index.GetHash());
  index.GetOrAdd(12);
  hash = GetSha256Hash(hash, 12);
  EXPECT_EQ(hash, index.GetHash());
  index.GetOrAdd(14);
  hash = GetSha256Hash(hash, 14);
  EXPECT_EQ(hash, index.GetHash());
  index.GetOrAdd(16);
  hash = GetSha256Hash(hash, 16);
  EXPECT_EQ(hash, index.GetHash());
}

REGISTER_TYPED_TEST_SUITE_P(IndexTest, TypeProperties,
                            IdentifiersAreAssignedInorder,
                            SameKeyLeadsToSameIdentifier,
                            ContainsIdentifiesIndexedElements,
                            GetRetrievesPresentKeys,
                            EmptyIndexHasHashEqualsZero,
                            IndexHashIsEqualToInsertionOrder);

// A generic mock implementation for mocking out index implementations.
template <typename K, typename V>
class MockIndex {
 public:
  using key_type = K;
  using value_type = V;
  MOCK_METHOD((std::pair<V, bool>), GetOrAdd, (const K& key));
  MOCK_METHOD((std::optional<V>), Get, (const K& key), (const));
  MOCK_METHOD(Hash, GetHash, ());
};

// A movable wrapper of a mock index. This may be required when a index needs to
// be moved into position.
template <typename K, typename V>
class MockIndexWrapper {
 public:
  using key_type = K;
  using value_type = V;

  MockIndexWrapper() : index_(std::make_unique<MockIndex<K, V>>()) {}

  std::pair<V, bool> GetOrAdd(const K& key) { return index_->GetOrAdd(key); }

  std::optional<V> Get(const K& key) const { return index_->Get(key); }

  Hash GetHash() { return index_->GetHash(); }

  // Returns a reference to the wrapped MockIndex. This pointer is stable.
  MockIndex<K, V>& GetMockIndex() { return *index_; }

 private:
  std::unique_ptr<MockIndex<K, V>> index_;
};

}  // namespace carmen::backend::index

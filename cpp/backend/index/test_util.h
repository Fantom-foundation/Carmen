#pragma once

#include <filesystem>
#include <type_traits>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/index/index.h"
#include "backend/index/index_handler.h"
#include "backend/structure.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {

using ::testing::_;
using ::testing::IsOk;
using ::testing::IsOkAndHolds;
using ::testing::Optional;
using ::testing::StatusIs;

// Implements a generic test suite for index implementations checking basic
// properties like GetOrAdd, contains, and hashing functionality.
template <Index I>
class IndexTest : public testing::Test {};

TYPED_TEST_SUITE_P(IndexTest);

TYPED_TEST_P(IndexTest, TypeProperties) {
  IndexHandler<TypeParam> wrapper;
  auto& index = wrapper.GetIndex();
  EXPECT_TRUE(std::is_move_constructible_v<decltype(index)>);
}

TYPED_TEST_P(IndexTest, IdentifiersAreAssignedInorder) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();
  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(std::pair(0, true)));
  EXPECT_THAT(index.GetOrAdd(2), IsOkAndHolds(std::pair(1, true)));
  EXPECT_THAT(index.GetOrAdd(3), IsOkAndHolds(std::pair(2, true)));
}

TYPED_TEST_P(IndexTest, SameKeyLeadsToSameIdentifier) {
  IndexHandler<TypeParam> wrapper;
  auto& index = wrapper.GetIndex();
  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(std::pair(0, true)));
  EXPECT_THAT(index.GetOrAdd(2), IsOkAndHolds(std::pair(1, true)));
  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(std::pair(0, false)));
  EXPECT_THAT(index.GetOrAdd(2), IsOkAndHolds(std::pair(1, false)));
}

TYPED_TEST_P(IndexTest, ContainsIdentifiesIndexedElements) {
  IndexHandler<TypeParam> wrapper;
  auto& index = wrapper.GetIndex();

  EXPECT_THAT(index.Get(1), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_THAT(index.Get(2), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_THAT(index.Get(3), StatusIs(absl::StatusCode::kNotFound, _));

  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(std::pair(0, true)));
  EXPECT_THAT(index.Get(1), IsOk());
  EXPECT_THAT(index.Get(2), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_THAT(index.Get(3), StatusIs(absl::StatusCode::kNotFound, _));

  EXPECT_THAT(index.GetOrAdd(2), IsOkAndHolds(std::pair(1, true)));
  EXPECT_THAT(index.Get(1), IsOk());
  EXPECT_THAT(index.Get(2), IsOk());
  EXPECT_THAT(index.Get(3), StatusIs(absl::StatusCode::kNotFound, _));
}

TYPED_TEST_P(IndexTest, GetRetrievesPresentKeys) {
  IndexHandler<TypeParam> wrapper;
  auto& index = wrapper.GetIndex();
  EXPECT_THAT(index.Get(1), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_THAT(index.Get(2), StatusIs(absl::StatusCode::kNotFound, _));

  ASSERT_OK_AND_ASSIGN(auto id1, index.GetOrAdd(1));
  EXPECT_THAT(index.Get(1), IsOkAndHolds(id1.first));

  EXPECT_THAT(index.Get(2), StatusIs(absl::StatusCode::kNotFound, _));
  ASSERT_OK_AND_ASSIGN(auto id2, index.GetOrAdd(2));

  EXPECT_THAT(index.Get(2), IsOkAndHolds(id2.first));
  EXPECT_THAT(index.Get(1), IsOkAndHolds(id1.first));
}

TYPED_TEST_P(IndexTest, EmptyIndexHasHashEqualsZero) {
  IndexHandler<TypeParam> wrapper;
  auto& index = wrapper.GetIndex();
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(Hash{}));
}

TYPED_TEST_P(IndexTest, IndexHashIsEqualToInsertionOrder) {
  Hash hash;
  IndexHandler<TypeParam> wrapper;
  auto& index = wrapper.GetIndex();
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash));
  ASSERT_OK(index.GetOrAdd(12));
  hash = GetSha256Hash(hash, 12);
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash));
  ASSERT_OK(index.GetOrAdd(14));
  hash = GetSha256Hash(hash, 14);
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash));
  ASSERT_OK(index.GetOrAdd(16));
  hash = GetSha256Hash(hash, 16);
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash));
}

TYPED_TEST_P(IndexTest, CanProduceMemoryFootprint) {
  IndexHandler<TypeParam> wrapper;
  auto& index = wrapper.GetIndex();
  auto summary = index.GetMemoryFootprint();
  EXPECT_GT(summary.GetTotal(), Memory(0));
}

TYPED_TEST_P(IndexTest, HashesMatchReferenceImplementation) {
  IndexHandler<TypeParam> wrapper;
  auto& index = wrapper.GetIndex();
  auto& reference_index = wrapper.GetReferenceIndex();

  ASSERT_OK(index.GetOrAdd(1));
  ASSERT_OK(index.GetOrAdd(2));
  ASSERT_OK(index.GetOrAdd(3));

  ASSERT_OK(reference_index.GetOrAdd(1));
  ASSERT_OK(reference_index.GetOrAdd(2));
  ASSERT_OK(reference_index.GetOrAdd(3));

  ASSERT_OK_AND_ASSIGN(auto hash, index.GetHash());
  EXPECT_THAT(reference_index.GetHash(), IsOkAndHolds(hash));
}

REGISTER_TYPED_TEST_SUITE_P(
    IndexTest, TypeProperties, IdentifiersAreAssignedInorder,
    SameKeyLeadsToSameIdentifier, ContainsIdentifiesIndexedElements,
    GetRetrievesPresentKeys, EmptyIndexHasHashEqualsZero,
    IndexHashIsEqualToInsertionOrder, CanProduceMemoryFootprint,
    HashesMatchReferenceImplementation);

// A generic mock implementation for mocking out index implementations.
template <typename K, typename V>
class MockIndex {
 public:
  using key_type = K;
  using value_type = V;
  static absl::StatusOr<MockIndex> Open(Context&,
                                        const std::filesystem::path&){};
  MOCK_METHOD((absl::StatusOr<std::pair<V, bool>>), GetOrAdd, (const K& key));
  MOCK_METHOD((absl::StatusOr<V>), Get, (const K& key), (const));
  MOCK_METHOD(absl::StatusOr<Hash>, GetHash, ());
  MOCK_METHOD(absl::Status, Flush, ());
  MOCK_METHOD(absl::Status, Close, ());
  MOCK_METHOD(MemoryFootprint, GetMemoryFootprint, (), (const));
};

// A movable wrapper of a mock index. This may be required when an index needs
// to be moved into position.
template <typename K, typename V>
class MockIndexWrapper {
 public:
  using key_type = K;
  using value_type = V;

  static absl::StatusOr<MockIndexWrapper> Open(Context&,
                                               const std::filesystem::path&) {
    return MockIndexWrapper();
  }

  MockIndexWrapper() : index_(std::make_unique<MockIndex<K, V>>()) {}
  MockIndexWrapper(MockIndexWrapper&&) noexcept = default;

  absl::StatusOr<std::pair<V, bool>> GetOrAdd(const K& key) {
    return index_->GetOrAdd(key);
  }

  absl::StatusOr<V> Get(const K& key) const { return index_->Get(key); }

  absl::StatusOr<Hash> GetHash() { return index_->GetHash(); }

  absl::Status Flush() { return index_->Flush(); }

  absl::Status Close() { return index_->Close(); }

  MemoryFootprint GetMemoryFootprint() const { index_->GetMemoryFootprint(); }

  // Returns a reference to the wrapped MockIndex. This pointer is stable.
  MockIndex<K, V>& GetMockIndex() { return *index_; }

 private:
  std::unique_ptr<MockIndex<K, V>> index_;
};

}  // namespace carmen::backend::index

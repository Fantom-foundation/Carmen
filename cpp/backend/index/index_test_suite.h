#pragma once

#include <filesystem>
#include <type_traits>

#include "absl/status/status.h"
#include "backend/index/index.h"
#include "backend/index/index_handler.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_test_util.h"
#include "common/type.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {

using ::testing::_;
using ::testing::IsOk;
using ::testing::IsOkAndHolds;
using ::testing::Not;
using ::testing::Optional;
using ::testing::Pair;
using ::testing::StatusIs;

// Implements a generic test suite for index implementations checking basic
// properties like GetOrAdd, contains, and hashing functionality.
template <Index I>
class IndexTest : public testing::Test {};

TYPED_TEST_SUITE_P(IndexTest);

TYPED_TEST_P(IndexTest, TypeProperties) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
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
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();
  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(std::pair(0, true)));
  EXPECT_THAT(index.GetOrAdd(2), IsOkAndHolds(std::pair(1, true)));
  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(std::pair(0, false)));
  EXPECT_THAT(index.GetOrAdd(2), IsOkAndHolds(std::pair(1, false)));
}

TYPED_TEST_P(IndexTest, ContainsIdentifiesIndexedElements) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
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
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
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
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(Hash{}));
}

TYPED_TEST_P(IndexTest, IndexHashIsEqualToInsertionOrder) {
  Hash hash{};
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
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
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();
  auto summary = index.GetMemoryFootprint();
  EXPECT_GT(summary.GetTotal(), Memory(0));
}

TYPED_TEST_P(IndexTest, HashesMatchReferenceImplementation) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
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

// TODO: remove this and all its calls once all indexes support snapshots.
template <typename Index>
bool SupportsSnapshots(const Index& index) {
  auto s = index.CreateSnapshot();
  return s.ok() || s.status().code() != absl::StatusCode::kUnimplemented;
}

TYPED_TEST_P(IndexTest, SnapshotHasSameProofAsIndex) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(index)) {
    GTEST_SKIP();
  }

  ASSERT_OK_AND_ASSIGN(auto snapshot1, index.CreateSnapshot());
  EXPECT_THAT(index.GetProof(), snapshot1.GetProof());

  EXPECT_OK(index.GetOrAdd(10));
  EXPECT_THAT(index.GetProof(), Not(snapshot1.GetProof()));

  ASSERT_OK_AND_ASSIGN(auto snapshot2, index.CreateSnapshot());
  EXPECT_THAT(index.GetProof(), snapshot2.GetProof());
}

TYPED_TEST_P(IndexTest, SnapshotShieldsMutations) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(index)) {
    GTEST_SKIP();
  }

  EXPECT_THAT(index.GetOrAdd(10), IsOkAndHolds(Pair(0, true)));
  EXPECT_THAT(index.GetOrAdd(12), IsOkAndHolds(Pair(1, true)));
  ASSERT_OK_AND_ASSIGN(auto snapshot, index.CreateSnapshot());

  EXPECT_THAT(index.GetOrAdd(14), IsOkAndHolds(Pair(2, true)));

  ASSERT_OK_AND_ASSIGN(auto wrapper2, IndexHandler<TypeParam>::Create());
  auto& restored = wrapper2.GetIndex();
  EXPECT_OK(restored.SyncTo(snapshot));
  EXPECT_THAT(restored.Get(10), 0);
  EXPECT_THAT(restored.Get(12), 1);
  EXPECT_THAT(restored.GetOrAdd(14), IsOkAndHolds(Pair(2, true)));
}

TYPED_TEST_P(IndexTest, SnapshotRecoveryHasSameHash) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(index)) {
    GTEST_SKIP();
  }

  ASSERT_OK(index.GetOrAdd(10));
  ASSERT_OK_AND_ASSIGN(auto hash, index.GetHash());
  ASSERT_OK_AND_ASSIGN(auto snapshot, index.CreateSnapshot());

  ASSERT_OK_AND_ASSIGN(auto wrapper2, IndexHandler<TypeParam>::Create());
  auto& restored = wrapper2.GetIndex();
  EXPECT_OK(restored.SyncTo(snapshot));
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
}

TYPED_TEST_P(IndexTest, LargeSnapshotRecoveryWorks) {
  constexpr const int kNumElements = 100000;

  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(index)) {
    GTEST_SKIP();
  }

  for (int i = 0; i < kNumElements; i++) {
    EXPECT_THAT(index.GetOrAdd(i + 10), IsOkAndHolds(Pair(i, true)));
  }
  ASSERT_OK_AND_ASSIGN(auto hash, index.GetHash());
  ASSERT_OK_AND_ASSIGN(auto snapshot, index.CreateSnapshot());
  EXPECT_LT(50, snapshot.GetSize());

  ASSERT_OK_AND_ASSIGN(auto wrapper2, IndexHandler<TypeParam>::Create());
  auto& restored = wrapper2.GetIndex();
  EXPECT_OK(restored.SyncTo(snapshot));
  for (int i = 0; i < kNumElements; i++) {
    EXPECT_THAT(index.Get(i + 10), IsOkAndHolds(i));
  }
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
}

TYPED_TEST_P(IndexTest, SnapshotsCanBeVerified) {
  constexpr const int kNumElements = 100000;

  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(index)) {
    GTEST_SKIP();
  }

  for (int i = 0; i < kNumElements; i++) {
    EXPECT_THAT(index.GetOrAdd(i + 10), IsOkAndHolds(Pair(i, true)));
  }
  ASSERT_OK_AND_ASSIGN(auto snapshot, index.CreateSnapshot());
  EXPECT_LT(50, snapshot.GetSize());

  // This step verifies that the proofs are consistent.
  EXPECT_OK(snapshot.VerifyProofs());

  // Verify that the content of parts is consistent with the proofs.
  for (std::size_t i = 0; i < snapshot.GetSize(); i++) {
    ASSERT_OK_AND_ASSIGN(auto proof, snapshot.GetProof(i));
    ASSERT_OK_AND_ASSIGN(auto part, snapshot.GetPart(i));
    EXPECT_THAT(part.GetProof(), proof);
    EXPECT_TRUE(part.Verify());
  }
}

TYPED_TEST_P(IndexTest, AnEmptySnapshotCanBeVerified) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(index)) {
    GTEST_SKIP();
  }

  ASSERT_OK_AND_ASSIGN(auto snapshot, index.CreateSnapshot());
  EXPECT_EQ(0, snapshot.GetSize());
  EXPECT_OK(snapshot.VerifyProofs());
}

REGISTER_TYPED_TEST_SUITE_P(
    IndexTest, TypeProperties, IdentifiersAreAssignedInorder,
    SameKeyLeadsToSameIdentifier, ContainsIdentifiesIndexedElements,
    GetRetrievesPresentKeys, EmptyIndexHasHashEqualsZero,
    IndexHashIsEqualToInsertionOrder, CanProduceMemoryFootprint,
    HashesMatchReferenceImplementation, SnapshotHasSameProofAsIndex,
    LargeSnapshotRecoveryWorks, SnapshotRecoveryHasSameHash,
    SnapshotShieldsMutations, SnapshotsCanBeVerified,
    AnEmptySnapshotCanBeVerified);

}  // namespace carmen::backend::index

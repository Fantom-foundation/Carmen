#pragma once

#include <string>
#include <vector>

#include "backend/store/store.h"
#include "backend/store/store_handler.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "common/test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using ::testing::IsOkAndHolds;
using ::testing::Not;
using ::testing::StrEq;

// A test configuration for store implementations.
template <template <typename K, typename V, std::size_t page_size> class store,
          std::size_t page_size, std::size_t branching_factor>
using StoreTestConfig = StoreHandler<store, page_size, branching_factor>;

Value ToValue(std::int64_t value) {
  return Value{static_cast<std::uint8_t>(value >> 32),
               static_cast<std::uint8_t>(value >> 24),
               static_cast<std::uint8_t>(value >> 16),
               static_cast<std::uint8_t>(value >> 8),
               static_cast<std::uint8_t>(value >> 0)};
}

// A test suite testing generic store implementations.
template <typename StoreHandler>
class StoreTest : public testing::Test {};

TYPED_TEST_SUITE_P(StoreTest);

TYPED_TEST_P(StoreTest, TypeProperties) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  static_assert(Store<std::decay_t<decltype(wrapper.GetStore())>>);
  EXPECT_TRUE(std::is_move_constructible_v<decltype(wrapper.GetStore())>);
}

TYPED_TEST_P(StoreTest, UninitializedValuesAreZero) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();
  EXPECT_THAT(store.Get(0), IsOkAndHolds(Value{}));
  EXPECT_THAT(store.Get(10), IsOkAndHolds(Value{}));
  EXPECT_THAT(store.Get(100), IsOkAndHolds(Value{}));
}

TYPED_TEST_P(StoreTest, DataCanBeAddedAndRetrieved) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();
  EXPECT_THAT(store.Get(10), IsOkAndHolds(Value{}));
  EXPECT_THAT(store.Get(12), IsOkAndHolds(Value{}));

  ASSERT_OK(store.Set(10, Value{12}));
  EXPECT_THAT(store.Get(10), IsOkAndHolds(Value{12}));
  EXPECT_THAT(store.Get(12), IsOkAndHolds(Value{}));

  ASSERT_OK(store.Set(12, Value{14}));
  EXPECT_THAT(store.Get(10), IsOkAndHolds(Value{12}));
  EXPECT_THAT(store.Get(12), IsOkAndHolds(Value{14}));
}

TYPED_TEST_P(StoreTest, EntriesCanBeUpdated) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();
  EXPECT_THAT(store.Get(10), IsOkAndHolds(Value{}));
  ASSERT_OK(store.Set(10, Value{12}));
  EXPECT_THAT(store.Get(10), IsOkAndHolds(Value{12}));
  ASSERT_OK(store.Set(10, Value{14}));
  EXPECT_THAT(store.Get(10), IsOkAndHolds(Value{14}));
}

TYPED_TEST_P(StoreTest, EmptyStoreHasZeroHash) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();
  EXPECT_THAT(store.GetHash(), IsOkAndHolds(Hash{}));
}

TYPED_TEST_P(StoreTest, HashesChangeWithUpdates) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();

  ASSERT_OK_AND_ASSIGN(auto empty_hash, store.GetHash());
  ASSERT_OK(store.Set(1, Value{0xAA}));
  ASSERT_OK_AND_ASSIGN(auto hash_a, store.GetHash());
  EXPECT_NE(empty_hash, hash_a);
  ASSERT_OK(store.Set(2, Value{0xFF}));
  ASSERT_OK_AND_ASSIGN(auto hash_b, store.GetHash());
  EXPECT_NE(empty_hash, hash_b);
  EXPECT_NE(hash_a, hash_b);
}

TYPED_TEST_P(StoreTest, HashesDoNotChangeWithReads) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();

  ASSERT_OK_AND_ASSIGN(auto empty_hash, store.GetHash());
  EXPECT_THAT(store.Get(1), IsOkAndHolds(Value{}));
  EXPECT_THAT(store.GetHash(), empty_hash);
  EXPECT_THAT(store.Get(10000), IsOkAndHolds(Value{}));
  EXPECT_THAT(store.GetHash(), empty_hash);

  EXPECT_OK(store.Set(10, Value{0xAA}));
  ASSERT_OK_AND_ASSIGN(auto non_empty_hash, store.GetHash());
  EXPECT_NE(empty_hash, non_empty_hash);
  EXPECT_THAT(store.Get(1), IsOkAndHolds(Value{}));
  EXPECT_THAT(store.GetHash(), non_empty_hash);
  EXPECT_THAT(store.Get(10000), IsOkAndHolds(Value{}));
  EXPECT_THAT(store.GetHash(), non_empty_hash);
}

TYPED_TEST_P(StoreTest, HashesCoverMultiplePages) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();

  ASSERT_OK_AND_ASSIGN(auto empty_hash, store.GetHash());
  for (int i = 0; i < 10000; i++) {
    ASSERT_OK(store.Set(i, ToValue(i + 1)));
  }
  ASSERT_OK_AND_ASSIGN(auto hash_a, store.GetHash());
  EXPECT_NE(empty_hash, hash_a);
  ASSERT_OK(store.Set(5000, Value{}));
  ASSERT_OK_AND_ASSIGN(auto hash_b, store.GetHash());
  EXPECT_NE(empty_hash, hash_b);
  EXPECT_NE(hash_a, hash_b);
}

TYPED_TEST_P(StoreTest, KnownHashesAreReproduced) {
  // We only hard-code hashes for a subset of the configurations.
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();
  EXPECT_THAT(store.GetHash(), IsOkAndHolds(Hash{}));

  if (TypeParam::kPageSize == 32 && TypeParam::kBranchingFactor == 32) {
    EXPECT_THAT(store.Get(0), IsOkAndHolds(Value{}));
    ASSERT_OK(store.Set(0, Value{}));
    ASSERT_OK_AND_ASSIGN(auto hash, store.GetHash());
    EXPECT_THAT(
        Print(hash),
        StrEq("0x66687aadf862bd776c8fc18b8e9f8e20089714856ee233b3902a591d"
              "0d5f2925"));

    EXPECT_THAT(store.Get(0), IsOkAndHolds(Value{}));
    ASSERT_OK(store.Set(0, Value{0xAA}));
    ASSERT_OK_AND_ASSIGN(hash, store.GetHash());
    EXPECT_THAT(
        Print(hash),
        StrEq("0xe7ac50af91de0eca8d6805f0cf111ac4f0937e3136292cace6a50392"
              "fe905615"));

    EXPECT_THAT(store.Get(1), IsOkAndHolds(Value{}));
    ASSERT_OK(store.Set(1, Value{0xBB}));
    ASSERT_OK_AND_ASSIGN(hash, store.GetHash());
    EXPECT_THAT(
        Print(hash),
        StrEq("0x1e7272c135640b8d6f1bb58f4887f022eddc7f21d077439c14bfb22f"
              "15952d5d"));

    EXPECT_THAT(store.Get(2), IsOkAndHolds(Value{}));
    ASSERT_OK(store.Set(2, Value{0xCC}));
    ASSERT_OK_AND_ASSIGN(hash, store.GetHash());
    EXPECT_THAT(
        Print(hash),
        StrEq("0xaf87d5bc44995a6d537df52a75ef073ff24581aef087e37ec981035b"
              "6b0072e4"));
  }

  if (TypeParam::kPageSize == 64 && TypeParam::kBranchingFactor == 3) {
    // Tests the hashes for values 0x00, 0x11 ... 0xFF inserted in sequence.
    std::vector<std::string> hashes{
        "0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b",
        "0x967293ee9d7ba679c3ef076bef139e2ceb96d45d19a624cc59bb5a3c1649ce38",
        "0x37617dfcbf34b6bd41ef1ba985de1e68b69bf4e42815981868abde09e9e09f0e",
        "0x735e056698bd4b4953a9838c4526c4d2138efd1aee9a94ff36ca100f16a77581",
        "0xc1e116b85f59f2ef61d6a64e61947e33c383f0adf252a3249b6172286ca244aa",
        "0x6001791dfa74121b9d177091606ebcd352e784ecfab05563c40b7ce8346c6f98",
        "0x57aee44f007524162c86d8ab0b1c67ed481c44d248c5f9c48fca5a5368d3a705",
        "0xdd29afc37e669458a3f4509023bf5a362f0c0cdc9bb206a6955a8f5124d26086",
        "0x0ab5ad3ab4f3efb90994cdfd72b2aa0532cc0f9708ea8fb8555677053583e161",
        "0x901d25766654678c6fe19c3364f34f9ed7b649514b9b5b25389de3bbfa346957",
        "0x50743156d6a4967c165a340166d31ca986ceebbb1812aebb3ce744ce7cffaa99",
        "0x592fd0da56dbc41e7ae8d4572c47fe12492eca9ae68b8786ebc322c2e2d61de2",
        "0xbc57674bfa2b806927af318a51025d833f5950ed6cdab5af3c8a876dac5ba1c4",
        "0x6523527158ccde9ed47932da61fed960019843f31f1fdbab3d18958450a00e0f",
        "0xe1bf187a4cd645c7adae643070f070dcb9c4aa8bbc0aded07b99dda3bac6b0ea",
        "0x9a5be401e5aa0b2b31a3b055811b15041f4842be6cd4cb146f3c2b48e2081e19",
        "0x6f060e465bb1b155a6b4822a13b704d3986ab43d7928c14b178e07a8f7673951",
    };
    int i = 0;
    Hash hash;
    for (const auto& expected_hash : hashes) {
      EXPECT_THAT(store.Get(i), IsOkAndHolds(Value{}));
      ASSERT_OK(store.Set(i, Value{static_cast<std::uint8_t>(i << 4 | i)}));
      ASSERT_OK_AND_ASSIGN(hash, store.GetHash());
      EXPECT_THAT(Print(hash), StrEq(expected_hash));
      i++;
    }
  }
}

TYPED_TEST_P(StoreTest, HashComputationIgnoresUnusedBytes) {
  // This test computes hashes for values not fully occupying each page.
  using Value = std::array<char, 7>;

  // There is space for one value and some padding is required.
  static_assert(TypeParam::kPageSize / sizeof(Value) > 1);
  static_assert(TypeParam::kPageSize % sizeof(Value) != 0);

  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto store, TypeParam::template Create<Value>(dir));
  ASSERT_OK(store.Set(0, Value{1, 2, 3}));

  // The hash should be the hash of the content of the only page.
  constexpr std::size_t kContentSize =
      TypeParam::kPageSize / sizeof(Value) * sizeof(Value);
  std::array<char, kContentSize> content{1, 2, 3};
  EXPECT_THAT(store.GetHash(), GetSha256Hash(content));
}

TYPED_TEST_P(StoreTest, HashesRespectBranchingFactor) {
  // This test computes the hash expected for a store containing 2*branching
  // factor empty pages.
  static_assert(TypeParam::kPageSize % sizeof(Value) == 0);
  constexpr auto kElementsPerPage = TypeParam::kPageSize / sizeof(Value);
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();

  // Initialize branching_factor * 2 pages.
  ASSERT_OK(store.Set(kElementsPerPage * TypeParam::kBranchingFactor * 2 - 1,
                      Value{}));

  // This should result in a hash tree with branching_factor * 2 leaves and one
  // inner node forming the root.
  Sha256Hasher hasher;

  // Get the hash of an empty page.
  std::array<std::byte, TypeParam::kPageSize> page{};
  Hash page_hash = GetHash(hasher, page);

  // Get the combined hash of branching-factor many empty pages.
  hasher.Reset();
  for (std::size_t i = 0; i < TypeParam::kBranchingFactor; i++) {
    hasher.Ingest(page_hash);
  }
  Hash block_hash = hasher.GetHash();

  // Compute the hash of the inner node, consisting of two block hashes followed
  // by zero hashes.
  hasher.Reset();
  hasher.Ingest(block_hash);
  hasher.Ingest(block_hash);
  for (std::size_t i = 2; i < TypeParam::kBranchingFactor; i++) {
    hasher.Ingest(Hash{});
  }
  Hash root_hash = hasher.GetHash();

  EXPECT_THAT(store.GetHash(), IsOkAndHolds(root_hash));
}

TYPED_TEST_P(StoreTest, HashesEqualReferenceImplementation) {
  constexpr int N = 100;
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();
  auto& reference = wrapper.GetReferenceStore();

  EXPECT_THAT(store.GetHash(), IsOkAndHolds(Hash{}));

  Hash hash;
  for (int i = 0; i < N; i++) {
    Value value{static_cast<unsigned char>(i >> 6 & 0x3),
                static_cast<unsigned char>(i >> 4 & 0x3),
                static_cast<unsigned char>(i >> 2 & 0x3),
                static_cast<unsigned char>(i >> 0 & 0x3)};
    ASSERT_OK(store.Set(i, value));
    ASSERT_OK(reference.Set(i, value));
    ASSERT_OK_AND_ASSIGN(hash, store.GetHash());
    EXPECT_THAT(reference.GetHash(), IsOkAndHolds(hash));
  }
}

TYPED_TEST_P(StoreTest, HashesRespectEmptyPages) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();
  auto& reference = wrapper.GetReferenceStore();

  // Implicitly create empty pages by setting an element with a high ID.
  ASSERT_OK(reference.Set(10000, Value{0x12}));
  ASSERT_OK(store.Set(10000, Value{0x12}));

  // Hash is computed as if all pages are initialized.
  ASSERT_OK_AND_ASSIGN(auto ref_hash, reference.GetHash());
  ASSERT_OK_AND_ASSIGN(auto trg_hash, store.GetHash());
  EXPECT_NE(Hash{}, trg_hash);
  EXPECT_EQ(ref_hash, trg_hash);
}

TYPED_TEST_P(StoreTest, CanProduceMemoryFootprint) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();
  auto summary = store.GetMemoryFootprint();
  EXPECT_GT(summary.GetTotal(), Memory(0));
}

// TODO: remove this and all its calls once all stores support snapshots.
template <typename Store>
bool SupportsSnapshots(const Store& store) {
  auto s = store.CreateSnapshot();
  return s.ok() || s.status().code() != absl::StatusCode::kUnimplemented;
}

TYPED_TEST_P(StoreTest, SnapshotHasSameProofAsStore) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(store)) {
    GTEST_SKIP();
  }

  ASSERT_OK_AND_ASSIGN(auto snapshot1, store.CreateSnapshot());
  EXPECT_THAT(store.GetProof(), snapshot1.GetProof());

  EXPECT_OK(store.Set(10, Value{1, 2, 3}));
  EXPECT_THAT(store.GetProof(), Not(snapshot1.GetProof()));

  ASSERT_OK_AND_ASSIGN(auto snapshot2, store.CreateSnapshot());
  EXPECT_THAT(store.GetProof(), snapshot2.GetProof());
}

TYPED_TEST_P(StoreTest, SnapshotShieldsMutations) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(store)) {
    GTEST_SKIP();
  }

  EXPECT_OK(store.Set(10, Value{1}));
  EXPECT_OK(store.Set(12, Value{2}));
  ASSERT_OK_AND_ASSIGN(auto snapshot, store.CreateSnapshot());

  EXPECT_OK(store.Set(14, Value{3}));

  ASSERT_OK_AND_ASSIGN(auto wrapper2, TypeParam::Create());
  auto& restored = wrapper2.GetStore();
  EXPECT_OK(restored.SyncTo(snapshot));
  EXPECT_THAT(restored.Get(10), IsOkAndHolds(Value{1}));
  EXPECT_THAT(restored.Get(12), IsOkAndHolds(Value{2}));
  EXPECT_THAT(restored.Get(14), IsOkAndHolds(Value{}));
}

TYPED_TEST_P(StoreTest, SnapshotRecoveryHasSameHash) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(store)) {
    GTEST_SKIP();
  }

  ASSERT_OK(store.Set(10, Value{0xAB}));
  ASSERT_OK_AND_ASSIGN(auto hash, store.GetHash());
  ASSERT_OK_AND_ASSIGN(auto snapshot, store.CreateSnapshot());

  ASSERT_OK_AND_ASSIGN(auto wrapper2, TypeParam::Create());
  auto& restored = wrapper2.GetStore();
  EXPECT_OK(restored.SyncTo(snapshot));
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
}

TYPED_TEST_P(StoreTest, LargeSnapshotRecoveryWorks) {
  constexpr const int kNumElements = 100000;

  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(store)) {
    GTEST_SKIP();
  }

  auto toValue = [](int i) {
    return Value{std::uint8_t(i), std::uint8_t(i >> 8), std::uint8_t(i >> 16),
                 std::uint8_t(i >> 24)};
  };

  for (int i = 0; i < kNumElements; i++) {
    EXPECT_OK(store.Set(i, toValue(i)));
  }
  ASSERT_OK_AND_ASSIGN(auto hash, store.GetHash());
  ASSERT_OK_AND_ASSIGN(auto snapshot, store.CreateSnapshot());
  EXPECT_LT(50, snapshot.GetSize());

  ASSERT_OK_AND_ASSIGN(auto wrapper2, TypeParam::Create());
  auto& restored = wrapper2.GetStore();
  EXPECT_OK(restored.SyncTo(snapshot));
  for (int i = 0; i < kNumElements; i++) {
    EXPECT_THAT(restored.Get(i), IsOkAndHolds(toValue(i)));
  }
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
}

TYPED_TEST_P(StoreTest, LargeSnapshotSerializationAndRecoveryWorks) {
  constexpr const int kNumElements = 100000;

  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(store)) {
    GTEST_SKIP();
  }

  auto toValue = [](int i) {
    return Value{std::uint8_t(i), std::uint8_t(i >> 8), std::uint8_t(i >> 16),
                 std::uint8_t(i >> 24)};
  };

  for (int i = 0; i < kNumElements; i++) {
    EXPECT_OK(store.Set(i, toValue(i)));
  }
  ASSERT_OK_AND_ASSIGN(auto hash, store.GetHash());
  ASSERT_OK_AND_ASSIGN(auto snapshot, store.CreateSnapshot());
  EXPECT_LT(50, snapshot.GetSize());

  // Create a second snapshot, based on a raw data source, provided by the first
  // snapshot.
  ASSERT_OK_AND_ASSIGN(
      auto remote, StoreSnapshot<Value>::FromSource(snapshot.GetDataSource()));

  ASSERT_OK_AND_ASSIGN(auto wrapper2, TypeParam::Create());
  auto& restored = wrapper2.GetStore();
  EXPECT_OK(restored.SyncTo(remote));
  for (int i = 0; i < kNumElements; i++) {
    EXPECT_THAT(restored.Get(i), IsOkAndHolds(toValue(i)));
  }
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
}

TYPED_TEST_P(StoreTest, SnycCanShrinkStoreSize) {
  constexpr const int kNumElements = 100000;

  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(store)) {
    GTEST_SKIP();
  }

  EXPECT_OK(store.Set(10, Value{12}));
  ASSERT_OK_AND_ASSIGN(auto snapshot, store.CreateSnapshot());
  ASSERT_OK_AND_ASSIGN(auto hash_of_small_store, store.GetHash());

  // Fill the restore target with data.
  ASSERT_OK_AND_ASSIGN(auto wrapper2, TypeParam::Create());
  auto& restored = wrapper2.GetStore();
  for (int i = 0; i < kNumElements; i++) {
    EXPECT_OK(restored.Set(i, Value{14}));
  }
  ASSERT_OK_AND_ASSIGN(auto hash_of_large_store, restored.GetHash());
  EXPECT_NE(hash_of_small_store, hash_of_large_store);

  // Sync to smaller store, should remove extra data.
  EXPECT_OK(restored.SyncTo(snapshot));
  EXPECT_THAT(restored.Get(10), IsOkAndHolds(Value{12}));
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash_of_small_store));
}

TYPED_TEST_P(StoreTest, SnapshotsCanBeVerified) {
  constexpr const int kNumElements = 100000;

  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(store)) {
    GTEST_SKIP();
  }

  for (int i = 0; i < kNumElements; i++) {
    EXPECT_OK(store.Set(i, Value{std::uint8_t(i)}));
  }
  ASSERT_OK_AND_ASSIGN(auto snapshot, store.CreateSnapshot());
  EXPECT_LT(50, snapshot.GetSize());

  // This step verifies that the proofs are consistent.
  EXPECT_THAT(store.GetHash(), IsOkAndHolds(snapshot.GetProof().hash));
  EXPECT_OK(snapshot.VerifyProofs());

  // Verify that the content of parts is consistent with the proofs.
  for (std::size_t i = 0; i < snapshot.GetSize(); i++) {
    ASSERT_OK_AND_ASSIGN(auto proof, snapshot.GetProof(i));
    ASSERT_OK_AND_ASSIGN(auto part, snapshot.GetPart(i));
    EXPECT_THAT(part.GetProof(), proof);
    EXPECT_TRUE(part.Verify());
  }
}

TYPED_TEST_P(StoreTest, SnapshotsCanBeSerializedAndVerified) {
  constexpr const int kNumElements = 100000;

  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(store)) {
    GTEST_SKIP();
  }

  for (int i = 0; i < kNumElements; i++) {
    EXPECT_OK(store.Set(i, Value{std::uint8_t(i)}));
  }
  ASSERT_OK_AND_ASSIGN(auto snapshot, store.CreateSnapshot());
  EXPECT_LT(50, snapshot.GetSize());

  // Create a second snapshot, based on a raw data source, provided by the first
  // snapshot.
  ASSERT_OK_AND_ASSIGN(
      auto remote, StoreSnapshot<Value>::FromSource(snapshot.GetDataSource()));

  // This step verifies that the proofs are consistent.
  EXPECT_THAT(store.GetHash(), IsOkAndHolds(remote.GetProof().hash));
  EXPECT_OK(remote.VerifyProofs());

  // Verify that the content of parts is consistent with the proofs.
  for (std::size_t i = 0; i < remote.GetSize(); i++) {
    ASSERT_OK_AND_ASSIGN(auto proof, remote.GetProof(i));
    ASSERT_OK_AND_ASSIGN(auto part, remote.GetPart(i));
    EXPECT_THAT(part.GetProof(), proof);
    EXPECT_TRUE(part.Verify());
  }
}

TYPED_TEST_P(StoreTest, AnEmptySnapshotCanBeVerified) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& store = wrapper.GetStore();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(store)) {
    GTEST_SKIP();
  }

  ASSERT_OK_AND_ASSIGN(auto snapshot, store.CreateSnapshot());
  EXPECT_EQ(0, snapshot.GetSize());
  EXPECT_OK(snapshot.VerifyProofs());
}

REGISTER_TYPED_TEST_SUITE_P(
    StoreTest, TypeProperties, UninitializedValuesAreZero,
    DataCanBeAddedAndRetrieved, EntriesCanBeUpdated, EmptyStoreHasZeroHash,
    KnownHashesAreReproduced, HashComputationIgnoresUnusedBytes,
    HashesRespectBranchingFactor, HashesEqualReferenceImplementation,
    HashesRespectEmptyPages, HashesChangeWithUpdates,
    HashesDoNotChangeWithReads, HashesCoverMultiplePages,
    CanProduceMemoryFootprint, SnapshotHasSameProofAsStore,
    SnapshotShieldsMutations, SnapshotRecoveryHasSameHash,
    LargeSnapshotRecoveryWorks, LargeSnapshotSerializationAndRecoveryWorks,
    SnycCanShrinkStoreSize, SnapshotsCanBeVerified,
    SnapshotsCanBeSerializedAndVerified, AnEmptySnapshotCanBeVerified);

}  // namespace
}  // namespace carmen::backend::store

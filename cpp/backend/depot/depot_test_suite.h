#pragma once

#include "backend/depot/depot.h"
#include "backend/depot/depot_handler.h"
#include "common/status_test_util.h"
#include "common/test_util.h"
#include "common/type.h"
#include "gmock/gmock-matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::depot {
namespace {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::IsEmpty;
using ::testing::IsOkAndHolds;
using ::testing::Not;
using ::testing::StatusIs;
using ::testing::StrEq;

// A test configuration for depot implementations.
template <Depot depot, std::size_t branching_factor, std::size_t hash_box_size>
using DepotTestConfig = DepotHandler<depot, branching_factor, hash_box_size>;

// A test suite testing generic depot implementations.
template <typename>
class DepotTest : public testing::Test {};

TYPED_TEST_SUITE_P(DepotTest);

TYPED_TEST_P(DepotTest, TypeProperties) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();
  EXPECT_TRUE(std::is_move_constructible_v<decltype(depot)>);
}

TYPED_TEST_P(DepotTest, DataCanBeAddedAndRetrieved) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();

  EXPECT_THAT(depot.Get(10), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_THAT(depot.Get(100), StatusIs(absl::StatusCode::kNotFound, _));

  EXPECT_OK(depot.Set(10, std::array{std::byte{1}, std::byte{2}}));
  EXPECT_THAT(depot.Get(10),
              IsOkAndHolds(ElementsAre(std::byte{1}, std::byte{2})));

  EXPECT_OK(
      depot.Set(100, std::array{std::byte{1}, std::byte{2}, std::byte{3}}));
  EXPECT_THAT(depot.Get(100), IsOkAndHolds(ElementsAre(
                                  std::byte{1}, std::byte{2}, std::byte{3})));
}

TYPED_TEST_P(DepotTest, EntriesCanBeUpdated) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();

  EXPECT_OK(depot.Set(10, std::array{std::byte{1}, std::byte{2}}));
  EXPECT_THAT(depot.Get(10),
              IsOkAndHolds(ElementsAre(std::byte{1}, std::byte{2})));

  EXPECT_OK(
      depot.Set(10, std::array{std::byte{1}, std::byte{2}, std::byte{3}}));
  EXPECT_THAT(depot.Get(10), IsOkAndHolds(ElementsAre(
                                 std::byte{1}, std::byte{2}, std::byte{3})));
}

TYPED_TEST_P(DepotTest, SizeCanBeFatched) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();

  EXPECT_THAT(depot.GetSize(10), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_OK(depot.Set(10, std::array{std::byte{1}, std::byte{2}}));
  ASSERT_OK_AND_ASSIGN(auto size, depot.GetSize(10));
  EXPECT_EQ(size, std::uint32_t{2});
}

TYPED_TEST_P(DepotTest, EmptyDepotHasZeroHash) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();
  EXPECT_THAT(depot.GetHash(), IsOkAndHolds(Hash{}));
}

TYPED_TEST_P(DepotTest, NonEmptyDepotHasHash) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();

  ASSERT_OK_AND_ASSIGN(auto initial_hash, depot.GetHash());
  EXPECT_OK(depot.Set(10, std::array{std::byte{1}, std::byte{2}}));
  ASSERT_OK_AND_ASSIGN(auto new_hash, depot.GetHash());
  ASSERT_NE(initial_hash, new_hash);
}

TYPED_TEST_P(DepotTest, HashChangesBack) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();

  EXPECT_OK(depot.Set(10, std::array{std::byte{1}, std::byte{2}}));
  EXPECT_OK(
      depot.Set(100, std::array{std::byte{1}, std::byte{2}, std::byte{3}}));
  ASSERT_OK_AND_ASSIGN(auto initial_hash, depot.GetHash());

  EXPECT_OK(
      depot.Set(10, std::array{std::byte{1}, std::byte{2}, std::byte{3}}));
  ASSERT_OK_AND_ASSIGN(auto new_hash, depot.GetHash());

  ASSERT_NE(initial_hash, new_hash);

  EXPECT_OK(depot.Set(10, std::array{std::byte{1}, std::byte{2}}));
  EXPECT_THAT(depot.GetHash(), IsOkAndHolds(initial_hash));
}

TYPED_TEST_P(DepotTest, KnownHashesAreReproduced) {
  if (TypeParam::kBranchingFactor != 3 || TypeParam::kHashBoxSize != 2) {
    GTEST_SKIP()
        << "This test is only valid for branching factor 3 and hash box size "
           "of 2.";
  }

  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();

  // Tests the hashes for values [0x00], [0x00, 0x11] ... [..., 0xFF] inserted
  // in sequence.
  std::vector<std::string> hashes{
      "0xa536aa3cede6ea3c1f3e0357c3c60e0f216a8c89b853df13b29daa8f85065dfb",
      "0xab03063682ff571fbdf1f26e310a09911a9eefb57014b24679c3b0c806a17f86",
      "0x6a3c781abaa02fe7f794e098db664d0261088dc3ae481ab5451e8b130e6a6eaf",
      "0x02f47ff7c23929f1ab915a06d1e7b64f7cc77924b33a0fa202f3aee9a94cc1d7",
      "0x516c2b341e44c4da030c3c285cf4600fa52d9466da8fdfb159654d8190ad704d",
      "0x493529675023185851f83ca17720e130721a84141292a145e7f7c24b7d50c713",
      "0xaa541f8619d33f6310ae0ef2ccd4f695a97daaf65e0530c8fc6fdb700cb3d05e",
      "0x91e7877b25a43d450ee1a41d1d63e3511b21dee519d503f95a150950bfb3c332",
      "0x1dc2edcabc1a59b9907acfc1679c0755db022df0abc73231186f4cd14004fa60",
      "0x9b5ddc81a683b80222ad5da9ad8455cd4652319deed5f3da19b27e4ca51a6027",
      "0x6bebc3e34057d536d3413e2e0e50dd70fa2367f0a66edbc5bcdf56799ce82abf",
      "0xcc686ef8a6e09a4f337ceb561295a47ce06040536bba221d3d6f3f5930b57424",
      "0x9c1650d324210e418bbd2963b0197e7dd9cf320af44f14447813f8ebee7fae96",
      "0xc6fdda270af771daa8516cc118eef1df7a265bccf10c2c3e705838bdcf2180e6",
      "0xc00a9e2dec151f7c40d5b029c7ea6a3f672fdf389ef6e2db196e20ef7d367ad5",
      "0x87875b163817fec8174795cb8a61a575b9c0e6e76ce573c5440f97b4a0742b1f",
  };

  int i = 0;
  std::vector<std::byte> data;
  for (const auto& expected_hash : hashes) {
    data.push_back(static_cast<std::byte>(i << 4 | i));
    EXPECT_OK(depot.Set(i, data));
    ASSERT_OK_AND_ASSIGN(auto actual_hash, depot.GetHash());
    EXPECT_THAT(Print(actual_hash), StrEq(expected_hash));
    i++;
  }
}

TYPED_TEST_P(DepotTest, EmptyCodeCanBeStored) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();
  EXPECT_OK(depot.Set(10, std::span<std::byte>{}));
  EXPECT_THAT(depot.Get(10), IsOkAndHolds(IsEmpty()));
}

TYPED_TEST_P(DepotTest, HashesEqualReferenceImplementation) {
  constexpr int N = 100;
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();
  auto& reference = wrapper.GetReferenceDepot();

  EXPECT_THAT(depot.GetHash(), IsOkAndHolds(Hash{}));

  // assert empty value
  ASSERT_OK(depot.Set(0, std::span<std::byte>{}));
  ASSERT_OK(reference.Set(0, std::span<std::byte>{}));
  ASSERT_OK_AND_ASSIGN(auto hash, depot.GetHash());
  EXPECT_THAT(reference.GetHash(), IsOkAndHolds(hash));

  std::array<std::byte, 4> value{};
  for (int i = 0; i < N; i++) {
    value = {static_cast<std::byte>(i >> 6 & 0x3),
             static_cast<std::byte>(i >> 4 & 0x3),
             static_cast<std::byte>(i >> 2 & 0x3),
             static_cast<std::byte>(i >> 0 & 0x3)};
    ASSERT_OK(depot.Set(i, value));
    ASSERT_OK(reference.Set(i, value));
    ASSERT_OK_AND_ASSIGN(hash, depot.GetHash());
    EXPECT_THAT(reference.GetHash(), IsOkAndHolds(hash));
  }
}

// TODO: remove this and all its calls once all depots support snapshots.
template <typename Depot>
bool SupportsSnapshots(const Depot& depot) {
  auto s = depot.CreateSnapshot();
  return s.ok() || s.status().code() != absl::StatusCode::kUnimplemented;
}

TYPED_TEST_P(DepotTest, SnapshotHasSameProofAsStore) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(depot)) {
    GTEST_SKIP();
  }

  ASSERT_OK_AND_ASSIGN(auto snapshot1, depot.CreateSnapshot());
  EXPECT_THAT(depot.GetProof(), snapshot1.GetProof());

  EXPECT_OK(depot.Set(10, Code{1, 2, 3}));
  EXPECT_THAT(depot.GetProof(), Not(snapshot1.GetProof()));

  ASSERT_OK_AND_ASSIGN(auto snapshot2, depot.CreateSnapshot());
  EXPECT_THAT(depot.GetProof(), snapshot2.GetProof());
}

TYPED_TEST_P(DepotTest, SnapshotShieldsMutations) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(depot)) {
    GTEST_SKIP();
  }

  EXPECT_OK(depot.Set(10, Code{1, 2, 3}));
  EXPECT_OK(depot.Set(12, Code{2, 4}));
  ASSERT_OK_AND_ASSIGN(auto snapshot, depot.CreateSnapshot());
  EXPECT_OK(depot.Set(14, Code{2, 6}));

  ASSERT_OK_AND_ASSIGN(auto wrapper2, TypeParam::Create());
  auto& restored = wrapper2.GetDepot();
  EXPECT_OK(restored.SyncTo(snapshot));
  EXPECT_THAT(restored.Get(10), IsOkAndHolds(ElementsAre(
                                    std::byte{1}, std::byte{2}, std::byte{3})));
  EXPECT_THAT(restored.Get(12),
              IsOkAndHolds(ElementsAre(std::byte{2}, std::byte{4})));
  EXPECT_THAT(restored.Get(14), StatusIs(absl::StatusCode::kNotFound, _));
}

TYPED_TEST_P(DepotTest, SnapshotRecoveryHasSameHash) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(depot)) {
    GTEST_SKIP();
  }

  ASSERT_OK(depot.Set(10, Code{1, 2, 3}));
  ASSERT_OK_AND_ASSIGN(auto hash, depot.GetHash());
  ASSERT_OK_AND_ASSIGN(auto snapshot, depot.CreateSnapshot());

  ASSERT_OK_AND_ASSIGN(auto wrapper2, TypeParam::Create());
  auto& restored = wrapper2.GetDepot();
  EXPECT_OK(restored.SyncTo(snapshot));
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
}

TYPED_TEST_P(DepotTest, LargeSnapshotRecoveryWorks) {
  constexpr const int kNumElements = 100000;

  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(depot)) {
    GTEST_SKIP();
  }

  for (int i = 0; i < kNumElements; i++) {
    if (i % 2) {
      EXPECT_OK(depot.Set(i, Code{std::uint8_t(i)}));
    } else {
      EXPECT_OK(depot.Set(i, Code{std::uint8_t(i >> 8), std::uint8_t(i)}));
    }
  }
  ASSERT_OK_AND_ASSIGN(auto hash, depot.GetHash());
  ASSERT_OK_AND_ASSIGN(auto snapshot, depot.CreateSnapshot());
  EXPECT_LT(50, snapshot.GetSize());

  ASSERT_OK_AND_ASSIGN(auto wrapper2, TypeParam::Create());
  auto& restored = wrapper2.GetDepot();
  EXPECT_OK(restored.SyncTo(snapshot));
  for (int i = 0; i < kNumElements; i++) {
    if (i % 2) {
      EXPECT_THAT(restored.Get(i), IsOkAndHolds(ElementsAre(std::byte(i))));
    } else {
      EXPECT_THAT(restored.Get(i),
                  IsOkAndHolds(ElementsAre(std::byte(i >> 8), std::byte(i))));
    }
  }
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
}

TYPED_TEST_P(DepotTest, LargeSnapshotSerializationAndRecoveryWorks) {
  constexpr const int kNumElements = 100000;

  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(depot)) {
    GTEST_SKIP();
  }

  for (int i = 0; i < kNumElements; i++) {
    if (i % 2) {
      EXPECT_OK(depot.Set(i, Code{std::uint8_t(i)}));
    } else {
      EXPECT_OK(depot.Set(i, Code{std::uint8_t(i >> 8), std::uint8_t(i)}));
    }
  }
  ASSERT_OK_AND_ASSIGN(auto hash, depot.GetHash());
  ASSERT_OK_AND_ASSIGN(auto snapshot, depot.CreateSnapshot());
  EXPECT_LT(50, snapshot.GetSize());

  // Create a second snapshot, based on a raw data source, provided by the first
  // snapshot.
  ASSERT_OK_AND_ASSIGN(auto remote,
                       DepotSnapshot::FromSource(snapshot.GetDataSource()));

  ASSERT_OK_AND_ASSIGN(auto wrapper2, TypeParam::Create());
  auto& restored = wrapper2.GetDepot();
  EXPECT_OK(restored.SyncTo(remote));
  for (int i = 0; i < kNumElements; i++) {
    if (i % 2) {
      EXPECT_THAT(restored.Get(i), IsOkAndHolds(ElementsAre(std::byte(i))));
    } else {
      EXPECT_THAT(restored.Get(i),
                  IsOkAndHolds(ElementsAre(std::byte(i >> 8), std::byte(i))));
    }
  }
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
}

TYPED_TEST_P(DepotTest, SnycCanShrinkStoreSize) {
  constexpr const int kNumElements = 100000;

  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(depot)) {
    GTEST_SKIP();
  }

  EXPECT_OK(depot.Set(10, Code{1, 2, 3}));
  ASSERT_OK_AND_ASSIGN(auto snapshot, depot.CreateSnapshot());
  ASSERT_OK_AND_ASSIGN(auto hash_of_small_depot, depot.GetHash());

  // Fill the restore target with data.
  ASSERT_OK_AND_ASSIGN(auto wrapper2, TypeParam::Create());
  auto& restored = wrapper2.GetDepot();
  for (int i = 0; i < kNumElements; i++) {
    EXPECT_OK(restored.Set(i, Code{2, 3, 4}));
  }
  ASSERT_OK_AND_ASSIGN(auto hash_of_large_depot, restored.GetHash());
  EXPECT_NE(hash_of_small_depot, hash_of_large_depot);

  // Sync to smaller depot, should remove extra data.
  EXPECT_OK(restored.SyncTo(snapshot));
  EXPECT_THAT(restored.Get(0), IsOkAndHolds(ElementsAre()));
  EXPECT_THAT(restored.Get(1), IsOkAndHolds(ElementsAre()));
  EXPECT_THAT(restored.Get(9), IsOkAndHolds(ElementsAre()));
  EXPECT_THAT(restored.Get(10), IsOkAndHolds(ElementsAre(
                                    std::byte{1}, std::byte{2}, std::byte{3})));
  EXPECT_THAT(restored.Get(11), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash_of_small_depot));
}

TYPED_TEST_P(DepotTest, SnapshotsCanBeVerified) {
  constexpr const int kNumElements = 100000;

  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(depot)) {
    GTEST_SKIP();
  }

  for (int i = 0; i < kNumElements; i++) {
    EXPECT_OK(depot.Set(i, Value{std::uint8_t(i)}));
  }
  ASSERT_OK_AND_ASSIGN(auto snapshot, depot.CreateSnapshot());
  EXPECT_LT(50, snapshot.GetSize());

  // This step verifies that the proofs are consistent.
  EXPECT_THAT(depot.GetHash(), IsOkAndHolds(snapshot.GetProof().hash));
  EXPECT_OK(snapshot.VerifyProofs());

  // Verify that the content of parts is consistent with the proofs.
  for (std::size_t i = 0; i < snapshot.GetSize(); i++) {
    ASSERT_OK_AND_ASSIGN(auto proof, snapshot.GetProof(i));
    ASSERT_OK_AND_ASSIGN(auto part, snapshot.GetPart(i));
    EXPECT_THAT(part.GetProof(), proof);
    EXPECT_TRUE(part.Verify());
  }
}

TYPED_TEST_P(DepotTest, SnapshotsCanBeSerializedAndVerified) {
  constexpr const int kNumElements = 100000;

  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(depot)) {
    GTEST_SKIP();
  }

  for (int i = 0; i < kNumElements; i++) {
    EXPECT_OK(depot.Set(i, Value{std::uint8_t(i)}));
  }
  ASSERT_OK_AND_ASSIGN(auto snapshot, depot.CreateSnapshot());
  EXPECT_LT(50, snapshot.GetSize());

  // Create a second snapshot, based on a raw data source, provided by the first
  // snapshot.
  ASSERT_OK_AND_ASSIGN(auto remote,
                       DepotSnapshot::FromSource(snapshot.GetDataSource()));

  // This step verifies that the proofs are consistent.
  EXPECT_THAT(depot.GetHash(), IsOkAndHolds(remote.GetProof().hash));
  EXPECT_OK(remote.VerifyProofs());

  // Verify that the content of parts is consistent with the proofs.
  for (std::size_t i = 0; i < remote.GetSize(); i++) {
    ASSERT_OK_AND_ASSIGN(auto proof, remote.GetProof(i));
    ASSERT_OK_AND_ASSIGN(auto part, remote.GetPart(i));
    EXPECT_THAT(part.GetProof(), proof);
    EXPECT_TRUE(part.Verify());
  }
}

TYPED_TEST_P(DepotTest, AnEmptySnapshotCanBeVerified) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, TypeParam::Create());
  auto& depot = wrapper.GetDepot();

  // Skip if snapshots are not implemented.
  if (!SupportsSnapshots(depot)) {
    GTEST_SKIP();
  }

  ASSERT_OK_AND_ASSIGN(auto snapshot, depot.CreateSnapshot());
  EXPECT_EQ(0, snapshot.GetSize());
  EXPECT_THAT(depot.GetHash(), IsOkAndHolds(snapshot.GetProof().hash));
  EXPECT_OK(snapshot.VerifyProofs());
}

REGISTER_TYPED_TEST_SUITE_P(
    DepotTest, TypeProperties, EmptyCodeCanBeStored, DataCanBeAddedAndRetrieved,
    EntriesCanBeUpdated, SizeCanBeFatched, EmptyDepotHasZeroHash,
    NonEmptyDepotHasHash, HashChangesBack, KnownHashesAreReproduced,
    HashesEqualReferenceImplementation, SnapshotHasSameProofAsStore,
    SnapshotShieldsMutations, SnapshotRecoveryHasSameHash,
    LargeSnapshotRecoveryWorks, LargeSnapshotSerializationAndRecoveryWorks,
    SnycCanShrinkStoreSize, SnapshotsCanBeVerified,
    SnapshotsCanBeSerializedAndVerified, AnEmptySnapshotCanBeVerified);

}  // namespace
}  // namespace carmen::backend::depot

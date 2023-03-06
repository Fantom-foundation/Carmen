#include "backend/snapshot_test_utils.h"

#include <algorithm>
#include <cstddef>
#include <span>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/snapshot.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend {
namespace {

TEST(Snapshot, SnapshotCanBeCreated) {
  TestData data("some test data");
  ASSERT_OK_AND_ASSIGN(auto snapshot, data.CreateSnapshot());
  ASSERT_THAT(data.GetData().size(), 14);
  EXPECT_THAT(snapshot.GetSize(), 14 / 4 + 1);
}

TEST(Snapshot, ProofOfDataEqualsProofOfSnapshot) {
  TestData data("some test data");
  ASSERT_OK_AND_ASSIGN(auto data_proof, data.GetProof());

  ASSERT_OK_AND_ASSIGN(auto snapshot, data.CreateSnapshot());
  auto shot_proof = snapshot.GetProof();

  EXPECT_EQ(data_proof, shot_proof);
}

TEST(Snapshot, ChangingTheDataDoesNotChangeTheSnapshotProof) {
  TestData data("some test data");
  ASSERT_OK_AND_ASSIGN(auto old_data_proof, data.GetProof());

  ASSERT_OK_AND_ASSIGN(auto snapshot, data.CreateSnapshot());
  auto old_shot_proof = snapshot.GetProof();

  data = "some other content";

  // The proof of the data has changed.
  ASSERT_OK_AND_ASSIGN(auto new_data_proof, data.GetProof());
  EXPECT_NE(old_data_proof, new_data_proof);

  // The proof of the snapshot has not changed.
  auto new_shot_proof = snapshot.GetProof();
  EXPECT_EQ(old_shot_proof, new_shot_proof);
}

TEST(Snapshot, SnapshotCanBeRestored) {
  TestData data("some test data");
  ASSERT_OK_AND_ASSIGN(auto data_proof, data.GetProof());
  ASSERT_OK_AND_ASSIGN(auto snapshot, data.CreateSnapshot());
  data = "some other content";

  ASSERT_OK_AND_ASSIGN(auto restored, TestData::Restore(snapshot));
  EXPECT_EQ(restored.GetData(), "some test data");
  EXPECT_THAT(restored.GetProof(), data_proof);
}

TEST(Snapshot, PartProofsCanBeVerified) {
  TestData data("some test data");
  ASSERT_OK_AND_ASSIGN(auto snapshot, data.CreateSnapshot());
  ASSERT_LT(1, snapshot.GetSize());
  for (std::size_t i = 0; i < snapshot.GetSize(); i++) {
    ASSERT_OK_AND_ASSIGN(auto part, snapshot.GetPart(i));
    EXPECT_THAT(snapshot.GetProof(i), part.GetProof());
  }
}

TEST(Snapshot, SnapshotProofsCanBeVerified) {
  TestData data("some test data");
  ASSERT_OK_AND_ASSIGN(auto snapshot, data.CreateSnapshot());
  ASSERT_LT(1, snapshot.GetSize());
  EXPECT_OK(snapshot.VerifyProofs());
}

}  // namespace
}  // namespace carmen::backend

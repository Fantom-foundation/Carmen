#include "backend/depot/snapshot.h"

#include "backend/snapshot.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "gtest/gtest.h"

namespace carmen::backend::depot {
namespace {

using ::testing::ElementsAre;

TEST(DepotProof, IsProof) { static_assert(Proof<DepotProof>); }

TEST(DepotPart, IsPart) { static_assert(Part<DepotPart>); }

TEST(DepotPart, CanBeSerializedAndDeserialized) {
  std::vector<std::byte> data;
  {
    DepotProof proof(Hash{1, 2, 3});
    std::vector<std::byte> payload = {std::byte(4), std::byte(5)};
    DepotPart part(proof, payload);
    data = part.ToBytes();
  }
  {
    ASSERT_OK_AND_ASSIGN(auto restored, DepotPart::FromBytes(data));
    EXPECT_THAT(restored.GetProof(), DepotProof(Hash{1, 2, 3}));
    EXPECT_THAT(restored.GetData(), ElementsAre(std::byte(4), std::byte(5)));
  }
}

TEST(DepotPart, VerificationPassesOnCorrectProof) {
  std::vector<std::byte> payload = {std::byte(4), std::byte(5)};
  Hash hash = GetSha256Hash(std::span(payload));
  DepotPart part(DepotProof{hash}, payload);
  EXPECT_EQ(part.GetProof(), DepotProof(hash));
  EXPECT_TRUE(part.Verify());
}

TEST(DepotPart, InvalidProofIsDetected) {
  std::vector<std::byte> payload = {std::byte(4), std::byte(5)};
  DepotPart part({}, payload);
  EXPECT_FALSE(part.Verify());
}

TEST(DepotSnapshotTest, IsSnapshot) { static_assert(Snapshot<DepotSnapshot>); }

}  // namespace
}  // namespace carmen::backend::depot

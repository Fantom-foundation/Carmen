#include "backend/index/snapshot.h"

#include "backend/snapshot.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::ElementsAre;

TEST(IndexProof, IsProof) { static_assert(Proof<IndexProof>); }

TEST(IndexPart, IsPart) {
  static_assert(Part<IndexPart<Address>>);
  static_assert(Part<IndexPart<Key>>);
}

TEST(IndexPart, CanBeSerializedAndDeserialized) {
  std::vector<std::byte> data;
  {
    IndexProof proof(Hash{1, 2, 3}, Hash{3, 2, 1});
    std::vector<int> keys = {1, 2, 3, 4};
    IndexPart<int> part(proof, keys);
    data = part.ToBytes();
  }
  {
    ASSERT_OK_AND_ASSIGN(auto restored, IndexPart<int>::FromBytes(data));
    EXPECT_THAT(restored.GetProof(), IndexProof(Hash{1, 2, 3}, Hash{3, 2, 1}));
    EXPECT_THAT(restored.GetKeys(), ElementsAre(1, 2, 3, 4));
  }
}

TEST(IndexPart, VerificationPassesOnCorrectProof) {
  std::vector<int> keys{1, 2, 3, 4, 5};
  for (std::uint8_t start_hash = 0; start_hash < 10; start_hash++) {
    // The start hash may be arbitrary.
    Hash hash{start_hash};
    auto begin = hash;
    for (int key : keys) {
      hash = GetSha256Hash(hash, key);
    }
    auto end = hash;
    IndexPart part({begin, end}, keys);
    EXPECT_EQ(part.GetProof(), IndexProof(begin, end));
    // The hash can be verified.
    EXPECT_TRUE(part.Verify());
  }
}

TEST(IndexPart, InvalidProofIsDetected) {
  std::vector<int> keys{1, 2, 3, 4, 5};
  IndexPart part({}, keys);
  EXPECT_FALSE(part.Verify());
}

TEST(IndexSnapshotTest, IsSnapshot) {
  static_assert(Snapshot<IndexSnapshot<Address>>);
  static_assert(Snapshot<IndexSnapshot<Key>>);
}

}  // namespace
}  // namespace carmen::backend::index

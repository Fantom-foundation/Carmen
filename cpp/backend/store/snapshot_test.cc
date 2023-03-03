#include "backend/store/snapshot.h"

#include "backend/snapshot.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using ::testing::ElementsAre;

TEST(StoreProof, IsProof) { static_assert(Proof<StoreProof>); }

TEST(StorePart, IsPart) {
  static_assert(Part<StorePart<Address>>);
  static_assert(Part<StorePart<Key>>);
}

TEST(StorePart, CanBeSerializedAndDeserialized) {
  std::vector<std::byte> data;
  {
    StoreProof proof(Hash{1, 2, 3});
    std::vector<int> keys = {4, 5, 6, 7};
    StorePart<int> part(proof, keys);
    data = part.ToBytes();
  }
  {
    ASSERT_OK_AND_ASSIGN(auto restored, StorePart<int>::FromBytes(data));
    EXPECT_THAT(restored.GetProof(), StoreProof(Hash{1, 2, 3}));
    EXPECT_THAT(restored.GetValues(), ElementsAre(4, 5, 6, 7));
  }
}

TEST(StorePart, VerificationPassesOnCorrectProof) {
  std::vector<int> values{1, 2, 3, 4, 5};
  Hash hash = GetSha256Hash(std::as_bytes(std::span(values)));
  StorePart part(StoreProof{hash}, values);
  EXPECT_EQ(part.GetProof(), StoreProof(hash));
  EXPECT_TRUE(part.Verify());
}

TEST(StorePart, InvalidProofIsDetected) {
  std::vector<int> keys{1, 2, 3, 4, 5};
  StorePart part({}, keys);
  EXPECT_FALSE(part.Verify());
}

TEST(StoreSnapshotTest, IsSnapshot) {
  static_assert(Snapshot<StoreSnapshot<Address>>);
  static_assert(Snapshot<StoreSnapshot<Key>>);
}

}  // namespace
}  // namespace carmen::backend::store

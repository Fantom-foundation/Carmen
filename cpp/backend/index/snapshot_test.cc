#include "backend/index/snapshot.h"

#include "backend/snapshot.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Return;

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

TEST(IndexSnapshot, IsSnapshot) {
  static_assert(Snapshot<IndexSnapshot<Address>>);
  static_assert(Snapshot<IndexSnapshot<Key>>);
}

class MockIndexDataSource : public IndexSnapshotDataSource<int> {
 public:
  MockIndexDataSource(std::size_t num_keys)
      : IndexSnapshotDataSource<int>(num_keys) {}
  MOCK_METHOD(absl::StatusOr<IndexProof>, GetProof, (std::size_t), (const));
  MOCK_METHOD(absl::StatusOr<IndexPart<int>>, GetPart, (std::size_t), (const));
};

TEST(IndexSnapshot, CanBeTransferedThroughDataSource) {
  auto mock_ptr = std::make_unique<MockIndexDataSource>(10000);
  auto& mock = *mock_ptr;
  EXPECT_CALL(mock, GetProof(_)).WillRepeatedly(Return(IndexProof(Hash{0x12})));
  EXPECT_CALL(mock, GetPart(_))
      .WillRepeatedly(Return(
          IndexPart<int>(IndexProof(Hash{0x12}), std::vector<int>{1, 2, 3})));

  // Creates a snapshot based on a local index, mocked above.
  IndexSnapshot<int> origin(Hash{0x12}, std::move(mock_ptr));

  // Create a second snapshot, based on a raw data source, provided by the first
  // snapshot.
  ASSERT_OK_AND_ASSIGN(auto remote,
                       IndexSnapshot<int>::FromSource(origin.GetDataSource()));

  // Check that the remote snapshot has the same data as the origin.
  EXPECT_EQ(origin.GetProof(), remote.GetProof());
  EXPECT_EQ(origin.GetSize(), remote.GetSize());

  ASSERT_OK_AND_ASSIGN(auto origin_proof, origin.GetProof(1));
  ASSERT_OK_AND_ASSIGN(auto remote_proof, remote.GetProof(1));
  EXPECT_EQ(origin_proof, remote_proof);

  ASSERT_OK_AND_ASSIGN(auto origin_part, origin.GetPart(1));
  ASSERT_OK_AND_ASSIGN(auto remote_part, remote.GetPart(1));
  EXPECT_EQ(origin_part.GetProof(), remote_part.GetProof());
  EXPECT_EQ(origin_part.GetKeys(), remote_part.GetKeys());
}

}  // namespace
}  // namespace carmen::backend::index

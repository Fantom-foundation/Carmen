#include "backend/store/snapshot.h"

#include "backend/snapshot.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Return;

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

TEST(StoreSnapshot, IsSnapshot) {
  static_assert(Snapshot<StoreSnapshot<Address>>);
  static_assert(Snapshot<StoreSnapshot<Key>>);
}

class MockStoreDataSource : public StoreSnapshotDataSource<int> {
 public:
  MockStoreDataSource(std::size_t num_pages)
      : StoreSnapshotDataSource<int>(num_pages) {}
  MOCK_METHOD(absl::StatusOr<StoreProof>, GetProof, (std::size_t), (const));
  MOCK_METHOD(absl::StatusOr<StorePart<int>>, GetPart, (std::size_t), (const));
};

TEST(StoreSnapshot, CanBeTransferedThroughDataSource) {
  auto mock_ptr = std::make_unique<MockStoreDataSource>(10000);
  auto& mock = *mock_ptr;
  EXPECT_CALL(mock, GetProof(_)).WillRepeatedly(Return(StoreProof(Hash{0x12})));
  EXPECT_CALL(mock, GetPart(_))
      .WillRepeatedly(Return(
          StorePart<int>(StoreProof(Hash{0x12}), std::vector<int>{1, 2, 3})));

  // Creates a snapshot based on a local index, mocked above.
  StoreSnapshot<int> origin(12, Hash{0x12}, std::move(mock_ptr));

  // Create a second snapshot, based on a raw data source, provided by the first
  // snapshot.
  ASSERT_OK_AND_ASSIGN(auto remote,
                       StoreSnapshot<int>::FromSource(origin.GetDataSource()));

  // Check that the remote snapshot has the same data as the origin.
  EXPECT_EQ(origin.GetProof(), remote.GetProof());
  EXPECT_EQ(origin.GetSize(), remote.GetSize());

  ASSERT_OK_AND_ASSIGN(auto origin_proof, origin.GetProof(1));
  ASSERT_OK_AND_ASSIGN(auto remote_proof, remote.GetProof(1));
  EXPECT_EQ(origin_proof, remote_proof);

  ASSERT_OK_AND_ASSIGN(auto origin_part, origin.GetPart(1));
  ASSERT_OK_AND_ASSIGN(auto remote_part, remote.GetPart(1));
  EXPECT_EQ(origin_part.GetProof(), remote_part.GetProof());
  EXPECT_EQ(origin_part.GetValues(), remote_part.GetValues());
}

}  // namespace
}  // namespace carmen::backend::store

#include "backend/depot/snapshot.h"

#include "backend/snapshot.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "gtest/gtest.h"

namespace carmen::backend::depot {
namespace {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Return;

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

TEST(DepotSnapshot, IsSnapshot) { static_assert(Snapshot<DepotSnapshot>); }

class MockDepotDataSource : public DepotSnapshotDataSource {
 public:
  MockDepotDataSource(std::size_t num_pages)
      : DepotSnapshotDataSource(num_pages) {}
  MOCK_METHOD(absl::StatusOr<DepotProof>, GetProof, (std::size_t), (const));
  MOCK_METHOD(absl::StatusOr<DepotPart>, GetPart, (std::size_t), (const));
};

TEST(DepotSnapshot, CanBeTransferedThroughDataSource) {
  auto mock_ptr = std::make_unique<MockDepotDataSource>(10000);
  auto& mock = *mock_ptr;
  EXPECT_CALL(mock, GetProof(_)).WillRepeatedly(Return(DepotProof(Hash{0x12})));
  EXPECT_CALL(mock, GetPart(_))
      .WillRepeatedly(Return(
          DepotPart(DepotProof(Hash{0x12}),
                    std::vector<std::byte>{std::byte{1}, std::byte{2}})));

  // Creates a snapshot based on a local index, mocked above.
  DepotSnapshot origin(12, Hash{0x12}, std::move(mock_ptr));

  // Create a second snapshot, based on a raw data source, provided by the first
  // snapshot.
  ASSERT_OK_AND_ASSIGN(auto remote,
                       DepotSnapshot::FromSource(origin.GetDataSource()));

  // Check that the remote snapshot has the same data as the origin.
  EXPECT_EQ(origin.GetProof(), remote.GetProof());
  EXPECT_EQ(origin.GetSize(), remote.GetSize());

  ASSERT_OK_AND_ASSIGN(auto origin_proof, origin.GetProof(1));
  ASSERT_OK_AND_ASSIGN(auto remote_proof, remote.GetProof(1));
  EXPECT_EQ(origin_proof, remote_proof);

  ASSERT_OK_AND_ASSIGN(auto origin_part, origin.GetPart(1));
  ASSERT_OK_AND_ASSIGN(auto remote_part, remote.GetPart(1));
  EXPECT_EQ(origin_part.GetProof(), remote_part.GetProof());
  EXPECT_EQ(origin_part.GetData(), remote_part.GetData());
}

}  // namespace
}  // namespace carmen::backend::depot

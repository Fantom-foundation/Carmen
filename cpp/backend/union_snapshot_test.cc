#include "backend/union_snapshot.h"

#include <cstddef>
#include <string_view>

#include "backend/snapshot.h"
#include "backend/snapshot_test_utils.h"
#include "common/status_test_util.h"
#include "common/status_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend {
namespace {

using ::testing::_;
using ::testing::StatusIs;

TEST(UnionProof, IsProof) {
  static_assert(Proof<UnionProof<>>);
  static_assert(Proof<UnionProof<TestProof>>);
  static_assert(Proof<UnionProof<TestProof, TestProof>>);
}

TEST(UnionProof, CanBeSerializedAndDeserialized) {
  UnionProof<TestProof> proof;
  auto data = proof.ToBytes();
  EXPECT_EQ(data[0], std::byte(0));
  ASSERT_OK_AND_ASSIGN(auto restored, UnionProof<TestProof>::FromBytes(data));
  EXPECT_EQ(proof, restored);

  proof = TestProof(Hash{1, 2, 3});
  data = proof.ToBytes();
  EXPECT_EQ(data[0], std::byte(1));
  ASSERT_OK_AND_ASSIGN(restored, UnionProof<TestProof>::FromBytes(data));
  EXPECT_EQ(proof, restored);

  data[0] = std::byte{0xff};
  EXPECT_THAT(UnionProof<TestProof>::FromBytes(data),
              StatusIs(absl::StatusCode::kInvalidArgument, _));
}

TEST(UnionPart, IsPart) {
  // static_assert(Part<UnionPart<TestPart>>);
  // static_assert(Part<UnionPart<TestPart, TestPart>>);
}

TEST(UnionPart, CanBeSerializedAndDeserialized) {
  UnionPart<TestPart> part = TestPart(TestProof(Hash{}), {});
  auto data = part.ToBytes();
  EXPECT_EQ(data[0], std::byte(0));
  ASSERT_OK_AND_ASSIGN(auto restored, UnionPart<TestPart>::FromBytes(data));
  EXPECT_EQ(part.GetProof(), restored.GetProof());

  data[0] = std::byte{0xff};
  EXPECT_THAT(UnionPart<TestPart>::FromBytes(data),
              StatusIs(absl::StatusCode::kInvalidArgument, _));
}

TEST(UnionSnapshot, CanCombineMultipleSnapshots) {
  // static_assert(Snapshot<UnionSnapshot<TestSnapshot>>);
  // static_assert(Snapshot<UnionSnapshot<TestSnapshot, TestSnapshot>>);
}

class TestComposedData {
 public:
  using Snapshot = UnionSnapshot<TestSnapshot, TestSnapshot>;
  using Proof = typename Snapshot::Proof;

  TestComposedData(std::string_view a, std::string_view b)
      : first(a), second(b) {}
  TestComposedData(TestData a, TestData b)
      : first(std::move(a)), second(std::move(b)) {}

  static absl::StatusOr<TestComposedData> Restore(const Snapshot& snapshot) {
    ASSIGN_OR_RETURN(auto first,
                     TestData::Restore(std::get<0>(snapshot.GetSnapshots())));
    ASSIGN_OR_RETURN(auto second,
                     TestData::Restore(std::get<1>(snapshot.GetSnapshots())));
    return TestComposedData(std::move(first), std::move(second));
  }

  absl::StatusOr<Proof> GetProof() const {
    ASSIGN_OR_RETURN(auto first_hash, first.GetProof());
    ASSIGN_OR_RETURN(auto second_hash, second.GetProof());
    return GetSha256Hash(first_hash.ToBytes(), second_hash.ToBytes());
  }

  absl::StatusOr<Snapshot> CreateSnapshot() const {
    return Snapshot::Create(first.CreateSnapshot(), second.CreateSnapshot());
  }

  TestData first;
  TestData second;
};

TEST(TestComposedData, IsSnapshotable) {
  // static_assert(Snapshotable<TestComposedData>);
}

TEST(TestComposedData, CanBeSnapshotted) {
  TestComposedData data("some", "test");
  ASSERT_OK_AND_ASSIGN(auto snapshot, data.CreateSnapshot());
}

TEST(TestComposedData, SnapshotCanBeVerified) {
  TestComposedData data("another", "example");
  ASSERT_OK_AND_ASSIGN(auto snapshot, data.CreateSnapshot());
  EXPECT_OK(snapshot.VerifyProofs());

  auto size = snapshot.GetSize();
  EXPECT_LE(4, size);
  for (std::size_t i = 0; i < size; i++) {
    ASSERT_OK_AND_ASSIGN(auto proof, snapshot.GetProof(i));
    ASSERT_OK_AND_ASSIGN(auto part, snapshot.GetPart(i));
    EXPECT_EQ(proof, part.GetProof());
    EXPECT_TRUE(part.Verify());
  }
}

TEST(TestComposedData, CanRestoreData) {
  TestComposedData data("original", "text");
  ASSERT_OK_AND_ASSIGN(auto snapshot, data.CreateSnapshot());
  EXPECT_OK(snapshot.VerifyProofs());

  ASSERT_OK_AND_ASSIGN(auto restored, TestComposedData::Restore(snapshot));
  EXPECT_EQ(restored.first.GetData(), "original");
  EXPECT_EQ(restored.second.GetData(), "text");
}

TEST(TestComposedData, CanSerializeAndRestoreData) {
  using Snapshot = typename TestComposedData::Snapshot;
  TestComposedData data("original", "text");
  ASSERT_OK_AND_ASSIGN(auto snapshot, data.CreateSnapshot());
  EXPECT_OK(snapshot.VerifyProofs());

  ASSERT_OK_AND_ASSIGN(auto remote,
                       Snapshot::FromSource(snapshot.GetDataSource()));

  ASSERT_OK_AND_ASSIGN(auto restored, TestComposedData::Restore(remote));
  EXPECT_EQ(restored.first.GetData(), "original");
  EXPECT_EQ(restored.second.GetData(), "text");
}

}  // namespace
}  // namespace carmen::backend

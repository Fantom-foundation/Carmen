#include "backend/snapshot.h"

#include <algorithm>
#include <cstddef>
#include <span>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend {
namespace {

// TestProof is an example proof implementation for test cases in this file.
using TestProof = Hash;
static_assert(Proof<TestProof>);

// TestPart is an example part implementation for tests cases in this file.
class TestPart {
 public:
  using Proof = TestProof;

  TestPart(Proof proof, std::span<const std::byte> data)
      : proof_(proof), data_(data.begin(), data.end()) {}

  static absl::StatusOr<TestPart> FromBytes(std::span<const std::byte> data) {
    if (data.size() < sizeof(Proof)) {
      return absl::InvalidArgumentError(
          "Invalid encoding of TestPart, too few bytes");
    }
    Proof proof;
    proof.SetBytes(data.subspan(0, sizeof(Proof)));
    return TestPart(proof, data.subspan(sizeof(Proof)));
  }

  const Proof& GetProof() const { return proof_; }

  bool Verify() const { return GetSha256Hash(data_) == proof_; }

  const std::vector<std::byte>& GetData() const { return data_; }

  std::vector<std::byte> ToBytes() const {
    // Serialized as proof, followed by data.
    std::vector<std::byte> res;
    res.reserve(sizeof(Hash) + data_.size());
    std::span<const std::byte> hash = proof_;
    res.insert(res.end(), hash.begin(), hash.end());
    res.insert(res.end(), data_.begin(), data_.end());
    return res;
  }

 private:
  Proof proof_;
  std::vector<std::byte> data_;
};

static_assert(Part<TestPart>);

// TestSnapshot is an example snapshot implementation demonstrating its concept
// and serving as a example implementation for the tests in this file.
class TestSnapshot {
 public:
  static constexpr std::size_t part_size = 4;
  using Proof = TestProof;
  using Part = TestPart;

  TestSnapshot(Proof proof, std::span<const std::byte> data)
      : proof_(proof), data_(data.begin(), data.end()) {
    // Some extra padding to make the internal snapshot data size a multiple of
    // the data size.
    auto extra = data_.size() % part_size;
    if (extra > 0) {
      data_.resize(data_.size() + (part_size - extra));
    }
    assert(data_.size() >= data.size());
    assert(data_.size() < data.size() + part_size);
    assert(data_.size() % part_size == 0);

    // In a real setup, those part hashes would come from another source.
    for (std::size_t i = 0; i < GetSize(); i++) {
      proofs_.push_back(
          GetSha256Hash(std::span(data_).subspan(i * part_size, part_size)));
    }
  }

  std::size_t GetSize() const {
    return data_.size() / part_size + (data_.size() % part_size > 0);
  }

  absl::StatusOr<Part> GetPart(std::size_t i) const {
    if (i >= GetSize()) {
      return absl::NotFoundError("no such part");
    }
    auto data = std::span(data_).subspan(i * part_size, part_size);
    return Part(GetSha256Hash(data), data);
  }

  Proof GetProof() const { return proof_; }

  absl::StatusOr<Proof> GetProof(std::size_t i) const {
    return i < proofs_.size() ? proofs_[i] : Proof{};
  }

  absl::Status VerifyProofs() const {
    Sha256Hasher hasher;
    for (const auto& proof : proofs_) {
      hasher.Ingest(proof);
    }
    Proof should = hasher.GetHash();
    if (should != proof_) {
      return absl::InternalError("Proofs are not consistent");
    }
    return absl::OkStatus();
  }

 private:
  // The full proof of the snapshot.
  Proof proof_;

  // The proofs of the individual parts.
  std::vector<Proof> proofs_;

  // The raw data of the snapshot, padded with 0s have a size that is a multiple
  // of the part_size.
  std::vector<std::byte> data_;
};

static_assert(Snapshot<TestSnapshot>);

class TestData {
 public:
  using Snapshot = TestSnapshot;

  TestData(std::string_view str) : data_(str) {}

  static absl::StatusOr<TestData> Restore(const Snapshot& snapshot) {
    std::string data;
    for (std::size_t i = 0; i < snapshot.GetSize(); i++) {
      ASSIGN_OR_RETURN(auto part, snapshot.GetPart(i));
      auto substr = part.GetData();
      data.append(reinterpret_cast<const char*>(substr.data()), substr.size());
    }
    // Remove the extra padding that is stored in the snapshot.
    while (data.size() > 0 && data[data.size() - 1] == '\0') {
      data.resize(data.size() - 1);
    }
    TestData res("");
    res.data_ = std::move(data);
    return res;
  }

  absl::StatusOr<TestProof> GetProof() const {
    std::string padded = data_;
    while (padded.size() % 4 != 0) {
      padded += '\0';
    }
    Sha256Hasher global_hasher;
    for (std::size_t i = 0; i < padded.size(); i += 4) {
      Sha256Hasher part_hasher;
      part_hasher.Ingest(std::as_bytes(std::span(padded).subspan(i, 4)));
      global_hasher.Ingest(part_hasher.GetHash());
    }
    return global_hasher.GetHash();
  }

  absl::StatusOr<Snapshot> CreateSnapshot() const {
    ASSIGN_OR_RETURN(auto proof, GetProof());
    return Snapshot(proof, std::as_bytes(std::span(data_)));
  }

  TestData& operator=(std::string_view data) {
    data_ = data;
    return *this;
  }

  std::string& GetData() { return data_; }

 private:
  std::string data_;
};

static_assert(Snapshotable<TestData>);

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

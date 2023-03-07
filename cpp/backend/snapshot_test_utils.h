#pragma once

#include <algorithm>
#include <cstddef>
#include <span>
#include <string>
#include <vector>

#include "backend/snapshot.h"
#include "common/hash.h"
#include "common/status_util.h"

namespace carmen::backend {

// This test utility file contains a complete example implementation of a
// snapshot-able type, including a proof, part, snapshot, and test-data type
// definition. It is intended to serve as a test utility for generic snapshot
// infrastructure, independent of any concrete snapshot implementation.

// TestProof is an example proof implementation for test cases.
class TestProof {
 public:
  TestProof(const Hash& hash) : hash_(hash) {}

  static absl::StatusOr<TestProof> FromBytes(std::span<const std::byte> data) {
    if (data.size() != sizeof(Hash)) {
      return absl::InvalidArgumentError(
          "Serialized TestProof has invalid length");
    }
    Hash hash;
    hash.SetBytes(data);
    return TestProof(hash);
  }

  std::vector<std::byte> ToBytes() const {
    std::span<const std::byte> data(hash_);
    return {data.begin(), data.end()};
  }

  const Hash& GetHash() const { return hash_; }

  bool operator==(const TestProof&) const = default;

 private:
  Hash hash_;
};

static_assert(Proof<TestProof>);

// TestPart is an example part implementation for tests cases.
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
    ASSIGN_OR_RETURN(auto proof,
                     Proof::FromBytes(data.subspan(0, sizeof(Proof))));
    return TestPart(proof, data.subspan(sizeof(Proof)));
  }

  const Proof& GetProof() const { return proof_; }

  bool Verify() const { return GetSha256Hash(data_) == proof_.GetHash(); }

  const std::vector<std::byte>& GetData() const { return data_; }

  std::vector<std::byte> ToBytes() const {
    // Serialized as proof, followed by data.
    std::vector<std::byte> res;
    res.reserve(sizeof(Hash) + data_.size());
    auto hash = proof_.ToBytes();
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
      : proof_(proof),
        proofs_(std::make_unique<std::vector<Proof>>()),
        data_(
            std::make_unique<std::vector<std::byte>>(data.begin(), data.end())),
        raw_source_(
            std::make_unique<ToRawDataSource>(proof, *proofs_, *data_)) {
    // Some extra padding to make the internal snapshot data size a multiple of
    // the data size.
    auto extra = data_->size() % part_size;
    if (extra > 0) {
      data_->resize(data_->size() + (part_size - extra));
    }
    assert(data_->size() >= data.size());
    assert(data_->size() < data.size() + part_size);
    assert(data_->size() % part_size == 0);

    // In a real setup, those part hashes would come from another source.
    for (std::size_t i = 0; i < GetSize(); i++) {
      proofs_->push_back(
          GetSha256Hash(std::span(*data_).subspan(i * part_size, part_size)));
    }
  }

  static absl::StatusOr<TestSnapshot> FromSource(
      const SnapshotDataSource& source) {
    // For the test snapshot, everything is stored in the metadata.
    ASSIGN_OR_RETURN(auto metadata, source.GetMetaData());
    if (metadata.size() < sizeof(Proof)) {
      return absl::InvalidArgumentError(
          "Invalid length of index snapshot metadata");
    }
    // TODO: build parsing and encoding utilities.
    Hash hash;
    hash.SetBytes(std::span(metadata).subspan(0, sizeof(Proof)));
    return TestSnapshot(hash, std::span(metadata).subspan(sizeof(Proof)));
  }

  const SnapshotDataSource& GetDataSource() const { return *raw_source_; }

  std::size_t GetSize() const {
    return data_->size() / part_size + (data_->size() % part_size > 0);
  }

  absl::StatusOr<Part> GetPart(std::size_t i) const {
    if (i >= GetSize()) {
      return absl::NotFoundError("no such part");
    }
    ASSIGN_OR_RETURN(auto proof, GetProof(i));
    auto data = std::span(*data_).subspan(i * part_size, part_size);
    return Part(proof, data);
  }

  Proof GetProof() const { return proof_; }

  absl::StatusOr<Proof> GetProof(std::size_t i) const {
    return i < proofs_->size() ? (*proofs_)[i] : Proof(Hash{});
  }

  absl::Status VerifyProofs() const {
    Sha256Hasher hasher;
    for (const auto& proof : *proofs_) {
      hasher.Ingest(proof.ToBytes());
    }
    Proof should = hasher.GetHash();
    if (should != proof_) {
      return absl::InternalError("Proofs are not consistent");
    }
    return absl::OkStatus();
  }

 private:
  class ToRawDataSource : public SnapshotDataSource {
   public:
    ToRawDataSource(const Proof& proof, const std::vector<Proof>& proofs,
                    const std::vector<std::byte>& data)
        : proofs_(proofs), data_(data) {
      // For the TestSnapshot, everything is encoded in the meta data for
      // simplicity.
      std::span<const std::byte> proof_span = proof.GetHash();
      metadata_.insert(metadata_.end(), proof_span.begin(), proof_span.end());
      metadata_.insert(metadata_.end(), data.begin(), data.end());
    }

    absl::StatusOr<std::vector<std::byte>> GetMetaData() const override {
      return metadata_;
    }

    absl::StatusOr<std::vector<std::byte>> GetProofData(
        std::size_t part_number) const override {
      return proofs_[part_number].ToBytes();
    }

    absl::StatusOr<std::vector<std::byte>> GetPartData(
        std::size_t part_number) const override {
      auto data = std::span(data_).subspan(part_number * part_size, part_size);
      return Part(proofs_[part_number], data).ToBytes();
    }

   private:
    std::vector<std::byte> metadata_;
    // Owned by the snapshot.
    const std::vector<Proof>& proofs_;
    const std::vector<std::byte>& data_;
  };

  // The full proof of the snapshot.
  Proof proof_;

  // The proofs of the individual parts.
  std::unique_ptr<std::vector<Proof>> proofs_;

  // The raw data of the snapshot, padded with 0s have a size that is a multiple
  // of the part_size.
  std::unique_ptr<std::vector<std::byte>> data_;

  std::unique_ptr<ToRawDataSource> raw_source_;
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

}  // namespace carmen::backend

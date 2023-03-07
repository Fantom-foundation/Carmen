#pragma once

#include <memory>
#include <vector>

#include "absl/status/statusor.h"
#include "backend/snapshot.h"
#include "common/hash.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::store {

// This file defines the snapshot format for stores. To that end, a format
// definition for proofs, parts, and the actual snapshot are provided.
//
// The snapshot of a store contains the list of pages in their respective order.
// Each page contains a list of values stored in the respective page of the
// store. The corresponding proofs are the respective page hashes. Thus, the
// individual verification of parts can be supported -- and the required hashes
// can be provided by stores efficiently.

// The proof type used by snapshots on stores. The proof for a part of the store
// is the hash of the page it represents. The proof for the full store is the
// recursively computed hash of the individual pages using the store's hash-tree
// algorithm.
struct StoreProof {
  StoreProof() = default;

  StoreProof(Hash hash) : hash(hash) {}

  bool operator==(const StoreProof&) const = default;

  // Serialization and deserialization.
  static absl::StatusOr<StoreProof> FromBytes(std::span<const std::byte>);
  std::vector<std::byte> ToBytes() const;

  // The hash of the store at the snapshot time.
  Hash hash;
};

// A StorePart is the unit of data to be transfered between synchronizing
// systems. Each part matches a single page of the store.
template <Trivial V>
class StorePart {
 public:
  using Proof = StoreProof;

  StorePart(Proof proof, std::vector<V> values)
      : proof_(proof), values_(std::move(values)) {}

  // Serialization and deserialization -- for instance, to be used for
  // exchanges.
  static absl::StatusOr<StorePart> FromBytes(std::span<const std::byte>);
  std::vector<std::byte> ToBytes() const;

  const StoreProof& GetProof() const { return proof_; }
  const std::vector<V>& GetValues() const { return values_; }

  // Verifies that the values stored in this part are consistent with the
  // present proof.
  bool Verify() const;

 private:
  // The proof certifying the content of this part.
  StoreProof proof_;
  // The values contained in this part.
  std::vector<V> values_;
};

// An interface to be implemented by concrete Store implementations or store
// synchronization sources to provide store synchronization data.
template <Trivial V>
class StoreSnapshotDataSource {
 public:
  StoreSnapshotDataSource(std::size_t num_pages) : num_parts_(num_pages) {}

  virtual ~StoreSnapshotDataSource(){};

  // Retrieves the total number of parts in a snapshot.
  std::size_t GetSize() const { return num_parts_; }

  // Retrieves the proof expected for a given part.
  virtual absl::StatusOr<StoreProof> GetProof(
      std::size_t part_number) const = 0;

  // Retrieves the data of an individual part of this snapshot.
  virtual absl::StatusOr<StorePart<V>> GetPart(
      std::size_t part_number) const = 0;

 private:
  // The number of parts the store snapshot comprises.
  const std::size_t num_parts_;
};

// A snapshot of the state of a store providing access to the contained data
// frozen at it creation time.
//
// The life cycle of a snapshot defines the duration of its availability.
// Snapshots are volatile, thus not persistent over application restarts. A
// snapshot is created by a call to `CreateSnapshot()` on a store instance, and
// destroyed upon destruction. It does not (need) to persist beyond the lifetime
// of the current process.
//
// Store snapshots consist of a range of StoreParts, partitioning the list of
// all values present in a store into fixed-sized, consecutive values, matching
// individual pages. Each part has its own proof, certifying its content.
// Furthermore, the snapshot retains a proof enabling the verification of the
// proofs of the individual parts.
template <Trivial V>
class StoreSnapshot {
 public:
  using value_type = V;
  using Proof = StoreProof;
  using Part = StorePart<V>;

  StoreSnapshot(const std::size_t branching_factor, const Hash& hash,
                std::unique_ptr<StoreSnapshotDataSource<V>> source)
      : branching_factor_(branching_factor),
        proof_(hash),
        source_(std::move(source)),
        raw_source_(std::make_unique<ToRawDataSource>(branching_factor, hash,
                                                      source_.get())) {}

  static absl::StatusOr<StoreSnapshot> FromSource(
      const SnapshotDataSource& source) {
    ASSIGN_OR_RETURN(auto metadata, source.GetMetaData());
    // TODO: build parsing and encoding utilities.
    static_assert(sizeof(std::size_t) == 8);
    if (metadata.size() != 8 + 8 + sizeof(Hash)) {
      return absl::InvalidArgumentError(
          "Invalid length of store snapshot metadata");
    }
    auto size_ptr = reinterpret_cast<std::size_t*>(metadata.data());
    std::size_t branching_factor = size_ptr[0];
    std::size_t num_pages = size_ptr[1];
    Hash hash;
    hash.SetBytes(std::span(metadata).subspan(16));
    return StoreSnapshot(
        branching_factor, hash,
        std::make_unique<FromRawDataSource>(num_pages, source));
  }

  const SnapshotDataSource& GetDataSource() const { return *raw_source_; }

  // Obtains the number of parts stored in the snapshot.
  std::size_t GetSize() const { return source_->GetSize(); }

  // Obtains the proof for the entire snapshot.
  Proof GetProof() const { return proof_; }

  // Obtains the expected proof for a given part.
  absl::StatusOr<Proof> GetProof(std::size_t part_number) const {
    return source_->GetProof(part_number);
  }

  // Obtains a copy of an individual part of this snapshot.
  absl::StatusOr<Part> GetPart(std::size_t part_number) const {
    return source_->GetPart(part_number);
  }

  // Verifies that the proofs of individual parts are consistent with the full
  // snapshot proof. Note: this does not verify that the content of individual
  // parts are consistent with their respective proof.
  absl::Status VerifyProofs() const;

 private:
  class FromRawDataSource : public StoreSnapshotDataSource<V> {
   public:
    FromRawDataSource(std::size_t num_pages, const SnapshotDataSource& source)
        : StoreSnapshotDataSource<V>(num_pages), source_(source) {}

    absl::StatusOr<StoreProof> GetProof(
        std::size_t part_number) const override {
      ASSIGN_OR_RETURN(auto data, source_.GetProofData(part_number));
      return StoreProof::FromBytes(data);
    }

    absl::StatusOr<StorePart<V>> GetPart(
        std::size_t part_number) const override {
      ASSIGN_OR_RETURN(auto data, source_.GetPartData(part_number));
      return StorePart<V>::FromBytes(data);
    }

   private:
    const SnapshotDataSource& source_;
  };

  class ToRawDataSource : public SnapshotDataSource {
   public:
    ToRawDataSource(std::size_t branching_factor, const Hash& hash,
                    StoreSnapshotDataSource<V>* source)
        : source_(source) {
      static_assert(sizeof(std::size_t) == 8);
      metadata_.reserve(8 + 8 + sizeof(hash));
      metadata_.resize(16);
      auto size_ptr = reinterpret_cast<std::size_t*>(metadata_.data());
      size_ptr[0] = branching_factor;
      size_ptr[1] = source->GetSize();
      std::span<const std::byte> hash_span = hash;
      metadata_.insert(metadata_.end(), hash_span.begin(), hash_span.end());
    }

    absl::StatusOr<std::vector<std::byte>> GetMetaData() const override {
      return metadata_;
    }

    absl::StatusOr<std::vector<std::byte>> GetProofData(
        std::size_t part_number) const override {
      ASSIGN_OR_RETURN(auto proof, source_->GetProof(part_number));
      return proof.ToBytes();
    }

    absl::StatusOr<std::vector<std::byte>> GetPartData(
        std::size_t part_number) const override {
      ASSIGN_OR_RETURN(auto part, source_->GetPart(part_number));
      return part.ToBytes();
    }

   private:
    std::vector<std::byte> metadata_;
    // Owned by the snapshot.
    StoreSnapshotDataSource<V>* source_;
  };

  // The branching factor used in the reduction tree for computing hashes.
  std::size_t branching_factor_;

  // The full-store proof of this snapshot.
  Proof proof_;

  // The data source for store data.
  std::unique_ptr<StoreSnapshotDataSource<V>> source_;

  // The raw data source this snapshot provides to external consumers.
  std::unique_ptr<ToRawDataSource> raw_source_;
};

// ----------------------------- Definitions ----------------------------------

template <Trivial V>
absl::StatusOr<StorePart<V>> StorePart<V>::FromBytes(
    std::span<const std::byte> data) {
  if (data.size() < sizeof(Proof)) {
    return absl::InvalidArgumentError(
        "Invalid encoding of store part, too few bytes.");
  }
  if ((data.size() - sizeof(Proof)) % sizeof(V) != 0) {
    return absl::InvalidArgumentError(
        "Invalid encoding of store part, invalid length.");
  }
  Proof proof;
  proof.hash.SetBytes(data.subspan(0, sizeof(Hash)));
  auto num_values = (data.size() - sizeof(Hash)) / sizeof(V);
  std::vector<V> values;
  values.resize(num_values);
  std::memcpy(values.data(), data.subspan(sizeof(Hash)).data(),
              num_values * sizeof(V));
  return StorePart(proof, std::move(values));
}

template <Trivial V>
std::vector<std::byte> StorePart<V>::ToBytes() const {
  std::vector<std::byte> res;
  res.reserve(sizeof(Hash) + sizeof(V) * values_.size());
  auto begin = reinterpret_cast<const std::byte*>(&proof_);
  res.insert(res.end(), begin, begin + sizeof(Proof));
  begin = reinterpret_cast<const std::byte*>(values_.data());
  res.insert(res.end(), begin, begin + sizeof(V) * values_.size());
  return res;
}

template <Trivial V>
bool StorePart<V>::Verify() const {
  Hash have = GetSha256Hash(std::as_bytes(std::span(values_)));
  return have == proof_.hash;
}

template <Trivial V>
absl::Status StoreSnapshot<V>::VerifyProofs() const {
  // Collect all hashes of the pages.
  std::vector<Hash> hashes;
  hashes.reserve(GetSize());
  for (std::size_t i = 0; i < GetSize(); i++) {
    ASSIGN_OR_RETURN(auto proof, GetProof(i));
    hashes.push_back(proof.hash);
  }

  if (hashes.empty()) {
    return proof_.hash == Hash{}
               ? absl::OkStatus()
               : absl::InternalError("Proof chain is inconsistent.");
  }

  // Create a utility padding the hash vector to a length being a multiple of
  // the branching factor.
  auto pad_hashes = [&]() {
    if (hashes.size() % branching_factor_ != 0) {
      hashes.resize(hashes.size() +
                    (branching_factor_ - hashes.size() % branching_factor_));
    }
    assert(hashes.size() % branching_factor_ == 0);
  };

  while (hashes.size() > 1) {
    pad_hashes();
    for (std::size_t i = 0; i < hashes.size() / branching_factor_; i++) {
      hashes[i] = GetSha256Hash(std::as_bytes(
          std::span(hashes).subspan(i * branching_factor_, branching_factor_)));
    }
    hashes.resize(hashes.size() / branching_factor_);
  }

  return proof_.hash == hashes[0]
             ? absl::OkStatus()
             : absl::InternalError("Proof chain is inconsistent.");
}

}  // namespace carmen::backend::store

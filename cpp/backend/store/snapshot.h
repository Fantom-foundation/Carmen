#pragma once

#include <memory>
#include <vector>

#include "absl/status/statusor.h"
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

template <Trivial K>
class StoreSnapshotDataSource;

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

  StoreSnapshot(const Hash& hash,
                std::unique_ptr<StoreSnapshotDataSource<V>> source)
      : proof_(hash), source_(std::move(source)) {}

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
  // The full-store proof of this snapshot.
  const Proof proof_;

  // The data source for store data.
  std::unique_ptr<StoreSnapshotDataSource<V>> source_;
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

  // Aggregate those hashes hierarchically.
  const std::size_t kBranching = 32;  // for now constant, update if needed

  // Create a utility padding the hash vector to a length being a multiple of
  // the branching factor.
  auto pad_hashes = [&]() {
    if (hashes.size() % kBranching != 0) {
      hashes.resize(hashes.size() + (kBranching - hashes.size() % kBranching));
    }
    assert(hashes.size() % kBranching == 0);
  };

  pad_hashes();
  while (hashes.size() > 1) {
    for (std::size_t i = 0; i < hashes.size(); i += kBranching) {
      hashes[i] = GetSha256Hash(
          std::as_bytes(std::span(hashes).subspan(i * kBranching, kBranching)));
    }
    hashes.resize(hashes.size() / kBranching);
    pad_hashes();
  }
  return proof_.hash == hashes[0]
             ? absl::OkStatus()
             : absl::InternalError("Proof chain is inconsistent.");
}

}  // namespace carmen::backend::store

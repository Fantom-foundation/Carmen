#pragma once

#include <memory>
#include <vector>

#include "absl/status/statusor.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::index {

// This file defines the snapshot format for indexes. To that end, a format
// definition for proofs, parts, and the actual snapshot need to be provided.
//
// The snapshot of an index contains the list of keys in their insertion order.
// This list is partitioned into fixed-length sub-lists (=Part), that can be
// transfered and verified independently. The corresponding proofs comprise the
// hash of the archive before the first key of the respective part was added to
// the index, as well as the hash after the last. Thus, the individual
// verification of parts can be supported -- and the required hashes can be
// provided by indexes efficiently.

// The proof type used by snapshots on Indexes. The proof for a sub-range of
// keys contains the hash before the first and after the last key in the range.
struct IndexProof {
  IndexProof() = default;

  IndexProof(Hash end) : begin(), end(end) {}
  IndexProof(Hash begin, Hash end) : begin(begin), end(end) {}

  bool operator==(const IndexProof&) const = default;

  // The hash before the first key of the certified range.
  Hash begin;
  // The hash after the last key of the certified range.
  Hash end;
};

// An IndexPart is the unit of data to be transfered between synchronizing
// systems. It comprises a range of keys stored in an index, in their insertion
// order. For a given (non-empty) snapshot, all but the last part exhibit the
// same fixed size.
template <Trivial K>
class IndexPart {
 public:
  using Proof = IndexProof;

  IndexPart(Proof proof, std::vector<K> keys)
      : proof_(proof), keys_(std::move(keys)) {}

  // Serialization and deserialization -- for instance, to be used for
  // exchanges.
  static absl::StatusOr<IndexPart> FromBytes(std::span<const std::byte>);
  std::vector<std::byte> ToBytes() const;

  const IndexProof& GetProof() const { return proof_; }
  const std::vector<K>& GetKeys() const { return keys_; }

  // Verifies that the keys stored in this part are consistent with the present
  // proof.
  bool Verify() const;

 private:
  // The proof certifying the content of this part.
  IndexProof proof_;
  // The keys contained in this part.
  std::vector<K> keys_;
};

template <Trivial K>
class IndexSnapshotDataSource;

// A snapshot of the state of an index providing access to the contained data
// frozen at it creation time.
//
// The life cycle of a snapshot defines the duration of its availability.
// Snapshots are volatile, thus not persistent over application restarts. A
// snapshot is created by a call to `CreateSnapshot()` on an index instance, and
// destroyed upon destruction. It does not (need) to persist beyond the lifetime
// of the current process.
//
// Index snapshots consist of a range of IndexParts, partitioning the list of
// all keys present in an index into fixed-sized, consecutive key ranges. Only
// the last range may be smaller than the fix size. Each part has its own proof,
// certifying its content. Furthermore, the snapshot retains a proof enabling
// the verification of the proofs of the individual parts.
template <Trivial K>
class IndexSnapshot {
 public:
  using key_type = K;
  using Proof = IndexProof;
  using Part = IndexPart<K>;

  IndexSnapshot(const Hash& hash,
                std::unique_ptr<IndexSnapshotDataSource<K>> source)
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
  // The full-index proof of this snapshot.
  const Proof proof_;

  // The data source for index data.
  std::unique_ptr<IndexSnapshotDataSource<K>> source_;
};

// An interface to be implemented by concrete Index implementations or index
// synchronization sources to provide index synchronization data.
template <Trivial K>
class IndexSnapshotDataSource {
 public:
  // The targeted size of a part in bytes.
  static constexpr std::size_t kPartSizeInBytes = 4096;  // = 4 KB
  static constexpr std::size_t kKeysPerPart = kPartSizeInBytes / sizeof(K);

  IndexSnapshotDataSource(std::size_t num_keys)
      : num_parts_(num_keys / kKeysPerPart + (num_keys % kKeysPerPart > 0)) {}

  virtual ~IndexSnapshotDataSource(){};

  // Retrieves the total number of parts in a snapshot.
  std::size_t GetSize() const { return num_parts_; }

  // Retrieves the proof expected for a given part.
  virtual absl::StatusOr<IndexProof> GetProof(
      std::size_t part_number) const = 0;

  // Retrieves the data of an individual part of this snapshot.
  virtual absl::StatusOr<IndexPart<K>> GetPart(
      std::size_t part_number) const = 0;

 private:
  // The number of parts the index snapshot comprises.
  const std::size_t num_parts_;
};

// ----------------------------- Definitions ----------------------------------

template <Trivial K>
absl::StatusOr<IndexPart<K>> IndexPart<K>::FromBytes(
    std::span<const std::byte> data) {
  if (data.size() < sizeof(Proof)) {
    return absl::InvalidArgumentError(
        "Invalid encoding of index part, too few bytes.");
  }
  if ((data.size() - sizeof(Proof)) % sizeof(K) != 0) {
    return absl::InvalidArgumentError(
        "Invalid encoding of index part, invalid length.");
  }
  Proof proof;
  proof.begin.SetBytes(data.subspan(0, sizeof(Hash)));
  proof.end.SetBytes(data.subspan(sizeof(Hash), sizeof(Hash)));
  auto num_keys = (data.size() - sizeof(Proof)) / sizeof(K);
  std::vector<K> keys;
  keys.resize(num_keys);
  std::memcpy(keys.data(), data.subspan(sizeof(Proof)).data(),
              num_keys * sizeof(K));
  return IndexPart(proof, std::move(keys));
}

template <Trivial K>
std::vector<std::byte> IndexPart<K>::ToBytes() const {
  std::vector<std::byte> res;
  res.reserve(sizeof(Proof) + sizeof(K) * keys_.size());
  auto begin = reinterpret_cast<const std::byte*>(&proof_);
  res.insert(res.end(), begin, begin + sizeof(Proof));
  begin = reinterpret_cast<const std::byte*>(keys_.data());
  res.insert(res.end(), begin, begin + sizeof(K) * keys_.size());
  return res;
}

template <Trivial K>
bool IndexPart<K>::Verify() const {
  Hash hash = proof_.begin;
  for (const K& cur : keys_) {
    hash = GetSha256Hash(hash, cur);
  }
  return hash == proof_.end;
}

template <Trivial K>
absl::Status IndexSnapshot<K>::VerifyProofs() const {
  Hash last{};
  for (std::size_t i = 0; i < GetSize(); i++) {
    ASSIGN_OR_RETURN(auto part_proof, GetProof(i));
    if (last != part_proof.begin) {
      return absl::InternalError("Proof chain is inconsistent.");
    }
    last = part_proof.end;
  }
  if (last != proof_) {
    return absl::InternalError("Proof chain is inconsistent.");
  }
  return absl::OkStatus();
}

}  // namespace carmen::backend::index

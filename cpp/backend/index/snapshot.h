#pragma once

#include <cstddef>
#include <memory>
#include <span>
#include <vector>

#include "absl/status/statusor.h"
#include "backend/snapshot.h"
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

  // Serialization and deserialization.
  static absl::StatusOr<IndexProof> FromBytes(std::span<const std::byte>);
  std::vector<std::byte> ToBytes() const;

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

// An interface to be implemented by concrete Index implementations.
template <Trivial K>
class IndexSnapshotDataSource {
 public:
  // The targeted size of a part in bytes.
  static constexpr std::size_t kPartSizeInBytes = 4096;  // = 4 KB
  static constexpr std::size_t kKeysPerPart = kPartSizeInBytes / sizeof(K);

  IndexSnapshotDataSource(std::size_t num_keys) : num_keys_(num_keys) {}

  virtual ~IndexSnapshotDataSource(){};

  // Retrieves the total number of parts in the covered snapshot.
  std::size_t GetSize() const {
    return num_keys_ / kKeysPerPart + (num_keys_ % kKeysPerPart > 0);
  }

  // Retrieves the total number of keys in this snapshot.
  std::size_t GetNumKeys() const { return num_keys_; }

  // Retrieves the proof expected for a given part.
  virtual absl::StatusOr<IndexProof> GetProof(
      std::size_t part_number) const = 0;

  // Retrieves the data of an individual part of this snapshot.
  virtual absl::StatusOr<IndexPart<K>> GetPart(
      std::size_t part_number) const = 0;

 private:
  // The number of keys the index snapshot comprises.
  const std::size_t num_keys_;
};

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
      : proof_(hash),
        source_(std::move(source)),
        raw_source_(std::make_unique<ToRawDataSource>(hash, source_.get())) {}

  static absl::StatusOr<IndexSnapshot> FromSource(
      const SnapshotDataSource& source) {
    ASSIGN_OR_RETURN(auto metadata, source.GetMetaData());
    if (metadata.size() != 8 + sizeof(Hash)) {
      return absl::InvalidArgumentError(
          "Invalid length of index snapshot metadata");
    }
    // TODO: build parsing and encoding utilities.
    std::size_t num_keys = *reinterpret_cast<std::size_t*>(metadata.data());
    Hash hash;
    hash.SetBytes(std::span(metadata).subspan(8));
    return IndexSnapshot(hash,
                         std::make_unique<FromRawDataSource>(num_keys, source));
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
  class FromRawDataSource : public IndexSnapshotDataSource<K> {
   public:
    FromRawDataSource(std::size_t num_keys, const SnapshotDataSource& source)
        : IndexSnapshotDataSource<K>(num_keys), source_(source) {}

    absl::StatusOr<IndexProof> GetProof(
        std::size_t part_number) const override {
      ASSIGN_OR_RETURN(auto data, source_.GetProofData(part_number));
      return IndexProof::FromBytes(data);
    }

    absl::StatusOr<IndexPart<K>> GetPart(
        std::size_t part_number) const override {
      ASSIGN_OR_RETURN(auto data, source_.GetPartData(part_number));
      return IndexPart<K>::FromBytes(data);
    }

   private:
    const SnapshotDataSource& source_;
  };

  class ToRawDataSource : public SnapshotDataSource {
   public:
    ToRawDataSource(const Hash& hash, IndexSnapshotDataSource<K>* source)
        : source_(source) {
      static_assert(sizeof(std::size_t) == 8);
      metadata_.reserve(8 + sizeof(hash));
      metadata_.resize(8);
      *reinterpret_cast<std::size_t*>(metadata_.data()) = source->GetNumKeys();
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
    IndexSnapshotDataSource<K>* source_;
  };

  // The full-index proof of this snapshot.
  Proof proof_;

  // The data source for index data.
  std::unique_ptr<IndexSnapshotDataSource<K>> source_;

  // The raw data source this snapshot provides to external consumers.
  std::unique_ptr<ToRawDataSource> raw_source_;
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

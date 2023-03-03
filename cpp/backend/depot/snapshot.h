#pragma once

#include <memory>
#include <vector>

#include "absl/status/statusor.h"
#include "common/hash.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::depot {

// This file defines the snapshot format for Depots. To that end, a format
// definition for proofs, parts, and the actual snapshot are provided.
//
// The snapshot of a depot contains the list of pages in their respective order.
// Each page contains a list of blobs stored in the respective page of the
// depot. The corresponding proofs are the respective page hashes. Thus, the
// individual verification of parts can be supported -- and the required hashes
// can be provided by depots efficiently.

// The proof type used by snapshots on depots. The proof for a part of the depot
// is the hash of the page it represents. The proof for the full depot is the
// recursively computed hash of the individual pages using the store's hash-tree
// algorithm.
struct DepotProof {
  DepotProof() = default;
  DepotProof(Hash hash) : hash(hash) {}
  bool operator==(const DepotProof&) const = default;

  // The hash of the depot at the snapshot time.
  Hash hash;
};

// A DepotPart is the unit of data to be transfered between synchronizing
// systems. Each part matches a single page of the depot.
class DepotPart {
 public:
  using Proof = DepotProof;

  DepotPart(Proof proof, std::vector<std::byte> data)
      : proof_(proof), data_(std::move(data)) {}

  // Serialization and deserialization -- for instance, to be used for
  // exchanges.
  static absl::StatusOr<DepotPart> FromBytes(std::span<const std::byte>);
  std::vector<std::byte> ToBytes() const;

  const DepotProof& GetProof() const { return proof_; }
  const std::vector<std::byte>& GetData() const { return data_; }

  // Verifies that the values stored in this part are consistent with the
  // present proof.
  bool Verify() const;

 private:
  // The proof certifying the content of this part.
  DepotProof proof_;
  // The values contained in this part.
  std::vector<std::byte> data_;
};

class DepotSnapshotDataSource;

// A snapshot of the state of a depot providing access to the contained data
// frozen at it creation time.
//
// The life cycle of a snapshot defines the duration of its availability.
// Snapshots are volatile, thus not persistent over application restarts. A
// snapshot is created by a call to `CreateSnapshot()` on a depot instance, and
// destroyed upon destruction. It does not (need) to persist beyond the lifetime
// of the current process.
//
// Depot snapshots consist of a range of DepotParts, partitioning the list of
// all values present in a depot into variable-sized, consecutive entries,
// matching individual depot-pages. Each part has its own proof, certifying its
// content. Furthermore, the snapshot retains a proof enabling the verification
// of the proofs of the individual parts.
class DepotSnapshot {
 public:
  using Proof = DepotProof;
  using Part = DepotPart;

  DepotSnapshot(const std::size_t branching_factor, const Hash& hash,
                std::unique_ptr<DepotSnapshotDataSource> source)
      : branching_factor_(branching_factor),
        proof_(hash),
        source_(std::move(source)) {}

  // Obtains the number of parts stored in the snapshot.
  std::size_t GetSize() const;

  // Obtains the proof for the entire snapshot.
  Proof GetProof() const;

  // Obtains the expected proof for a given part.
  absl::StatusOr<Proof> GetProof(std::size_t part_number) const;

  // Obtains a copy of an individual part of this snapshot.
  absl::StatusOr<Part> GetPart(std::size_t part_number) const;

  // Verifies that the proofs of individual parts are consistent with the full
  // snapshot proof. Note: this does not verify that the content of individual
  // parts are consistent with their respective proof.
  absl::Status VerifyProofs() const;

 private:
  // The branching factor used in the reduction tree for computing hashes.
  const std::size_t branching_factor_;

  // The full-depot proof of this snapshot.
  const Proof proof_;

  // The data source for depot data.
  std::unique_ptr<DepotSnapshotDataSource> source_;
};

// An interface to be implemented by concrete Depot implementations or depot
// synchronization sources to provide depot synchronization data.
class DepotSnapshotDataSource {
 public:
  DepotSnapshotDataSource(std::size_t num_pages) : num_parts_(num_pages) {}

  virtual ~DepotSnapshotDataSource(){};

  // Retrieves the total number of parts in a snapshot.
  std::size_t GetSize() const { return num_parts_; }

  // Retrieves the proof expected for a given part.
  virtual absl::StatusOr<DepotProof> GetProof(
      std::size_t part_number) const = 0;

  // Retrieves the data of an individual part of this snapshot.
  virtual absl::StatusOr<DepotPart> GetPart(std::size_t part_number) const = 0;

 private:
  // The number of parts the store snapshot comprises.
  const std::size_t num_parts_;
};

}  // namespace carmen::backend::depot

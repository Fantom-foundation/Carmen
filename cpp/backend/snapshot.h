#pragma once

#include <concepts>
#include <cstddef>
#include <span>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/type.h"

namespace carmen::backend {

// This header file defines the basic concepts for components contributing to
// the snapshot infrastructure of data structures. The infrastructures intention
// is to provide a common abstract model of data to be verified and synchronized
// among multiple instances.
//
// The snapshot infrastructure composed by three concepts:
//  - Snapshots, comprising a finite list of parts
//  - Parts, describing chunks of data of a structure, and
//  - Proofs, to verify the consistency of parts and snapshots
//
// The structure of proofs, parts, and snapshots may be data structure specific,
// and are thus expressed as concepts. Abstract base types (=interfaces) have
// been considered, yet the consequential need of the utilization of (smart)
// pointers to facilitate polymorthism in the interfaces let us decide against
// it.

template <typename S>
concept Serializable = requires(const S s) {
  // Types have to be serializable into a sequence of bytes.
  { s.ToBytes() } -> std::convertible_to<std::vector<std::byte>>;

  // The deserialization should be able to reconstruct a instance that was
  // previously serialized using ToBytes().
  {
    S::FromBytes(std::declval<std::span<const std::byte>>())
    } -> std::same_as<absl::StatusOr<S>>;
};

template <typename P>
concept Proof =
    // Proof have to be serializable to be exchangeable between nodes.
    Serializable<P> &&
    // Proofs also need to be comparable.
    std::equality_comparable<P>;

template <typename P>
concept Part =
    // Parts have to define a proof type.
    Proof<typename P::Proof> &&
    // Parts have to be serializable to be exchangeable between nodes.
    Serializable<P> && requires(const P p) {
  // Parts have to be able to produce proof of their content.
  { p.GetProof() } -> std::convertible_to<typename P::Proof>;

  // Verifies that the contained proof matches the data of this part.
  { p.Verify() } -> std::convertible_to<bool>;
};

template <typename S>
concept Snapshot =
    // A snapshot must define a proof type.
    Proof<typename S::Proof> &&

    // A snapshot must define a part type.
    Part<typename S::Part> &&

    // A snapshost can be move-constructed, to support moving it in and out of a
    // absl::StatusOr instance.
    std::is_move_constructible_v<S> &&

    requires(const S s) {
  // --- Part Inspection ---

  // A snapshot must provide the total number of parts.
  { s.GetSize() } -> std::convertible_to<std::size_t>;

  // A snapshot must provide access to parts.
  {
    s.GetPart(std::declval<std::size_t>())
    } -> std::same_as<absl::StatusOr<typename S::Part>>;

  // --- Verification ---

  // A snapshot must be able to produce proof of their content.
  { s.GetProof() } -> std::convertible_to<typename S::Proof>;

  // A snapshot must also be able to produce proofs for each of its parts.
  {
    s.GetProof(std::declval<std::size_t>())
    } -> std::same_as<absl::StatusOr<typename S::Proof>>;

  // A snapshot must have a procedure to verify the proofs of the parts.
  { s.VerifyProofs() } -> std::same_as<absl::Status>;
};

template <typename S>
concept Snapshotable =
    // A snapshotable structure must export a snapshot type.
    Snapshot<typename S::Snapshot> &&

    requires(const S s) {
  // A snapshotable structure must provide a proof of its content.
  { s.GetProof() } -> std::same_as<absl::StatusOr<typename S::Snapshot::Proof>>;

  // A snapshotable type must provide a way to capture a snapshot.
  { s.CreateSnapshot() } -> std::same_as<absl::StatusOr<typename S::Snapshot>>;
};

}  // namespace carmen::backend

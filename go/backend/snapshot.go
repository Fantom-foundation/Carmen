// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package backend

import "errors"

// Snapshots capture a state of a single or a set of data structures at a
// moment in time for future reference.
//
// Snapshots are volatile, thus they do not survive the current process.
// Snapshots are created by calling a corresponding factory function, typically
// `CreateSnapshot` on the data structure to be snapshotted, and released by
// calling the snapshot's `Release` method.
//
// Logically, each snapshot describes a range of `Parts`, where each part
// describes a chunk of data of the data structure's forzen state. Parts can
// be retrieved individually, to facilitate streaming of snapshots, potentially
// even from multiple sources.
//
// Each part has a Proof associated, capable of certifying its content. Those
// proofs are, like Parts, data structure dependent.
//
// Furthermore, snapshots provide access to Proofs of Parts without retrieving
// the full Part and its associated data, for faster cross-validation.
//
// Finally, a snapshot combines the proofs of all its parts into a single root
// proof, to cross-validate snapshots without the need of exchanging all the
// proofs of its parts. The algorithm used for aggregating those proofs is
// data structure dependent.
type Snapshot interface {
	// GetRootProof retrieves the aggregated proof for this snapshot.
	GetRootProof() Proof
	// GetNumParts retrieves the number of parts in this snapshot.
	GetNumParts() int
	// GetProof retrieves the proof for a part, without loading the part.
	GetProof(partNumber int) (Proof, error)
	// GetPart retrieves a part of the snapshot.
	GetPart(partNumber int) (Part, error)

	// GetData provides a type-erased view on this snapshot to be used for
	// syncing data structures, potentially over the network.
	GetData() SnapshotData

	// Release destroys this snapshot, invalidating all derived objects.
	Release() error
}

// Part is a chunk of data of a data structure's snapshot.
type Part interface {
	// ToBytes serializes this part such that it can be transfered through IO.
	ToBytes() []byte
}

// Proof is a piece of information that can be used to certify the content of
// a Part or an entire snapshot.
type Proof interface {
	// Tests whether this proof is equal to the given proof.
	Equal(proof Proof) bool
	// ToBytes serializes this proof such that it can be transfered through IO.
	ToBytes() []byte
}

// SnapshotData is a type-erased view on a snapshot that is intended to be used
// for syncing data between data structure instances.
type SnapshotData interface {
	// GetMetaData retrieves snapshot specific metadata describing the content
	// and structure of the snapshot. For instance, it is likely to include the
	// snapshot's root proof and number of parts. However, the format is data-
	// structure specific.
	GetMetaData() ([]byte, error)
	// GetProofData retrieves a serialized form of the proof of a requested part.
	GetProofData(partNumber int) ([]byte, error)
	// GetPartData retrieves a serialized form of a requested part.
	GetPartData(partNumber int) ([]byte, error)
}

// SnapshotVerifier provides abstract means for verifying individual parts of a
// snapshot. It is to be provided by a snapshotable data structure to enable
// the verification of snapshot data during synchronization.
type SnapshotVerifier interface {
	// VerifyRootProof verifies that the proofs of the parts provided by the
	// given SnapshotData are consistent with the snapshot's root proof, which
	// is returned for cross-referencing with other sources.
	VerifyRootProof(data SnapshotData) (Proof, error)
	// VerifyPart tests that the given proof is valid for the provided part.
	VerifyPart(number int, proof, part []byte) error
}

// Snapshotable is an interface to be implemented by data structure to support
// integration into the snapshotting infrastructure.
type Snapshotable interface {
	// GetProof returns a proof the snapshot exhibits if it is created
	// for the current state of the data structure.
	GetProof() (Proof, error)
	// CreateSnapshot creates a snapshot of the current state of the data
	// structure. The snapshot should be shielded from subsequent modifications
	// and be accessible until released.
	CreateSnapshot() (Snapshot, error)
	// Restore restores the data structure to the given snapshot state. This
	// may invalidate any former snapshots created on the data structure. In
	// particular, it is not required to be able to synchronize to a former
	// snapshot derived from the targeted data structure.
	Restore(data SnapshotData) error
	// GetSnapshotVerifier produces a verifier for snapshot data accepted by
	// this Snapshotable data structure. This fails if the given metadata does
	// not describing a snapshot format compatible with this data structure.
	GetSnapshotVerifier(metadata []byte) (SnapshotVerifier, error)
}

// An error that implementations may return if snapshots are not supported.
var ErrSnapshotNotSupported = errors.New("this implementation does not support snapshots")

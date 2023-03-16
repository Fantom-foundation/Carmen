package backend

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
// proofs are, like Parts, data structure dependent. However, Parts provide can
// by verified through ther `Verify` method.
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
	GetProof(part_number int) (Proof, error)
	// GetPart retrieves a part of the snapshot.
	GetPart(part_number int) (Part, error)

	// VerifyProofs verifies that the proofs of the parts are consistent with
	// snapshot's root proof. Note: it does not verify individual parts.
	VerifyRootProof() error

	// GetData provides a type-erased view on this snapshot to be used for
	// syncing data structures, potentially over the network.
	GetData() SnapshotData

	// Release destroys this snapshot, invalidating all derived objects.
	Release() error
}

// Part is a chunk of data of a data structure's snapshot.
type Part interface {
	// Verify tests that the given proof is valid for the contained data.
	Verify(proof Proof) bool
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
	GetProofData(part_number int) ([]byte, error)
	// GetPartData retrieves a serialized form of a requested part.
	GetPartData(part_number int) ([]byte, error)
}

// Snapshotable is an interface to be implemented by data structure to support
// integration into the snapshotting infrastructure.
type Snapshotable interface {
	// GetProof returns a proof a snapshot would exhibit if it would be created
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
}

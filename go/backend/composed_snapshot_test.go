package backend_test

import (
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// This test file contains an example implementation of a data structure
// (myComposedDataStructure) using two snapshot-able data structures as sub-
// components and the ComposableSnapshot utility to realize snapshot support.

// -------------------------- Composed DataStructure --------------------------

type myComposedDataStructure struct {
	a, b myDataStructure
}

func (s *myComposedDataStructure) GetProof() (backend.Proof, error) {
	proof_a, err := s.a.GetProof()
	if err != nil {
		return nil, err
	}
	proof_b, err := s.b.GetProof()
	if err != nil {
		return nil, err
	}
	return backend.GetComposedProof([]backend.Proof{proof_a, proof_b}), nil
}

func (s *myComposedDataStructure) CreateSnapshot() (backend.Snapshot, error) {
	snapshot_a, err := s.a.CreateSnapshot()
	if err != nil {
		return nil, err
	}
	snapshot_b, err := s.b.CreateSnapshot()
	if err != nil {
		return nil, err
	}
	return backend.NewComposedSnapshot([]backend.Snapshot{snapshot_a, snapshot_b}), nil
}

func (s *myComposedDataStructure) Restore(data backend.SnapshotData) error {
	snapshot, err := createCompositeDataSnapshotFromData(data)
	if err != nil {
		return err
	}

	snapshots := snapshot.GetSnapshots()
	if err := s.a.Restore(snapshots[0].GetData()); err != nil {
		return err
	}
	return s.b.Restore(snapshots[1].GetData())
}

func (s *myComposedDataStructure) GetSnapshotVerifier(data backend.SnapshotData) (backend.SnapshotVerifier, error) {
	subData, partCounts, err := backend.SplitCompositeData(data)
	if err != nil {
		return nil, err
	}
	verifierA, err := s.a.GetSnapshotVerifier(subData[0])
	if err != nil {
		return nil, err
	}
	verifierB, err := s.a.GetSnapshotVerifier(subData[1])
	if err != nil {
		return nil, err
	}
	return backend.NewComposedSnapshotVerifier([]backend.SnapshotVerifier{verifierA, verifierB}, partCounts), nil
}

func createCompositeDataSnapshotFromData(data backend.SnapshotData) (*backend.ComposedSnapshot, error) {
	split, _, err := backend.SplitCompositeData(data)
	if err != nil {
		return nil, err
	}
	if len(split) != 2 {
		return nil, fmt.Errorf("invalid number of sub-snapshots")
	}

	snapshot_a, err := createMySnapshotFromData(split[0])
	if err != nil {
		return nil, err
	}
	snapshot_b, err := createMySnapshotFromData(split[1])
	if err != nil {
		return nil, err
	}

	return backend.NewComposedSnapshot([]backend.Snapshot{snapshot_a, snapshot_b}), nil
}

// ---------------------------------- Tests -----------------------------------

func TestComposedSnapshot_IsSnapshot(t *testing.T) {
	var _ backend.Snapshot = backend.NewComposedSnapshot([]backend.Snapshot{})
}

func TestMyComposedDataStructure_IsSnaphotable(t *testing.T) {
	var _ backend.Snapshotable = &myComposedDataStructure{}
}

func TestMyComposedDataStructureSnapshotCanBeCreatedAndRestored(t *testing.T) {
	original := &myComposedDataStructure{}
	original.a.Set(1, []byte{1, 2, 3})
	original.b.Set(2, []byte{4, 5})
	original.a.Set(3, []byte{7, 8, 9})

	snapshot, err := original.CreateSnapshot()
	if err != nil {
		t.Errorf("failed to create snapshot: %v", err)
		return
	}
	if snapshot == nil {
		t.Errorf("failed to create snapshot")
		return
	}

	// clear the original data structure, to eliminate the old copy.
	original.a.data = [][]byte{}
	original.b.data = [][]byte{}

	recovered := &myComposedDataStructure{}
	if err := recovered.Restore(snapshot.GetData()); err != nil {
		t.Errorf("failed to sync to snapshot: %v", err)
		return
	}

	common.AssertArraysEqual(t, recovered.a.Get(1), []byte{1, 2, 3})
	common.AssertArraysEqual(t, recovered.b.Get(2), []byte{4, 5})
	common.AssertArraysEqual(t, recovered.a.Get(3), []byte{7, 8, 9})

	if err := snapshot.Release(); err != nil {
		t.Errorf("failed to release snapshot: %v", err)
	}
}

func TestMyComposedDataStructureSnapshotCanBeCreatedAndValidated(t *testing.T) {
	structure := &myComposedDataStructure{}
	structure.a.Set(1, []byte{1, 2, 3})
	structure.b.Set(2, []byte{4, 5})
	structure.a.Set(3, []byte{7, 8, 9})

	snapshot, err := structure.CreateSnapshot()
	if err != nil {
		t.Errorf("failed to create snapshot: %v", err)
		return
	}
	if snapshot == nil {
		t.Errorf("failed to create snapshot")
		return
	}

	remote, err := createCompositeDataSnapshotFromData(snapshot.GetData())
	if err != nil {
		t.Fatalf("failed to create snapshot from snapshot data: %v", err)
	}

	// Test direct and serialized snapshot data access.
	for _, cur := range []backend.Snapshot{snapshot, remote} {

		// The root proof should be equivalent.
		want, err := structure.GetProof()
		if err != nil {
			t.Errorf("failed to get root proof from data structure")
		}

		have := cur.GetRootProof()
		if !want.Equal(have) {
			t.Errorf("root proof of snapshot does not match proof of data structure: %v vs %v", want, have)
		}

		verifier, err := structure.GetSnapshotVerifier(cur.GetData())
		if err != nil {
			t.Fatalf("failed to obtain snapshot verifier")
		}

		if proof, err := verifier.VerifyRootProof(cur.GetData()); err != nil || !proof.Equal(want) {
			t.Errorf("snapshot invalid, inconsistent proofs: %v, want %v, got %v", err, want, proof)
		}

		// Verify all pages
		for i := 0; i < cur.GetNumParts(); i++ {
			want, err := cur.GetProof(i)
			if err != nil {
				t.Errorf("failed to fetch proof of part %d", i)
			}
			part, err := cur.GetPart(i)
			if err != nil || part == nil {
				t.Errorf("failed to fetch part %d", i)
			}
			if part != nil && verifier.VerifyPart(i, want.ToBytes(), part.ToBytes()) != nil {
				t.Errorf("failed to verify content of part %d", i)
			}
		}
	}

	if err := remote.Release(); err != nil {
		t.Errorf("failed to release remote snapshot: %v", err)
	}
	if err := snapshot.Release(); err != nil {
		t.Errorf("failed to release snapshot: %v", err)
	}
}

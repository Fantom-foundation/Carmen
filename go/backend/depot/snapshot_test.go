package depot_test

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/backend/depot"
	"github.com/Fantom-foundation/Carmen/go/backend/depot/memory"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend"
)

func TestDepotProof_IsProof(t *testing.T) {
	var _ backend.Proof = &depot.DepotProof{}
}

func TestDepotPart_IsPart(t *testing.T) {
	var _ backend.Part = &depot.DepotPart{}
}

func TestDepotSnapshot_IsSnapshot(t *testing.T) {
	var _ backend.Snapshot = &depot.DepotSnapshot{}
}

func TestDepotSnapshot_MyDepotIsSnapshotable(t *testing.T) {
	myDepot, err := memory.NewDepot[uint32](32, hashtree.GetNoHashFactory())
	if err != nil {
		t.Fatal(err)
	}
	var _ backend.Snapshotable = myDepot
}

func fillDepot(t *testing.T, depot depot.Depot[uint32], size int) {
	for i := 0; i < size; i++ {
		err := depot.Set(uint32(i), []byte{byte(i), byte(i >> 8), byte(i >> 16)})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func checkDepotContent(t *testing.T, depot depot.Depot[uint32], size int) {
	for i := 0; i < size; i++ {
		if val, err := depot.Get(uint32(i)); err != nil || !bytes.Equal(val, []byte{byte(i), byte(i >> 8), byte(i >> 16)}) {
			t.Errorf("invalid value at position %d", i)
		}
	}
}

func TestDepotSnapshot_MyDepotSnapshotCanBeCreatedAndRestored(t *testing.T) {
	const branchingFactor = 3
	const hashItems = 2
	for _, size := range []int{0, 1, 5, 1000} {

		hashTree := htmemory.CreateHashTreeFactory(branchingFactor)
		original, err := memory.NewDepot[uint32](hashItems, hashTree)
		if err != nil {
			t.Fatal(err)
		}

		fillDepot(t, original, size)
		originalProof, err := original.GetProof()
		if err != nil {
			t.Errorf("failed to produce a proof for the original state")
		}

		snapshot, err := original.CreateSnapshot()
		if err != nil {
			t.Errorf("failed to create snapshot: %v", err)
			return
		}
		if snapshot == nil {
			t.Errorf("failed to create snapshot")
			return
		}

		if !originalProof.Equal(snapshot.GetRootProof()) {
			t.Errorf("snapshot proof does not match data structure proof")
		}

		hashTreeRec := htmemory.CreateHashTreeFactory(branchingFactor)
		recovered, err := memory.NewDepot[uint32](hashItems, hashTreeRec)
		if err != nil {
			t.Fatal(err)
		}

		if err := recovered.Restore(snapshot.GetData()); err != nil {
			t.Errorf("failed to sync to snapshot: %v", err)
			return
		}

		recoveredProof, err := recovered.GetProof()
		if err != nil {
			t.Errorf("failed to produce a proof for the recovered state")
		}

		if !recoveredProof.Equal(snapshot.GetRootProof()) {
			t.Errorf("snapshot proof does not match recovered proof")
		}

		checkDepotContent(t, recovered, size)

		if err := snapshot.Release(); err != nil {
			t.Errorf("failed to release snapshot: %v", err)
		}
	}
}

func TestDepotSnapshot_MyDepotSnapshotIsShieldedFromMutations(t *testing.T) {
	const branchingFactor = 3
	const hashItems = 2
	hashTree := htmemory.CreateHashTreeFactory(branchingFactor)
	original, err := memory.NewDepot[uint32](hashItems, hashTree)
	if err != nil {
		t.Fatal(err)
	}
	fillDepot(t, original, 20)
	originalProof, err := original.GetProof()
	if err != nil {
		t.Errorf("failed to produce a proof for the original state")
	}

	snapshot, err := original.CreateSnapshot()
	if err != nil {
		t.Errorf("failed to create snapshot: %v", err)
		return
	}
	if snapshot == nil {
		t.Errorf("failed to create snapshot")
		return
	}

	// Additional mutations of the original should not be affected.
	original.Set(15, []byte{0xaa})

	if !originalProof.Equal(snapshot.GetRootProof()) {
		t.Errorf("snapshot proof does not match data structure proof")
	}

	hashTreeRec := htmemory.CreateHashTreeFactory(branchingFactor)
	recovered, err := memory.NewDepot[uint32](hashItems, hashTreeRec)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Restore(snapshot.GetData()); err != nil {
		t.Errorf("failed to sync to snapshot: %v", err)
		return
	}

	if val, err := recovered.Get(15); err != nil || !bytes.Equal(val, []byte{15, 0, 0}) {
		t.Errorf("recovered state should not include elements added after snapshot creation")
	}

	if err := snapshot.Release(); err != nil {
		t.Errorf("failed to release snapshot: %v", err)
	}
}

func TestDepotSnapshot_MyDepotSnapshotCanBeCreatedAndValidated(t *testing.T) {
	const branchingFactor = 3
	const hashItems = 2
	for _, size := range []int{0, 1, 5, 1000, 100000} {
		hashTree := htmemory.CreateHashTreeFactory(branchingFactor)
		original, err := memory.NewDepot[uint32](hashItems, hashTree)
		if err != nil {
			t.Fatal(err)
		}
		fillDepot(t, original, size)

		snapshot, err := original.CreateSnapshot()
		if err != nil {
			t.Errorf("failed to create snapshot: %v", err)
			return
		}
		if snapshot == nil {
			t.Errorf("failed to create snapshot")
			return
		}

		remote, err := depot.CreateDepotSnapshotFromData(snapshot.GetData())
		if err != nil {
			t.Fatalf("failed to create snapshot from snapshot data: %v", err)
		}

		// Test direct and serialized snapshot data access.
		for _, cur := range []backend.Snapshot{snapshot, remote} {

			// The root proof should be equivalent.
			want, err := original.GetProof()
			if err != nil {
				t.Errorf("failed to get root proof from data structure")
			}

			have := cur.GetRootProof()
			if !want.Equal(have) {
				t.Errorf("root proof of snapshot does not match proof of data structure")
			}

			metadata, err := cur.GetData().GetMetaData()
			if err != nil {
				t.Fatalf("failed to obtain metadata from snapshot")
			}

			verifier, err := original.GetSnapshotVerifier(metadata)
			if err != nil {
				t.Fatalf("failed to obtain snapshot verifier")
			}

			if proof, err := verifier.VerifyRootProof(cur.GetData()); err != nil || !proof.Equal(want) {
				t.Errorf("snapshot invalid, inconsistent proofs")
			}

			// Verify all pages
			for i := 0; i < cur.GetNumParts(); i++ {
				want, err := cur.GetProof(i)
				if err != nil {
					t.Errorf("failed to fetch proof of part %d; %s", i, err)
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
}

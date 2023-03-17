package depot

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestDepotProof_IsProof(t *testing.T) {
	var _ backend.Proof = &DepotProof{}
}

func TestDepotPart_IsPart(t *testing.T) {
	var _ backend.Part = &DepotPart{}
}

func TestDepotSnapshot_IsSnapshot(t *testing.T) {
	var _ backend.Snapshot = &DepotSnapshot{}
}

const myBranchingFactor = 16

// myDepot implements a simple depot to test and demonstrate the snapshotting on depots.
type myDepot struct {
	pages [][32][]byte
}

func (s *myDepot) Get(pos int) []byte {
	pageId := pos / 32
	if pos < 0 || pageId >= len(s.pages) {
		return []byte{}
	}
	// Return a copy of the data to avoid mutation.
	data := s.pages[pageId][pos%32]
	res := make([]byte, len(data))
	copy(res, data)
	return res
}

func (s *myDepot) Set(pos int, value []byte) {
	if pos < 0 {
		return
	}
	if s.pages == nil {
		s.pages = [][32][]byte{}
	}
	pageId := pos / 32
	for len(s.pages) <= pageId {
		s.pages = append(s.pages, [32][]byte{})
	}
	// Store a copy of the value, to avoid mutation.
	trg := make([]byte, len(value))
	copy(trg, value)
	s.pages[pageId][pos%32] = trg
}

func (s *myDepot) getHash() common.Hash {
	hashTree := htmemory.CreateHashTreeFactory(myBranchingFactor).Create(s)
	for i := 0; i < len(s.pages); i++ {
		hashTree.MarkUpdated(i)
	}
	hash, err := hashTree.HashRoot()
	if err != nil {
		panic(fmt.Sprintf("failed to compute hash of pages: %v", err))
	}
	return hash
}

func (s *myDepot) GetPage(page int) ([]byte, error) {
	res := []byte{}
	for _, value := range s.pages[page] {
		res = binary.LittleEndian.AppendUint32(res, uint32(len(value)))
	}
	for _, value := range s.pages[page] {
		res = append(res, value...)
	}
	return res, nil
}

func (s *myDepot) GetProof() (backend.Proof, error) {
	return &DepotProof{s.getHash()}, nil
}

func (s *myDepot) CreateSnapshot() (backend.Snapshot, error) {
	hash := s.getHash()

	// Note: this is a shallow copy, exploiting the immutable nature of data.
	copyOfPages := make([][32][]byte, len(s.pages))
	copy(copyOfPages, s.pages)

	return CreateDepotSnapshotFromDepot(
		myBranchingFactor,
		hash,
		len(s.pages),
		&myDepotSnapshotSource{hash, copyOfPages}), nil
}

func (s *myDepot) Restore(data backend.SnapshotData) error {
	snapshot, err := CreateDepotSnapshotFromData(data)
	if err != nil {
		return err
	}

	// Reset the depot.
	s.pages = s.pages[0:0]

	for i := 0; i < snapshot.GetNumParts(); i++ {
		part, err := snapshot.GetPart(i)
		if err != nil {
			return err
		}
		depotPart, ok := part.(*DepotPart)
		if !ok {
			return fmt.Errorf("invalid part format encountered")
		}
		for j, value := range depotPart.GetValues() {
			s.Set(i*32+j, value)
		}
	}
	return nil
}

func (s *myDepot) GetSnapshotVerifier([]byte) (backend.SnapshotVerifier, error) {
	return CreateDepotSnapshotVerifier(), nil
}

type myDepotSnapshotSource struct {
	// The hash at the time the snapshot was created.
	hash common.Hash
	// A shallow copy of the depot data at snapshot creation.
	pages [][32][]byte
}

func (s *myDepotSnapshotSource) GetHash(page int) (common.Hash, error) {
	if page < 0 || page >= len(s.pages) {
		return common.Hash{}, fmt.Errorf("invalid page number, not covered by snapshot")
	}

	h := sha256.New()
	for _, value := range s.pages[page] {
		buffer := [4]byte{}
		binary.LittleEndian.AppendUint32(buffer[0:0], uint32(len(value)))
		h.Write(buffer[:])
	}
	for _, value := range s.pages[page] {
		h.Write(value)
	}
	var hash common.Hash
	h.Sum(hash[0:0])
	return hash, nil
}

func (s *myDepotSnapshotSource) GetValues(page int) ([][]byte, error) {
	if page < 0 || page >= len(s.pages) {
		return nil, fmt.Errorf("invalid page number, not covered by snapshot")
	}
	return s.pages[page][:], nil
}

func (i *myDepotSnapshotSource) Release() error {
	// nothing to do
	return nil
}

func TestDepotSnapshot_MyDepotIsSnapshotable(t *testing.T) {
	var _ backend.Snapshotable = &myDepot{}
}

func fillDepot(t *testing.T, depot *myDepot, size int) {
	for i := 0; i < size; i++ {
		depot.Set(i, []byte{byte(i), byte(i >> 8), byte(i >> 16)})
	}
}

func checkDepotContent(t *testing.T, depot *myDepot, size int) {
	for i := 0; i < size; i++ {
		if !bytes.Equal(depot.Get(i), []byte{byte(i), byte(i >> 8), byte(i >> 16)}) {
			t.Errorf("invalid value at position %d", i)
		}
	}
}

func TestDepotSnapshot_MyDepotSnapshotCanBeCreatedAndRestored(t *testing.T) {
	for _, size := range []int{0, 1, 5, 1000} {
		original := &myDepot{}
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

		recovered := &myDepot{}
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
	original := &myDepot{}
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

	recovered := &myDepot{}
	if err := recovered.Restore(snapshot.GetData()); err != nil {
		t.Errorf("failed to sync to snapshot: %v", err)
		return
	}

	if !bytes.Equal(recovered.Get(15), []byte{15, 0, 0}) {
		t.Errorf("recovered state should not include elements added after snapshot creation")
	}

	if err := snapshot.Release(); err != nil {
		t.Errorf("failed to release snapshot: %v", err)
	}
}

func TestDepotSnapshot_MyDepotSnapshotCanBeCreatedAndValidated(t *testing.T) {
	for _, size := range []int{0, 1, 5, 1000, 100000} {
		original := &myDepot{}
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

		remote, err := CreateDepotSnapshotFromData(snapshot.GetData())
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
}

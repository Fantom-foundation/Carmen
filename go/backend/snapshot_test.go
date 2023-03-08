package backend_test

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// This test file contains an example implementation of a data structure
// (myDataStructure) supporting snapshots. The snapshot type is mySnapshot,
// the parts are of type myPart, and the proofs are of type myProof.

// ---------------------------------- Proof -----------------------------------

type myProof struct {
	hash common.Hash
}

func (p *myProof) Equal(other backend.Proof) bool {
	trg, ok := other.(*myProof)
	return ok && trg.hash == p.hash
}

func (p *myProof) ToBytes() []byte {
	return p.hash[:]
}

func getProofFor(data []byte) myProof {
	hash := sha256.New()
	hash.Write(data)
	var res common.Hash
	hash.Sum(res[:])
	return myProof{res}
}

// ----------------------------------- Part -----------------------------------

type myPart struct {
	proof *myProof
	data  []byte
}

func (p *myPart) GetProof() backend.Proof {
	return p.proof
}

func (p *myPart) Verify() bool {
	have := getProofFor(p.data)
	return p.GetProof().Equal(&have)
}

func (p *myPart) ToBytes() []byte {
	res := p.proof.ToBytes()
	res = append(res, p.data...)
	return res
}

// --------------------------------- Snapshot ---------------------------------

type mySnapshot struct {
	proof  *myProof
	size   int
	source mySnapshotDataSource
}

func (e *mySnapshot) GetRootProof() backend.Proof {
	return e.proof
}

func (e *mySnapshot) GetNumParts() int {
	return e.size
}

func (e *mySnapshot) GetProof(part_number int) (backend.Proof, error) {
	if part_number >= e.size {
		return nil, fmt.Errorf("no such part")
	}
	return e.source.GetProof(part_number)
}

func (e *mySnapshot) GetPart(part_number int) (backend.Part, error) {
	proof, err := e.GetProof(part_number)
	if err != nil {
		return nil, err
	}
	data, err := e.source.GetData(part_number)
	if err != nil {
		return nil, err
	}
	return &myPart{proof: proof.(*myProof), data: data}, nil
}

func (e *mySnapshot) VerifyRootProof() error {
	h := sha256.New()
	for i := 0; i < e.GetNumParts(); i++ {
		proof, err := e.GetProof(i)
		if err != nil {
			return err
		}
		h.Write(proof.(*myProof).ToBytes())
	}
	var hash common.Hash
	h.Sum(hash[:])
	if hash != e.proof.hash {
		return fmt.Errorf("proof validation failed")
	}
	return nil
}

func (e *mySnapshot) GetData() backend.SnapshotData {
	return e
}

func (e *mySnapshot) GetMetaData() ([]byte, error) {
	res := []byte{}
	res = append(res, byte(e.GetNumParts()))
	res = append(res, e.proof.hash[:]...)
	return res, nil
}

func (e *mySnapshot) GetProofData(part_number int) ([]byte, error) {
	proof, err := e.GetProof(part_number)
	if err != nil {
		return nil, err
	}
	return proof.ToBytes(), nil
}

func (e *mySnapshot) GetPartData(part_number int) ([]byte, error) {
	part, err := e.GetPart(part_number)
	if err != nil {
		return nil, err
	}
	return part.ToBytes(), nil
}

func (e *mySnapshot) Release() error {
	return nil // nothing to do
}

func createMySnapshotFromData(data backend.SnapshotData) (*mySnapshot, error) {
	metadata, err := data.GetMetaData()
	if err != nil {
		return nil, err
	}
	if len(metadata) != common.HashSize+1 {
		return nil, fmt.Errorf("invalid meta data encoding, invalid number of bytes")
	}
	size := int(metadata[0])
	var hash common.Hash
	copy(hash[:], metadata[1:])
	return &mySnapshot{&myProof{hash}, size, &SnapshotDataSource{data}}, nil
}

type mySnapshotDataSource interface {
	GetProof(part_number int) (*myProof, error)
	GetData(part_number int) ([]byte, error)
}

type SnapshotDataSource struct {
	snapshot backend.SnapshotData
}

func (s *SnapshotDataSource) GetProof(part_number int) (*myProof, error) {
	data, err := s.snapshot.GetProofData(part_number)
	if err != nil {
		return nil, err
	}
	if len(data) != common.HashSize {
		return nil, fmt.Errorf("invalid length of example proof")
	}
	var hash common.Hash
	copy(hash[:], data)
	return &myProof{hash}, nil
}

func (s *SnapshotDataSource) GetData(part_number int) ([]byte, error) {
	data, err := s.snapshot.GetPartData(part_number)
	if err != nil {
		return nil, err
	}
	if len(data) < common.HashSize {
		return nil, fmt.Errorf("invalid length of example part")
	}
	return data[common.HashSize:], nil
}

// ------------------------------- DataStructure ------------------------------

type myDataStructure struct {
	proofs []myProof
	data   [][]byte
}

func (e *myDataStructure) Set(pos int, data []byte) {
	for len(e.data) <= pos {
		e.data = append(e.data, nil)
		e.proofs = append(e.proofs, getProofFor(nil))
	}
	e.data[pos] = make([]byte, len(data))
	copy(e.data[pos], data)
	e.proofs[pos] = getProofFor(data)
}

func (e *myDataStructure) Get(pos int) []byte {
	if pos < 0 || pos >= len(e.data) {
		return nil
	}
	return e.data[pos]
}

func (d *myDataStructure) GetProof() (backend.Proof, error) {
	h := sha256.New()
	for _, cur := range d.proofs {
		h.Write(cur.hash[:])
	}
	var hash common.Hash
	h.Sum(hash[:])
	return &myProof{hash}, nil
}

func (d *myDataStructure) CreateSnapshot() (backend.Snapshot, error) {
	proof, err := d.GetProof()
	if err != nil {
		return nil, err
	}
	proofs := make([]myProof, len(d.proofs))
	copy(proofs, d.proofs)
	data := make([][]byte, len(d.data))
	for i, cur := range d.data {
		cpy := make([]byte, len(cur))
		copy(cpy, cur)
		data[i] = cpy
	}
	return &mySnapshot{proof.(*myProof), len(data), &myDataStructureCopy{proofs, data}}, nil
}

func (d *myDataStructure) Restore(data backend.SnapshotData) error {
	snapshot, err := createMySnapshotFromData(data)
	if err != nil {
		return err
	}

	d.data = make([][]byte, snapshot.GetNumParts())

	for i := 0; i < snapshot.GetNumParts(); i++ {
		part, err := snapshot.GetPart(i)
		if err != nil {
			return err
		}
		cpy := make([]byte, len(part.(*myPart).data))
		copy(cpy, part.(*myPart).data)
		d.data[i] = cpy
	}
	return nil
}

type myDataStructureCopy struct {
	proofs []myProof
	data   [][]byte
}

func (s *myDataStructureCopy) GetProof(part_number int) (*myProof, error) {
	if part_number < 0 || part_number >= len(s.proofs) {
		return nil, fmt.Errorf("no such part")
	}
	return &s.proofs[part_number], nil
}

func (s *myDataStructureCopy) GetData(part_number int) ([]byte, error) {
	if part_number < 0 || part_number >= len(s.proofs) {
		return nil, fmt.Errorf("no such part")
	}
	return s.data[part_number], nil
}

// ----------------------------------- Tests ----------------------------------

func TestMyDataStructureImplementsInterfaces(t *testing.T) {
	var _ backend.Proof = &myProof{}
	var _ backend.Part = &myPart{}
	var _ backend.Snapshot = &mySnapshot{}
	var _ backend.Snapshotable = &myDataStructure{}
}

func TestSnapshotCanBeCreatedAndRestored(t *testing.T) {
	original := &myDataStructure{}
	original.Set(1, []byte{1, 2, 3})
	original.Set(2, []byte{4, 5})
	original.Set(3, []byte{7, 8, 9})

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
	original.data = [][]byte{}

	recovered := &myDataStructure{}
	if err := recovered.Restore(snapshot.GetData()); err != nil {
		t.Errorf("failed to sync to snapshot: %v", err)
		return
	}

	common.AssertArraysEqual(t, recovered.Get(1), []byte{1, 2, 3})
	common.AssertArraysEqual(t, recovered.Get(2), []byte{4, 5})
	common.AssertArraysEqual(t, recovered.Get(3), []byte{7, 8, 9})

	if err := snapshot.Release(); err != nil {
		t.Errorf("failed to release snapshot: %v", err)
	}
}

func TestSnapshotCanBeCreatedAndValidated(t *testing.T) {
	structure := &myDataStructure{}
	structure.Set(1, []byte{1, 2, 3})
	structure.Set(2, []byte{4, 5})
	structure.Set(3, []byte{7, 8, 9})

	snapshot, err := structure.CreateSnapshot()
	if err != nil {
		t.Errorf("failed to create snapshot: %v", err)
		return
	}
	if snapshot == nil {
		t.Errorf("failed to create snapshot")
		return
	}

	remote, err := createMySnapshotFromData(snapshot.GetData())
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
			t.Errorf("root proof of snapshot does not match proof of data structure")
		}

		if err := cur.VerifyRootProof(); err != nil {
			t.Errorf("snapshot invalid, inconsistent proofs")
		}

		// Verify all pages
		for i := 0; i < cur.GetNumParts(); i++ {
			want, err := cur.GetProof(i)
			if err != nil {
				t.Errorf("failed to fetch proof of part %d", i)
			}
			part, err := cur.GetPart(i)
			if err != nil {
				t.Errorf("failed to fetch part %d", i)
			}
			if !want.Equal(part.GetProof()) {
				t.Errorf("proof of part does not equal proof provided by snapshot")
			}
			if !part.Verify() {
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

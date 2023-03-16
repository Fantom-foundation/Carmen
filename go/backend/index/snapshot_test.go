package index

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestIndexProof_IsProof(t *testing.T) {
	var _ backend.Proof = &IndexProof{}
}

func TestIndexPart_IsPart(t *testing.T) {
	var _ backend.Part = &IndexPart[common.Address]{}
}

func TestIndexSnapshot_IsSnapshot(t *testing.T) {
	var _ backend.Snapshot = &IndexSnapshot[common.Address]{}
}

// myIndex implements a simple index to test and demonstrate the snapshotting on indexes.
type myIndex struct {
	index map[common.Address]int
	list  []common.Address
	hash  common.Hash
	hashs []common.Hash
}

func (i *myIndex) GetOrAdd(value common.Address) int {
	if i.index == nil {
		i.index = map[common.Address]int{}
		i.list = []common.Address{}
		i.hashs = []common.Hash{}
	}
	if res, exists := i.index[value]; exists {
		return res
	}

	res := len(i.index)
	i.index[value] = res

	i.list = append(i.list, value)

	if res%GetKeysPerPart[common.Address](common.AddressSerializer{}) == 0 {
		i.hashs = append(i.hashs, i.hash)
	}

	h := sha256.New()
	h.Write(i.hash[:])
	h.Write(value[:])
	h.Sum(i.hash[0:0])

	return res
}

func (i *myIndex) Contains(value common.Address) bool {
	_, exists := i.index[value]
	return exists
}

func (i *myIndex) GetProof() (backend.Proof, error) {
	return &IndexProof{common.Hash{}, i.hash}, nil
}

func (i *myIndex) CreateSnapshot() (backend.Snapshot, error) {
	return CreateIndexSnapshotFromIndex[common.Address](
		common.AddressSerializer{},
		i.hash,
		len(i.list),
		&myIndexSnapshotSource{i, len(i.list), i.hash}), nil
}

func (mi *myIndex) Restore(data backend.SnapshotData) error {
	snapshot, err := CreateIndexSnapshotFromData[common.Address](common.AddressSerializer{}, data)
	if err != nil {
		return err
	}

	// Reset and re-initialize the index.
	mi.hash = common.Hash{}
	mi.hashs = mi.hashs[0:0]
	mi.list = mi.list[0:0]
	mi.index = map[common.Address]int{}

	for i := 0; i < snapshot.GetNumParts(); i++ {
		part, err := snapshot.GetPart(i)
		if err != nil {
			return err
		}
		indexPart, ok := part.(*IndexPart[common.Address])
		if !ok {
			return fmt.Errorf("invalid part format encountered")
		}
		for _, key := range indexPart.GetKeys() {
			mi.GetOrAdd(key)
		}
	}
	return nil
}

type myIndexSnapshotSource struct {
	// The index this snapshot is based on.
	index *myIndex
	// The number of keys at the time the snapshot was created.
	num_keys int
	// The hash at the time the snapshot was created.
	hash common.Hash
}

func (i *myIndexSnapshotSource) GetHash(key_height int) (common.Hash, error) {
	keysPerPart := GetKeysPerPart[common.Address](common.AddressSerializer{})

	if key_height == i.num_keys {
		return i.hash, nil
	}
	if key_height > i.num_keys {
		return common.Hash{}, fmt.Errorf("invalid key height, not covered by snapshot")
	}

	if key_height%keysPerPart != 0 {
		return common.Hash{}, fmt.Errorf("invalid key height, only supported at part boundaries")
	}
	return i.index.hashs[key_height/keysPerPart], nil
}

func (i *myIndexSnapshotSource) GetKeys(from, to int) ([]common.Address, error) {
	return i.index.list[from:to], nil
}

func (i *myIndexSnapshotSource) Release() error {
	// nothing to do
	return nil
}

func TestIndexSnapshot_MyIndexIsSnapshotable(t *testing.T) {
	var _ backend.Snapshotable = &myIndex{}
}

func fillIndex(t *testing.T, index *myIndex, size int) {
	for i := 0; i < size; i++ {
		if index.GetOrAdd(common.Address{byte(i), byte(i >> 8), byte(i >> 16)}) != i {
			t.Errorf("failed to add address %d", i)
		}
	}
}

func checkIndexContent(t *testing.T, index *myIndex, size int) {
	for i := 0; i < size; i++ {
		if index.GetOrAdd(common.Address{byte(i), byte(i >> 8), byte(i >> 16)}) != i {
			t.Errorf("failed to locate address %d", i)
		}
	}
}

func TestIndexSnapshot_MyIndexSnapshotCanBeCreatedAndRestored(t *testing.T) {
	for _, size := range []int{0, 1, 5, 1000} {
		original := &myIndex{}
		fillIndex(t, original, size)
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

		recovered := &myIndex{}
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

		checkIndexContent(t, recovered, size)

		if err := snapshot.Release(); err != nil {
			t.Errorf("failed to release snapshot: %v", err)
		}
	}
}

func TestIndexSnapshot_MyIndexSnapshotIsShieldedFromMutations(t *testing.T) {
	original := &myIndex{}
	fillIndex(t, original, 20)
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
	original.GetOrAdd(common.Address{0xaa})

	if !originalProof.Equal(snapshot.GetRootProof()) {
		t.Errorf("snapshot proof does not match data structure proof")
	}

	recovered := &myIndex{}
	if err := recovered.Restore(snapshot.GetData()); err != nil {
		t.Errorf("failed to sync to snapshot: %v", err)
		return
	}

	if recovered.Contains(common.Address{0xaa}) {
		t.Errorf("recovered state should not include elements added after snapshot creation")
	}

	if err := snapshot.Release(); err != nil {
		t.Errorf("failed to release snapshot: %v", err)
	}
}

func TestIndexSnapshot_MyIndexSnapshotCanBeCreatedAndValidated(t *testing.T) {
	for _, size := range []int{0, 1, 5, 1000} {
		original := &myIndex{}
		fillIndex(t, original, size)

		snapshot, err := original.CreateSnapshot()
		if err != nil {
			t.Errorf("failed to create snapshot: %v", err)
			return
		}
		if snapshot == nil {
			t.Errorf("failed to create snapshot")
			return
		}

		remote, err := CreateIndexSnapshotFromData[common.Address](common.AddressSerializer{}, snapshot.GetData())
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

			if err := cur.VerifyRootProof(); err != nil {
				t.Errorf("snapshot invalid, inconsistent proofs: %v", err)
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
				if part != nil && !part.Verify(want) {
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

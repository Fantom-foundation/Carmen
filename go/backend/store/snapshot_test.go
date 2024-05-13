// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package store

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestStoreProof_IsProof(t *testing.T) {
	var _ backend.Proof = &StoreProof{}
}

func TestStorePart_IsPart(t *testing.T) {
	var _ backend.Part = &StorePart[common.Value]{}
}

func TestStoreSnapshot_IsSnapshot(t *testing.T) {
	var _ backend.Snapshot = &StoreSnapshot[common.Value]{}
}

const myBranchingFactor = 16
const myPageItems = 32
const myPageSize = myPageItems * common.ValueSize

// myStore implements a simple store to test and demonstrate the snapshotting on stores.
type myStore struct {
	pages [][myPageSize]byte
}

func (s *myStore) Get(pos int) common.Value {
	pageId := pos / myPageItems
	if pos < 0 || pageId >= len(s.pages) {
		return common.Value{}
	}
	return *(*common.Value)(s.pages[pageId][pos%myPageItems*common.ValueSize : pos%myPageItems*common.ValueSize+common.ValueSize])
}

func (s *myStore) Set(pos int, value common.Value) {
	if pos < 0 {
		return
	}
	pageId := pos / myPageItems
	for len(s.pages) <= pageId {
		s.pages = append(s.pages, [myPageSize]byte{})
	}
	common.ValueSerializer{}.CopyBytes(value, s.pages[pageId][pos%myPageItems*common.ValueSize:(pos%myPageItems+1)*common.ValueSize])
}

func (s *myStore) getHash() common.Hash {
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

func (s *myStore) GetPage(page int) ([]byte, error) {
	return s.pages[page][:], nil
}

func (s *myStore) GetProof() (backend.Proof, error) {
	return &StoreProof{s.getHash()}, nil
}

func (s *myStore) CreateSnapshot() (backend.Snapshot, error) {
	hash := s.getHash()

	// Note: a production ready implementation would not use a deep copy here
	copyOfPages := make([][myPageSize]byte, len(s.pages))
	copy(copyOfPages, s.pages)

	return CreateStoreSnapshotFromStore[common.Value](
		common.ValueSerializer{},
		myBranchingFactor,
		hash,
		len(s.pages),
		&myStoreSnapshotSource{hash, copyOfPages}), nil
}

func (s *myStore) Restore(data backend.SnapshotData) error {
	snapshot, err := CreateStoreSnapshotFromData[common.Value](common.ValueSerializer{}, data)
	if err != nil {
		return err
	}

	// Reset the store.
	s.pages = s.pages[0:0]

	for i := 0; i < snapshot.GetNumParts(); i++ {
		part, err := snapshot.GetPart(i)
		if err != nil {
			return err
		}
		storePart, ok := part.(*StorePart[common.Value])
		if !ok {
			return fmt.Errorf("invalid part format encountered")
		}
		var partData [myPageSize]byte
		copy(partData[:], storePart.ToBytes())
		s.pages = append(s.pages, partData)
	}
	return nil
}

func (s *myStore) GetSnapshotVerifier([]byte) (backend.SnapshotVerifier, error) {
	return CreateStoreSnapshotVerifier[common.Value](common.ValueSerializer{}), nil
}

type myStoreSnapshotSource struct {
	// The hash at the time the snapshot was created.
	hash common.Hash
	// A deep copy of the store data at snapshot creation. Note, a real store
	// implementation should attemt a smarter solution using some copy-on-write
	// approch or similar.
	pages [][myPageSize]byte
}

func (s *myStoreSnapshotSource) GetHash(page int) (common.Hash, error) {
	if page < 0 || page >= len(s.pages) {
		return common.Hash{}, fmt.Errorf("invalid page number, not covered by snapshot")
	}

	h := sha256.New()
	h.Write(s.pages[page][:])
	var hash common.Hash
	h.Sum(hash[0:0])
	return hash, nil
}

func (s *myStoreSnapshotSource) GetPage(page int) ([]byte, error) {
	if page < 0 || page >= len(s.pages) {
		return nil, fmt.Errorf("invalid page number, not covered by snapshot")
	}
	return s.pages[page][:], nil
}

func (i *myStoreSnapshotSource) Release() error {
	// nothing to do
	return nil
}

func TestStoreSnapshot_MyStoreIsSnapshotable(t *testing.T) {
	var _ backend.Snapshotable = &myStore{}
}

func fillStore(t *testing.T, store *myStore, size int) {
	for i := 0; i < size; i++ {
		store.Set(i, common.Value{byte(i), byte(i >> 8), byte(i >> 16)})
	}
}

func checkStoreContent(t *testing.T, store *myStore, size int) {
	for i := 0; i < size; i++ {
		if store.Get(i) != (common.Value{byte(i), byte(i >> 8), byte(i >> 16)}) {
			t.Errorf("invalid value at position %d", i)
		}
	}
}

func TestStoreSnapshot_MyStoreSnapshotCanBeCreatedAndRestored(t *testing.T) {
	for _, size := range []int{0, 1, 5, 1000} {
		original := &myStore{}
		fillStore(t, original, size)
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

		recovered := &myStore{}
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

		checkStoreContent(t, recovered, size)

		if err := snapshot.Release(); err != nil {
			t.Errorf("failed to release snapshot: %v", err)
		}
	}
}

func TestStoreSnapshot_MyStoreSnapshotIsShieldedFromMutations(t *testing.T) {
	original := &myStore{}
	fillStore(t, original, 20)
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
	original.Set(21, common.Value{0xaa})

	if !originalProof.Equal(snapshot.GetRootProof()) {
		t.Errorf("snapshot proof does not match data structure proof")
	}

	recovered := &myStore{}
	if err := recovered.Restore(snapshot.GetData()); err != nil {
		t.Errorf("failed to sync to snapshot: %v", err)
		return
	}

	if recovered.Get(21) != (common.Value{}) {
		t.Errorf("recovered state should not include elements added after snapshot creation")
	}

	if err := snapshot.Release(); err != nil {
		t.Errorf("failed to release snapshot: %v", err)
	}
}

func TestStoreSnapshot_MyStoreSnapshotCanBeCreatedAndValidated(t *testing.T) {
	for _, size := range []int{0, 1, 5, 1000, 100000} {
		original := &myStore{}
		fillStore(t, original, size)

		snapshot, err := original.CreateSnapshot()
		if err != nil {
			t.Errorf("failed to create snapshot: %v", err)
			return
		}
		if snapshot == nil {
			t.Errorf("failed to create snapshot")
			return
		}

		remote, err := CreateStoreSnapshotFromData[common.Value](common.ValueSerializer{}, snapshot.GetData())
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

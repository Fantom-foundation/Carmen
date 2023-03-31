package pagedfile

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/array/pagedarray"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/backend/store/memsnap"
	"github.com/Fantom-foundation/Carmen/go/common"
	"os"
	"unsafe"
)

// Store is a filesystem-based store.Store implementation - it stores mapping of ID to value in binary files.
type Store[I common.Identifier, V any] struct {
	array        *pagedarray.Array[I, V]
	serializer   common.Serializer[V]
	hashTree     hashtree.HashTree
	lastSnapshot *memsnap.SnapshotSource[I, V]
}

// NewStore constructs a new instance of FileStore.
// It needs a serializer of data items and the default value for a not-set item.
func NewStore[I common.Identifier, V any](path string, serializer common.Serializer[V], pageSize int, hashtreeFactory hashtree.Factory, poolSize int) (*Store[I, V], error) {
	// create directory structure
	err := os.MkdirAll(path+"/hashes", 0700)
	if err != nil {
		return nil, fmt.Errorf("failed to create hashes directory; %s", err)
	}

	arr, err := pagedarray.NewArray[I, V](path, serializer, pageSize, poolSize)
	if err != nil {
		return nil, err
	}
	hashTree := hashtreeFactory.Create(arr)

	m := &Store[I, V]{
		array:      arr,
		serializer: serializer,
		hashTree:   hashTree,
	}

	arr.SetOnDirtyPageCallback(func(pageId int, pageBytes []byte) error {
		if m.lastSnapshot != nil && !m.lastSnapshot.Contains(pageId) { // backup into snapshot if first write into page
			oldPage := make([]byte, len(pageBytes))
			copy(oldPage, pageBytes)
			oldHash, err := m.hashTree.GetPageHash(pageId)
			if err != nil {
				return fmt.Errorf("failed to get page hash; %s", err)
			}
			err = m.lastSnapshot.AddIntoSnapshot(pageId, oldPage, oldHash)
			if err != nil {
				return fmt.Errorf("failed to add into snapshot; %s", err)
			}
		}
		hashTree.MarkUpdated(pageId)
		return nil
	})
	return m, nil
}

// Set a value of an item
func (m *Store[I, V]) Set(id I, value V) error {
	return m.array.Set(id, value)
}

// Get a value of the item (or a zero value, if not defined)
func (m *Store[I, V]) Get(id I) (value V, err error) {
	return m.array.Get(id)
}

// GetPage provides a page bytes for needs of the hash obtaining (required by PartsSource interface)
func (m *Store[I, V]) GetPage(pageId int) ([]byte, error) {
	return m.array.GetPage(pageId)
}

// GetHash provides a hash of the page (in the latest state)
func (m *Store[I, V]) GetHash(partNum int) (hash common.Hash, err error) {
	return m.hashTree.GetPageHash(partNum)
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Store[I, V]) GetStateHash() (hash common.Hash, err error) {
	// update the hashTree and get the hash
	return m.hashTree.HashRoot()
}

// GetProof returns a proof the snapshot exhibits if it is created
// for the current state of the data structure.
func (m *Store[I, V]) GetProof() (backend.Proof, error) {
	hash, err := m.GetStateHash()
	if err != nil {
		return nil, err
	}
	return store.NewProof(hash), nil
}

// CreateSnapshot creates a snapshot of the current state of the data
// structure. The snapshot should be shielded from subsequent modifications
// and be accessible until released.
func (m *Store[I, V]) CreateSnapshot() (backend.Snapshot, error) {
	branchingFactor := m.hashTree.GetBranchingFactor()
	hash, err := m.hashTree.HashRoot()
	if err != nil {
		return nil, err
	}

	newSnap := memsnap.NewSnapshotSource[I, V](m, m.lastSnapshot) // insert between the last snapshot and the store
	if m.lastSnapshot != nil {
		m.lastSnapshot.SetNextSource(newSnap) // new snapshot now follows after the former last one
	}
	m.lastSnapshot = newSnap

	snapshot := store.CreateStoreSnapshotFromStore[V](m.serializer, branchingFactor, hash, m.array.GetPagesCount(), newSnap)
	return snapshot, nil
}

// Restore restores the data structure to the given snapshot state. This
// may invalidate any former snapshots created on the data structure. In
// particular, it is not required to be able to synchronize to a former
// snapshot derived from the targeted data structure.
func (m *Store[I, V]) Restore(snapshotData backend.SnapshotData) error {
	snapshot, err := store.CreateStoreSnapshotFromData[V](m.serializer, snapshotData)
	if err != nil {
		return fmt.Errorf("unable to restore snapshot; %s", err)
	}
	if snapshot.GetBranchingFactor() != m.hashTree.GetBranchingFactor() {
		return fmt.Errorf("unable to restore snapshot - unexpected branching factor")
	}

	err = m.hashTree.Reset()
	if err != nil {
		return fmt.Errorf("unable to restore snapshot - failed to remove old hashTree; %s", err)
	}

	partsNum := snapshot.GetNumParts()
	for pageId := 0; pageId < partsNum; pageId++ {
		data, err := snapshot.GetPartData(pageId)
		if err != nil {
			return err
		}
		err = m.array.SetPage(pageId, data)
		if err != nil {
			return fmt.Errorf("unable to restore snapshot - failed to set page; %s", err)
		}
		m.hashTree.MarkUpdated(pageId)
	}

	return nil
}

func (m *Store[I, V]) ReleasePreviousSnapshot() {
	m.lastSnapshot = nil
}

func (m *Store[I, V]) GetSnapshotVerifier([]byte) (backend.SnapshotVerifier, error) {
	return store.CreateStoreSnapshotVerifier[V](m.serializer), nil
}

// Flush all changes to the disk
func (m *Store[I, V]) Flush() (err error) {
	// flush dirty pages and update the hashTree
	if _, err = m.GetStateHash(); err != nil {
		return err
	}
	// flush data file changes to disk
	if err = m.array.Flush(); err != nil {
		return err
	}
	return nil
}

// Close the store
func (m *Store[I, V]) Close() (err error) {
	if err = m.Flush(); err != nil {
		return err
	}
	return m.array.Close()
}

// GetMemoryFootprint provides the size of the store in memory in bytes
func (m *Store[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("hashTree", m.hashTree.GetMemoryFootprint())
	mf.AddChild("array", m.array.GetMemoryFootprint())
	if m.lastSnapshot != nil {
		mf.AddChild("lastSnapshot", m.lastSnapshot.GetMemoryFootprint())
	}
	return mf
}

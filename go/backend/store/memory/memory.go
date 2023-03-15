package memory

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
)

const InitialSnapshotPagesMapSize = 1024

// Store is an in-memory store.Store implementation - it maps IDs to values
type Store[I common.Identifier, V any] struct {
	data           [][]byte // data of pages [page][byte of page]
	hashTree       hashtree.HashTree
	serializer     common.Serializer[V]
	pageSize       int // the amount of bytes of one page
	pageItems      int // the amount of items stored in one page
	hashedPageSize int // the amount of the page bytes to be passed into the hashing function - rounded to whole items
	itemSize       int // the amount of bytes per one value
	lastSnapshot   *SnapshotSource[I, V]
}

// NewStore constructs a new instance of Store.
// It needs a serializer of data items and the default value for a not-set item.
func NewStore[I common.Identifier, V any](serializer common.Serializer[V], pageSize int, hashtreeFactory hashtree.Factory) (*Store[I, V], error) {
	if pageSize < serializer.Size() {
		return nil, fmt.Errorf("memory store pageSize too small (minimum %d)", serializer.Size())
	}

	itemSize := serializer.Size()
	memory := &Store[I, V]{
		data:           [][]byte{},
		serializer:     serializer,
		pageSize:       pageSize,
		pageItems:      pageSize / itemSize,
		hashedPageSize: pageSize / itemSize * itemSize,
		itemSize:       itemSize,
	}
	memory.hashTree = hashtreeFactory.Create(memory)
	return memory, nil
}

// itemPosition provides the position of an item in data pages
func (m *Store[I, V]) itemPosition(id I) (page int, position int64) {
	// casting to I for division in proper bit width
	return int(id / I(m.pageItems)), (int64(id) % int64(m.pageItems)) * int64(m.itemSize)
}

// GetPage provides the hashing page data
func (m *Store[I, V]) GetPage(pageNum int) ([]byte, error) {
	return m.data[pageNum][0:m.hashedPageSize], nil
}

// GetHash provides a hash of the page (in the latest state)
func (m *Store[I, V]) GetHash(partNum int) (hash common.Hash, err error) {
	return m.hashTree.GetPageHash(partNum)
}

// Set a value of an item
func (m *Store[I, V]) Set(id I, value V) error {
	pageNum, itemPosition := m.itemPosition(id)
	for pageNum >= len(m.data) {
		m.data = append(m.data, make([]byte, m.pageSize))
	}
	if m.lastSnapshot != nil && !m.lastSnapshot.Contains(pageNum) { // copy-on-write for snapshotting
		oldPage := m.data[pageNum]
		oldHash, err := m.hashTree.GetPageHash(pageNum)
		if err != nil {
			return err
		}
		err = m.lastSnapshot.AddIntoSnapshot(pageNum, SnapshotPart{
			data: oldPage,
			hash: oldHash,
		})
		if err != nil {
			return err
		}
		newPage := make([]byte, m.pageSize)
		copy(newPage, oldPage)
		m.data[pageNum] = newPage
	}
	copy(m.data[pageNum][itemPosition:itemPosition+int64(m.itemSize)], m.serializer.ToBytes(value))
	m.hashTree.MarkUpdated(pageNum)
	return nil
}

// Get a value of the item (or the itemDefault, if not defined)
func (m *Store[I, V]) Get(id I) (item V, err error) {
	page, itemPosition := m.itemPosition(id)
	if page < len(m.data) {
		item = m.serializer.FromBytes(m.data[page][itemPosition : itemPosition+int64(m.itemSize)])
	}
	return item, nil
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Store[I, V]) GetStateHash() (common.Hash, error) {
	return m.hashTree.HashRoot()
}

// GetProof returns a proof a snapshot would exhibit if it would be created
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
	numPages := len(m.data)
	branchingFactor := m.hashTree.GetBranchingFactor()
	hash, err := m.hashTree.HashRoot()
	if err != nil {
		return nil, err
	}

	newSnap := &SnapshotSource[I, V]{
		pages:      make(map[int]SnapshotPart, InitialSnapshotPagesMapSize),
		nextSource: m, // the Store itself is the last source of data
		prevSource: m.lastSnapshot,
	}
	if m.lastSnapshot != nil {
		m.lastSnapshot.nextSource = newSnap // new snapshot now follows after the former last one
	}
	m.lastSnapshot = newSnap

	snapshot := store.CreateStoreSnapshotFromStore[V](m.serializer, branchingFactor, hash, numPages, newSnap)
	return snapshot, nil
}

// Restore restores the data structure to the given snapshot state. This
// may invalidate any former snapshots created on the data structure. In
// particular, it is not required to be able to synchronize to a former
// snapshot derived from the targeted data structure.
func (m *Store[I, V]) Restore(data backend.SnapshotData) error {
	snapshot, err := store.CreateStoreSnapshotFromData[V](m.serializer, data)
	if err != nil {
		return err
	}
	partsNum := snapshot.GetNumParts()
	m.data = make([][]byte, partsNum)
	for i := 0; i < partsNum; i++ {
		m.data[i], err = snapshot.GetPartData(i)
		if err != nil {
			return err
		}
		m.hashTree.MarkUpdated(i)
	}
	return nil
}

// Flush the store
func (m *Store[I, V]) Flush() error {
	return nil // no-op for in-memory database
}

// Close the store
func (m *Store[I, V]) Close() error {
	return nil // no-op for in-memory database
}

func (m *Store[I, V]) ReleasePreviousSnapshot() {
	m.lastSnapshot = nil
}

// GetMemoryFootprint provides the size of the store in memory in bytes
func (m *Store[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	dataSize := uintptr(len(m.data) * m.pageSize)
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m) + dataSize)
	mf.AddChild("hashTree", m.hashTree.GetMemoryFootprint())
	if m.lastSnapshot != nil {
		mf.AddChild("lastSnapshot", m.lastSnapshot.GetMemoryFootprint())
	}
	return mf
}

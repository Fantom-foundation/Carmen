package memsnap

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

const InitialSnapshotPagesMapSize = 1024

// SnapshotSource is backend.StoreSnapshotSource implementation for in-memory store.
type SnapshotSource[I common.Identifier, V any] struct {
	pages      map[int]SnapshotPart
	nextSource PartsSource // next snapshot or (if this is the last snapshot) the store
	prevSource *SnapshotSource[I, V]
}

type SnapshotPart struct {
	data []byte
	hash common.Hash
}

type PartsSource interface {
	// GetPage provides the snapshot page data
	GetPage(pageNum int) ([]byte, error)
	// GetHash provides the snapshot page hash
	GetHash(pageNum int) (common.Hash, error)
	// ReleasePreviousSnapshot let the source know its previous snapshot was released, and it should be unreferenced
	ReleasePreviousSnapshot()
	// GetMemoryFootprint provides the memory consumption of the snapshot (and its predecessors)
	GetMemoryFootprint() *common.MemoryFootprint
}

func NewSnapshotSource[I common.Identifier, V any](nextSource PartsSource, prevSource *SnapshotSource[I, V]) *SnapshotSource[I, V] {
	return &SnapshotSource[I, V]{
		pages:      make(map[int]SnapshotPart, InitialSnapshotPagesMapSize),
		nextSource: nextSource,
		prevSource: prevSource,
	}
}

func (s *SnapshotSource[I, V]) SetNextSource(nextSource PartsSource) {
	s.nextSource = nextSource
}

// GetPage provides the content of a snapshot part
func (s *SnapshotSource[I, V]) GetPage(pageNum int) (data []byte, err error) {
	part, exists := s.pages[pageNum]
	if exists {
		return part.data, nil
	} else {
		return s.nextSource.GetPage(pageNum)
	}
}

// GetHash provides pages hashes for snapshotting
func (s *SnapshotSource[I, V]) GetHash(pageNum int) (common.Hash, error) {
	page, exists := s.pages[pageNum]
	if exists {
		return page.hash, nil
	} else {
		return s.nextSource.GetHash(pageNum)
	}
}

func (s *SnapshotSource[I, V]) AddIntoSnapshot(pageNum int, data []byte, hash common.Hash) error {
	_, contains := s.pages[pageNum]
	if contains {
		return fmt.Errorf("unable to add page into store snapshot - already present")
	}
	s.pages[pageNum] = SnapshotPart{
		data: data,
		hash: hash,
	}
	return nil
}

func (s *SnapshotSource[I, V]) Contains(pageNum int) bool {
	_, contains := s.pages[pageNum]
	return contains
}

func (s *SnapshotSource[I, V]) ReleasePreviousSnapshot() {
	s.prevSource = nil
}

// Release the snapshot data
func (s *SnapshotSource[I, V]) Release() error {
	s.nextSource.ReleasePreviousSnapshot()
	return nil
}

func (s *SnapshotSource[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	size := unsafe.Sizeof(*s)
	var pagesKey int
	if len(s.pages) > 0 {
		pagesValueSize := unsafe.Sizeof(s.pages[0]) + uintptr(len(s.pages[0].data))
		size += uintptr(len(s.pages)) * (pagesValueSize + unsafe.Sizeof(pagesKey))
	}
	mf := common.NewMemoryFootprint(size)
	if s.prevSource != nil {
		mf.AddChild("prevSource", s.prevSource.GetMemoryFootprint())
	}
	return mf
}

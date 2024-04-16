//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package memsnap

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

const InitialSnapshotPagesMapSize = 1024

// SnapshotSource is backend.StoreSnapshotSource implementation for in-memory store.
type SnapshotSource struct {
	pages      map[int]SnapshotPart
	nextSource PartsSource // next snapshot or (if this is the last snapshot) the store
	prevSource *SnapshotSource
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

func NewSnapshotSource(nextSource PartsSource, prevSource *SnapshotSource) *SnapshotSource {
	return &SnapshotSource{
		pages:      make(map[int]SnapshotPart, InitialSnapshotPagesMapSize),
		nextSource: nextSource,
		prevSource: prevSource,
	}
}

func (s *SnapshotSource) SetNextSource(nextSource PartsSource) {
	s.nextSource = nextSource
}

// GetPage provides the content of a snapshot part
func (s *SnapshotSource) GetPage(pageNum int) (data []byte, err error) {
	part, exists := s.pages[pageNum]
	if exists {
		return part.data, nil
	} else {
		return s.nextSource.GetPage(pageNum)
	}
}

// GetHash provides pages hashes for snapshotting
func (s *SnapshotSource) GetHash(pageNum int) (common.Hash, error) {
	page, exists := s.pages[pageNum]
	if exists {
		return page.hash, nil
	} else {
		return s.nextSource.GetHash(pageNum)
	}
}

func (s *SnapshotSource) AddIntoSnapshot(pageNum int, data []byte, hash common.Hash) error {
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

func (s *SnapshotSource) Contains(pageNum int) bool {
	_, contains := s.pages[pageNum]
	return contains
}

func (s *SnapshotSource) ReleasePreviousSnapshot() {
	s.prevSource = nil
}

// Release the snapshot data
func (s *SnapshotSource) Release() error {
	if s.prevSource != nil {
		return fmt.Errorf("unable to release snapshot - older snapshot must be released first")
	}
	s.nextSource.ReleasePreviousSnapshot()
	return nil
}

func (s *SnapshotSource) GetMemoryFootprint() *common.MemoryFootprint {
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

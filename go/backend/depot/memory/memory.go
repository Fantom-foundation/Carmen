//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.
//

package memory

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/depot"
	"github.com/Fantom-foundation/Carmen/go/backend/memsnap"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
)

const LengthSize = 4 // uint32

// Depot is an in-memory store.Depot implementation - it maps IDs to values
type Depot[I common.Identifier] struct {
	data         [][]byte // data of pages [item][byte of item]
	hashTree     hashtree.HashTree
	groupSize    int // the amount of items in one hashing group
	lastSnapshot *memsnap.SnapshotSource
}

// NewDepot constructs a new instance of Depot.
// It needs a serializer of data items and the default value for a not-set item.
func NewDepot[I common.Identifier](groupSize int, hashtreeFactory hashtree.Factory) (*Depot[I], error) {
	if groupSize <= 0 || hashtreeFactory == nil {
		return nil, fmt.Errorf("depot parameters invalid")
	}

	m := &Depot[I]{
		data:      [][]byte{},
		groupSize: groupSize,
	}
	m.hashTree = hashtreeFactory.Create(m)
	return m, nil
}

// itemGroup provides the hash group into which belongs the item
func (m *Depot[I]) itemGroup(id I) (page int) {
	// casting to I for division in proper bit width
	return int(id / I(m.groupSize))
}

// GetPage provides all data of one hashing group in a byte slice
func (m *Depot[I]) GetPage(hashGroup int) (out []byte, err error) {
	start := m.groupSize * hashGroup
	end := start + m.groupSize
	if end > len(m.data) {
		end = len(m.data)
	}
	outLen := m.groupSize * LengthSize
	for i := start; i < end; i++ {
		outLen += len(m.data[i])
	}
	out = make([]byte, outLen)
	outIt := 0
	for i := start; i < start+m.groupSize; i++ {
		if i < end {
			binary.LittleEndian.PutUint32(out[outIt:], uint32(len(m.data[i])))
		}
		outIt += LengthSize
	}
	for i := start; i < end; i++ {
		copy(out[outIt:], m.data[i])
		outIt += len(m.data[i])
	}
	return
}

// setPage sets data from the page exported using GetPage method into the depot
func (m *Depot[I]) setPage(hashGroup int, data []byte) (err error) {
	lengths := make([]int, m.groupSize)
	totalLength := 0
	if len(data) < m.groupSize*LengthSize {
		return fmt.Errorf("unable to set depot page - data (len %d) is not long enough to contain all lengths (expected %d)", len(data), m.groupSize*LengthSize)
	}
	for i := 0; i < m.groupSize; i++ {
		length := int(binary.LittleEndian.Uint32(data))
		lengths[i] = length
		totalLength += length
		data = data[LengthSize:]
	}
	if len(data) != totalLength {
		return fmt.Errorf("unable to set depot page - incosistent data length (data len %d, expected len %d)", len(data), totalLength)
	}
	pageStart := hashGroup * m.groupSize
	for i := 0; i < m.groupSize; i++ {
		if err := m.Set(I(pageStart+i), data[:lengths[i]]); err != nil {
			return err
		}
		data = data[lengths[i]:]
	}
	m.hashTree.MarkUpdated(hashGroup)
	return nil
}

// GetHash provides a hash of the page (in the latest state)
func (m *Depot[I]) GetHash(partNum int) (hash common.Hash, err error) {
	return m.hashTree.GetPageHash(partNum)
}

// Set a value of an item
func (m *Depot[I]) Set(id I, value []byte) error {
	for int(id) >= len(m.data) {
		m.data = append(m.data, nil)
	}
	pageNum := m.itemGroup(id)
	if m.lastSnapshot != nil && !m.lastSnapshot.Contains(pageNum) { // copy-on-write for snapshotting
		oldPage, err := m.GetPage(pageNum)
		if err != nil {
			return err
		}
		oldHash, err := m.hashTree.GetPageHash(pageNum)
		if err != nil {
			return err
		}
		err = m.lastSnapshot.AddIntoSnapshot(pageNum, oldPage, oldHash)
		if err != nil {
			return err
		}
	}
	newValue := make([]byte, len(value))
	copy(newValue, value)
	m.data[id] = newValue
	m.hashTree.MarkUpdated(m.itemGroup(id))
	return nil
}

// Get a value of the item (or nil if not defined)
func (m *Depot[I]) Get(id I) (out []byte, err error) {
	if int(id) < len(m.data) {
		out = m.data[id]
	}
	return
}

// GetSize of the item (or 0 if not defined)
func (m *Depot[I]) GetSize(id I) (int, error) {
	value, err := m.Get(id)
	return len(value), err
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Depot[I]) GetStateHash() (common.Hash, error) {
	return m.hashTree.HashRoot()
}

// GetProof returns a proof the snapshot exhibits if it is created
// for the current state of the data structure.
func (m *Depot[I]) GetProof() (backend.Proof, error) {
	hash, err := m.GetStateHash()
	if err != nil {
		return nil, err
	}
	return depot.NewProof(hash), nil
}

func (m *Depot[I]) getPagesCount() int {
	numPages := len(m.data) / m.groupSize
	if len(m.data)%m.groupSize != 0 {
		numPages++
	}
	return numPages
}

// CreateSnapshot creates a snapshot of the current state of the data
// structure. The snapshot should be shielded from subsequent modifications
// and be accessible until released.
func (m *Depot[I]) CreateSnapshot() (backend.Snapshot, error) {
	branchingFactor := m.hashTree.GetBranchingFactor()
	hash, err := m.hashTree.HashRoot()
	if err != nil {
		return nil, err
	}

	newSnap := memsnap.NewSnapshotSource(m, m.lastSnapshot) // insert between the last snapshot and the store
	if m.lastSnapshot != nil {
		m.lastSnapshot.SetNextSource(newSnap) // new snapshot now follows after the former last one
	}
	m.lastSnapshot = newSnap

	snapshot := depot.CreateDepotSnapshotFromDepot(branchingFactor, hash, m.getPagesCount(), newSnap)
	return snapshot, nil
}

// Restore restores the data structure to the given snapshot state. This
// may invalidate any former snapshots created on the data structure. In
// particular, it is not required to be able to synchronize to a former
// snapshot derived from the targeted data structure.
func (m *Depot[I]) Restore(snapshotData backend.SnapshotData) error {
	snapshot, err := depot.CreateDepotSnapshotFromData(snapshotData)
	if err != nil {
		return fmt.Errorf("unable to restore snapshot; %s", err)
	}
	if snapshot.GetBranchingFactor() != m.hashTree.GetBranchingFactor() {
		return fmt.Errorf("unable to restore snapshot - unexpected branching factor %d (expected %d)", snapshot.GetBranchingFactor(), m.hashTree.GetBranchingFactor())
	}
	partsNum := snapshot.GetNumParts()

	m.data = make([][]byte, partsNum)
	m.lastSnapshot = nil
	if err := m.hashTree.Reset(); err != nil {
		return fmt.Errorf("unable to restore snapshot - failed to remove old hashTree; %s", err)
	}

	for i := 0; i < partsNum; i++ {
		data, err := snapshot.GetPartData(i)
		if err != nil {
			return err
		}
		if err = m.setPage(i, data); err != nil {
			return err
		}
	}
	return nil
}

func (m *Depot[I]) GetSnapshotVerifier([]byte) (backend.SnapshotVerifier, error) {
	return depot.CreateDepotSnapshotVerifier(), nil
}

// Flush the depot
func (m *Depot[I]) Flush() error {
	return nil // no-op for in-memory database
}

// Close the depot
func (m *Depot[I]) Close() error {
	return nil // no-op for in-memory database
}

func (m *Depot[I]) ReleasePreviousSnapshot() {
	m.lastSnapshot = nil
}

// GetMemoryFootprint provides the size of the depot in memory in bytes
func (m *Depot[I]) GetMemoryFootprint() *common.MemoryFootprint {
	size := unsafe.Sizeof(*m)
	for _, d := range m.data {
		size += uintptr(len(d))
	}
	mf := common.NewMemoryFootprint(size)
	mf.AddChild("hashTree", m.hashTree.GetMemoryFootprint())
	if m.lastSnapshot != nil {
		mf.AddChild("lastSnapshot", m.lastSnapshot.GetMemoryFootprint())
	}
	return mf
}

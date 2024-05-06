// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package ldb

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/depot"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/backend/memsnap"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"unsafe"
)

const LengthSize = 4 // uint32

// Depot is an LevelDB backed store.Depot implementation
type Depot[I common.Identifier] struct {
	db              backend.LevelDB
	table           backend.TableSpace
	hashTree        hashtree.HashTree
	indexSerializer common.Serializer[I]
	groupSize       int // the amount of items in one hashing group
	lastSnapshot    *memsnap.SnapshotSource
	pagesCount      int // the amount of depot pages
}

// NewDepot constructs a new instance of Depot.
func NewDepot[I common.Identifier](db backend.LevelDB,
	table backend.TableSpace,
	indexSerializer common.Serializer[I],
	hashtreeFactory hashtree.Factory,
	groupSize int,
) (*Depot[I], error) {
	m := &Depot[I]{
		db:              db,
		table:           table,
		indexSerializer: indexSerializer,
		groupSize:       groupSize,
	}
	m.hashTree = hashtreeFactory.Create(m)
	if err := m.loadPagesCount(); err != nil {
		return nil, err
	}
	return m, nil
}

// itemGroup provides the hashing/snapshotting group into which the item belongs
func (m *Depot[I]) itemGroup(id I) (page int) {
	// casting to I for division in proper bit width
	return int(id / I(m.groupSize))
}

// hashGroupRange provides range of data indexes of given hashing group
func (m *Depot[I]) hashGroupRange(page int) (start int, end int) {
	return m.groupSize * page, (m.groupSize * page) + m.groupSize
}

func (m *Depot[I]) GetPage(hashGroup int) ([]byte, error) {
	startKey, endKey := m.hashGroupRange(hashGroup)
	startDbKey := m.convertKey(I(startKey)).ToBytes()
	endDbKey := m.convertKey(I(endKey)).ToBytes()
	keysRange := util.Range{Start: startDbKey, Limit: endDbKey}
	iter := m.db.NewIterator(&keysRange, nil)
	defer iter.Release()

	// the output consists of values lengths prefix and the values itself
	// the slice is initialized for the prefix, values are appended as they are read
	prefixLength := m.groupSize * LengthSize
	out := make([]byte, prefixLength)
	prefixIt := 0
	for iter.Next() {
		value := iter.Value() // returned slice is valid only in this iteration
		if len(value) != 0 {
			// length is written into the output prefix section
			binary.LittleEndian.PutUint32(out[prefixIt:prefixLength], uint32(len(value)))
			// value is appended at the end of the output slice
			out = append(out, value...)
		}
		prefixIt += LengthSize
	}
	return out, iter.Error()
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

// Set a value of an item
func (m *Depot[I]) Set(id I, value []byte) error {
	pageNum := m.itemGroup(id)
	if pageNum >= m.pagesCount {
		if err := m.setPagesCount(pageNum + 1); err != nil {
			return err
		}
	}

	// copy-on-write for snapshotting
	if m.lastSnapshot != nil && !m.lastSnapshot.Contains(pageNum) {
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

	err := m.db.Put(m.convertKey(id).ToBytes(), value, nil)
	if err != nil {
		return err
	}
	m.hashTree.MarkUpdated(pageNum)
	return nil
}

// Get a value of the item (or nil if not defined)
func (m *Depot[I]) Get(id I) (out []byte, err error) {
	out, err = m.db.Get(m.convertKey(id).ToBytes(), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	return
}

// GetSize of the item (or 0 if not defined)
func (m *Depot[I]) GetSize(id I) (int, error) {
	value, err := m.Get(id)
	return len(value), err
}

// convertKey translates the Index representation of the key into a database key.
// The database key is prepended with the table space prefix, furthermore the input key is converted to bytes
// by the key serializer
func (m *Depot[I]) convertKey(idx I) backend.DbKey {
	return backend.ToDBKey(m.table, m.indexSerializer.ToBytes(idx))
}

// getPagesCountDbKey provides a database key, where should be the amount of pages in the depot stored
func getPagesCountDbKey(table backend.TableSpace) []byte {
	return []byte{byte(table), 0xC0}
}

// loadPagesCount loads the amount of pages in the depot into the pagesCount field
func (m *Depot[I]) loadPagesCount() error {
	val, err := m.db.Get(getPagesCountDbKey(m.table), nil)
	if err == leveldb.ErrNotFound {
		m.pagesCount = 0
		return nil
	}
	if err != nil {
		return err
	}
	m.pagesCount = int(binary.LittleEndian.Uint32(val))
	return nil
}

// setPagesCount allows to set pages count - to be called when a new page is created
func (m *Depot[I]) setPagesCount(count int) error {
	m.pagesCount = count
	val := binary.LittleEndian.AppendUint32(nil, uint32(count))
	return m.db.Put(getPagesCountDbKey(m.table), val, nil)
}

// GetHash provides a hash of the page (in the latest state)
func (m *Depot[I]) GetHash(partNum int) (hash common.Hash, err error) {
	return m.hashTree.GetPageHash(partNum)
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

	snapshot := depot.CreateDepotSnapshotFromDepot(branchingFactor, hash, m.pagesCount, newSnap)
	return snapshot, nil
}

// reset sets the depot into the empty state
func (m *Depot[I]) reset() error {
	m.lastSnapshot = nil

	if err := m.hashTree.Reset(); err != nil {
		return fmt.Errorf("failed to reset hashTree; %s", err)
	}

	r := util.Range{Start: []byte{byte(m.table)}, Limit: []byte{byte(m.table) + 1}}
	iter := m.db.NewIterator(&r, nil)
	defer iter.Release()

	batch := leveldb.Batch{}
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	if iter.Error() != nil {
		return iter.Error()
	}
	if err := m.db.Write(&batch, nil); err != nil {
		return fmt.Errorf("failed delete depot content; %s", err)
	}
	return nil
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

	if err := m.reset(); err != nil {
		return fmt.Errorf("unable to reset depot for restoring a snapshot; %s", err)
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
	// commit state hash root
	_, err := m.GetStateHash()
	return err
}

// Close the store
func (m *Depot[I]) Close() error {
	return m.Flush()
}

func (m *Depot[I]) ReleasePreviousSnapshot() {
	m.lastSnapshot = nil
}

// GetMemoryFootprint provides the size of the depot in memory in bytes
func (m *Depot[I]) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("hashTree", m.hashTree.GetMemoryFootprint())
	mf.AddChild("levelDb", m.db.GetMemoryFootprint())
	if m.lastSnapshot != nil {
		mf.AddChild("lastSnapshot", m.lastSnapshot.GetMemoryFootprint())
	}
	return mf
}

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

package ldb

import (
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htldb"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"unsafe"
)

// SnapshotSource is backend.StoreSnapshotSource implementation for leveldb store.
type SnapshotSource[I common.Identifier, V any] struct {
	snap  *leveldb.Snapshot
	store *Store[I, V]
}

// GetPage provides the content of a snapshot part
func (s *SnapshotSource[I, V]) GetPage(pageNum int) (data []byte, err error) {
	return s.store.getPageFromLdbReader(pageNum, s.snap)
}

// GetHash provides pages hashes for snapshotting
func (s *SnapshotSource[I, V]) GetHash(pageNum int) (common.Hash, error) {
	return htldb.GetPageHashFromLdb(s.store.table, pageNum, s.snap)
}

// Release the snapshot data
func (s *SnapshotSource[I, V]) Release() error {
	s.snap.Release()
	return nil
}

func (s *SnapshotSource[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	return common.NewMemoryFootprint(unsafe.Sizeof(*s))
}

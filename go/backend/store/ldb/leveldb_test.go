//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

package ldb

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"testing"
)

func TestFileStoreImplements(t *testing.T) {
	var s Store[uint32, common.Value]
	var _ store.Store[uint32, common.Value] = &s
	var _ io.Closer = &s
}

var (
	A = common.Value{0xAA}
	B = common.Value{0xBB}
	C = common.Value{0xCC}

	table = []byte("V")
)

const (
	BranchingFactor = 3
	PageSize        = 5 * 32
)

func TestEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	db := openStoreDb(t, tmpDir)
	s := createNewStore(t, db)

	if val, err := s.Get(10); err != nil || val != (common.Value{}) {
		t.Errorf("Result is incorrect. Res: %s, Err: %s", val, err)
	}
}

func TestBasicOperations(t *testing.T) {
	tmpDir := t.TempDir()
	db := openStoreDb(t, tmpDir)
	s := createNewStore(t, db)

	if err := s.Set(10, A); err != nil {
		t.Errorf("Error: %s", err)
	}

	if val, err := s.Get(10); err != nil || val != A {
		t.Errorf("Result is incorrect. Res: %s, Err: %s", val, err)
	}
}

func TestPages(t *testing.T) {
	tmpDir := t.TempDir()
	db := openStoreDb(t, tmpDir)
	s := createNewStore(t, db)

	serializer := common.ValueSerializer{}

	// fill-in three pages
	_ = s.Set(2, A) // page 1
	_ = s.Set(3, B)

	_ = s.Set(5, C) // page 2
	_ = s.Set(6, A)

	_ = s.Set(12, B) // page 3
	_ = s.Set(13, C)
	_ = s.Set(14, A)

	p1, _ := s.GetPage(0)

	p1Expected := make([]byte, 0)
	p1Expected = append(p1Expected, make([]byte, 32)...)      // position 0 in page
	p1Expected = append(p1Expected, make([]byte, 32)...)      // position 1 in page
	p1Expected = append(p1Expected, serializer.ToBytes(A)...) // position 2 in page
	p1Expected = append(p1Expected, serializer.ToBytes(B)...) // position 3 in page
	p1Expected = append(p1Expected, make([]byte, 32)...)      // position 4 in page

	if bytes.Compare(p1, p1Expected) != 0 {
		t.Errorf("Page is incorrect")
	}

	p2, _ := s.GetPage(1)

	p2Expected := make([]byte, 0)
	p2Expected = append(p2Expected, serializer.ToBytes(C)...) // position 0 in page
	p2Expected = append(p2Expected, serializer.ToBytes(A)...) // position 1 in page
	p2Expected = append(p2Expected, make([]byte, 32)...)      // position 2 in page
	p2Expected = append(p2Expected, make([]byte, 32)...)      // position 3 in page
	p2Expected = append(p2Expected, make([]byte, 32)...)      // position 4 in page

	if bytes.Compare(p2, p2Expected) != 0 {
		t.Errorf("Page is incorrect")
	}

	p3, _ := s.GetPage(2)

	p3Expected := make([]byte, 0)
	p3Expected = append(p3Expected, make([]byte, 32)...)      // position 1 in page
	p3Expected = append(p3Expected, make([]byte, 32)...)      // position 2 in page
	p3Expected = append(p3Expected, serializer.ToBytes(B)...) // position 3 in page
	p3Expected = append(p3Expected, serializer.ToBytes(C)...) // position 4 in page
	p3Expected = append(p3Expected, serializer.ToBytes(A)...) // position 5 in page

	if bytes.Compare(p3, p3Expected) != 0 {
		t.Errorf("Page is incorrect")
	}

	// test correct lexicografical order
	_ = s.Set(1500, A) // page 300   key hex: Little-Endian: DC 05,  Big-Endian: 05 DC
	_ = s.Set(1501, A)
	_ = s.Set(1502, A)
	_ = s.Set(1503, A)
	_ = s.Set(1504, A)

	_ = s.Set(1756, B) // page 351   key hex: Little-Endian: DC 06,  Big-Endian: DC 06

	p4, _ := s.GetPage(300)
	p4Expected := make([]byte, 0)
	p4Expected = append(p4Expected, serializer.ToBytes(A)...) // position 1 in page
	p4Expected = append(p4Expected, serializer.ToBytes(A)...) // position 2 in page
	p4Expected = append(p4Expected, serializer.ToBytes(A)...) // position 3 in page
	p4Expected = append(p4Expected, serializer.ToBytes(A)...) // position 4 in page
	p4Expected = append(p4Expected, serializer.ToBytes(A)...) // position 5 in page

	if bytes.Compare(p4, p4Expected) != 0 {
		t.Errorf("Page is incorrect")
	}

}

func TestDataPersisted(t *testing.T) {
	tmpDir := t.TempDir()
	db := openStoreDb(t, tmpDir)
	s := createNewStore(t, db)

	if err := s.Set(10, A); err != nil {
		t.Errorf("Error: %s", err)
	}

	closeDb(db, s)
	db = openStoreDb(t, tmpDir)
	s = createNewStore(t, db)

	if val, err := s.Get(10); err != nil || val != A {
		t.Errorf("Result is incorrect. Res: %s, Err: %s", val, err)
	}
}

func TestBasicHashing(t *testing.T) {
	tmpDir := t.TempDir()
	db := openStoreDb(t, tmpDir)
	s := createNewStore(t, db)

	if hash, err := s.GetStateHash(); (err != nil || hash != common.Hash{}) {
		t.Errorf("Hash does not much. Hash: %s, Err: %s", hash, err)
	}

	_ = s.Set(2, A) // page 1
	_ = s.Set(3, B)

	hashP1, err := s.GetStateHash()
	if (hashP1 == common.Hash{}) {
		t.Errorf("Hash does not change. Hash: %s, err %s", hashP1, err)
	}

	_ = s.Set(5, C) // page 2
	_ = s.Set(6, A)

	hashP2, err := s.GetStateHash()
	if hashP1 == hashP2 {
		t.Errorf("Hash does not change. Hash: %s, err %s", hashP2, err)
	}

	_ = s.Set(12, B) // page 3
	_ = s.Set(13, C)
	_ = s.Set(14, A)

	hashP3, _ := s.GetStateHash()
	if hashP2 == hashP3 {
		t.Errorf("Hash does not change. Hash: %s, err %s", hashP3, err)
	}
}

func TestInMemoryStoreSnapshotRecovery(t *testing.T) {
	tmpDir1 := t.TempDir()
	db1 := openStoreDb(t, tmpDir1)
	s1 := createNewStore(t, db1)

	err := s1.Set(1, A)
	if err != nil {
		t.Fatalf("failed to set; %s", err)
	}
	err = s1.Set(3, B)
	if err != nil {
		t.Fatalf("failed to set; %s", err)
	}
	stateHash1, err := s1.GetStateHash()
	if err != nil {
		t.Fatalf("failed to get state hash; %s", err)
	}

	snapshot1, err := s1.CreateSnapshot()
	if err != nil {
		t.Fatalf("failed to create snapshot; %s", err)
	}
	err = s1.Set(3, A) // should not be included in the snapshot
	if err != nil {
		t.Fatalf("failed to set; %s", err)
	}
	snapshot1data := snapshot1.GetData()

	tmpDir2 := t.TempDir()
	db2 := openStoreDb(t, tmpDir2)
	s2 := createNewStore(t, db2)

	err = s2.Restore(snapshot1data)
	if err != nil {
		t.Fatalf("failed to recover snapshot; %s", err)
	}

	val, err := s2.Get(1)
	if err != nil {
		t.Fatalf("failed get from new memory; %s", err)
	}
	if val != A {
		t.Errorf("value loaded from recovered store does not match")
	}
	stateHash2, err := s2.GetStateHash()
	if err != nil {
		t.Fatalf("failed to get state hash; %s", err)
	}
	if stateHash1 != stateHash2 {
		t.Errorf("recovered store hash does not match; %x != %x", stateHash1, stateHash2)
	}
}

func openStoreDb(t *testing.T, path string) *backend.LevelDbMemoryFootprintWrapper {
	db, err := backend.OpenLevelDb(path, nil)
	if err != nil {
		t.Fatalf("Cannot open DB, err: %s", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	return db
}

func closeDb[I common.Identifier, K common.Value](db *backend.LevelDbMemoryFootprintWrapper, p *Store[I, K]) {
	_ = p.Close()
	_ = db.Close()
}

func createNewStore(t *testing.T, db backend.LevelDB) *Store[uint32, common.Value] {
	hashTree := htmemory.CreateHashTreeFactory(BranchingFactor)
	s, err := NewStore[uint32, common.Value](db, backend.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, hashTree, PageSize)

	if err != nil {
		t.Fatalf("unable to create Store")
	}

	t.Cleanup(func() {
		_ = s.Close()
	})

	return s
}

package kvdb

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/backend/store/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"io"
	"os"
	"testing"
)

func TestFileStoreImplements(t *testing.T) {
	var s KVStore[common.Value]
	var _ store.Store[uint32, common.Value] = &s
	var _ io.Closer = &s
}

var (
	A = common.Value{0xAA}
	B = common.Value{0xBB}
	C = common.Value{0xCC}

	defaultItem = common.Value{0xFF}
	table       = []byte("V")
)

const (
	DbPath          = "./test_store_db"
	BranchingFactor = 3
	PageSize        = 5
)

func TestEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "leveldb-based-store-test")
	if err != nil {
		t.Fatalf("unable to create directory")
	}

	db := openDb(t, tmpDir)
	hashTree := memory.NewHashTree(BranchingFactor)
	s, err := NewStore[common.Value](db, table, common.ValueSerializer{}, &hashTree, defaultItem, PageSize)
	defer closeDb(db, s)

	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if val, err := s.Get(10); err != nil || val != defaultItem {
		t.Errorf("Result is incorrect. Res: %s, Err: %s", val, err)
	}
}

func TestBasicOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "leveldb-based-store-test")
	if err != nil {
		t.Fatalf("unable to create directory")
	}

	db := openDb(t, tmpDir)
	hashTree := memory.NewHashTree(BranchingFactor)
	s, _ := NewStore[common.Value](db, table, common.ValueSerializer{}, &hashTree, defaultItem, PageSize)
	defer closeDb(db, s)

	if err := s.Set(10, A); err != nil {
		t.Errorf("Error: %s", err)
	}

	if val, err := s.Get(10); err != nil || val != A {
		t.Errorf("Result is incorrect. Res: %s, Err: %s", val, err)
	}
}

func TestPages(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "leveldb-based-store-test")
	if err != nil {
		t.Fatalf("unable to create directory")
	}

	db := openDb(t, tmpDir)
	hashTree := memory.NewHashTree(BranchingFactor)
	serializer := common.ValueSerializer{}
	s, _ := NewStore[common.Value](db, table, serializer, &hashTree, defaultItem, PageSize)
	defer closeDb(db, s)

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

}

func TestDataPersisted(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "leveldb-based-store-test")
	if err != nil {
		t.Fatalf("unable to create directory")
	}

	db := openDb(t, tmpDir)
	hashTree := memory.NewHashTree(BranchingFactor)
	s, _ := NewStore[common.Value](db, table, common.ValueSerializer{}, &hashTree, defaultItem, PageSize)
	defer closeDb(db, s)

	if err := s.Set(10, A); err != nil {
		t.Errorf("Error: %s", err)
	}

	closeDb(db, s)
	db = openDb(t, tmpDir)
	s, _ = NewStore[common.Value](db, table, common.ValueSerializer{}, &hashTree, defaultItem, PageSize)
	defer closeDb(db, s)

	if val, err := s.Get(10); err != nil || val != A {
		t.Errorf("Result is incorrect. Res: %s, Err: %s", val, err)
	}
}

func TestBasicHashing(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "leveldb-based-store-test")
	if err != nil {
		t.Fatalf("unable to create directory")
	}

	db := openDb(t, tmpDir)
	hashTree := memory.NewHashTree(BranchingFactor)
	s, _ := NewStore[common.Value](db, table, common.ValueSerializer{}, &hashTree, defaultItem, PageSize)
	defer closeDb(db, s)

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

func openDb(t *testing.T, path string) (db *leveldb.DB) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		t.Errorf("Cannot open DB, err: %s", err)
	}

	return
}

func closeDb[K common.Value](db *leveldb.DB, p *KVStore[K]) {
	_ = p.Close()
	_ = db.Close()
}

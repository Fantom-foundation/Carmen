package kvdb

import (
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/backend/store/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
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
)

const (
	DbPath          = "./test_store_db"
	BranchingFactor = 3
	PageSize        = 5
)

func TestEmpty(t *testing.T) {
	if err := os.RemoveAll(DbPath); err != nil {
		t.Errorf("IO Error: %s", err)
	}

	hashTree := memory.NewHashTree(BranchingFactor)
	s, err := NewStore[common.Value](DbPath, common.ValueSerializer{}, &hashTree, defaultItem, PageSize)
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if val, err := s.Get(10); err != nil || val != defaultItem {
		t.Errorf("Result is incorrect. Res: %s, Err: %s", val, err)
	}
}

func TestBasicOperations(t *testing.T) {
	if err := os.RemoveAll(DbPath); err != nil {
		t.Errorf("IO Error: %s", err)
	}

	hashTree := memory.NewHashTree(BranchingFactor)
	s, _ := NewStore[common.Value](DbPath, common.ValueSerializer{}, &hashTree, defaultItem, PageSize)

	if err := s.Set(10, A); err != nil {
		t.Errorf("Error: %s", err)
	}

	if val, err := s.Get(10); err != nil || val != A {
		t.Errorf("Result is incorrect. Res: %s, Err: %s", val, err)
	}
}

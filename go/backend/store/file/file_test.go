package file

import (
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"os"
	"testing"
)

func TestFileStoreImplements(t *testing.T) {
	var s Store[common.Value]
	var _ store.Store[uint64, common.Value] = &s
	var _ io.Closer = &s
}

var (
	A = common.Value{0xAA}
	B = common.Value{0xBB}
	C = common.Value{0xCC}
)

func TestStoringIntoFileStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "file-based-store-test")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}

	defaultItem := common.Value{}
	memory, err := NewStore[common.Value](tmpDir, common.ValueSerializer{}, defaultItem, 8, 3)
	if err != nil {
		t.Fatalf("unable to create store; %s", err)
	}
	defer memory.Close()

	err = memory.Set(0, A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}
	err = memory.Set(1, B)
	if err != nil {
		t.Fatalf("failed to set B; %s", err)
	}
	err = memory.Set(2, C)
	if err != nil {
		t.Fatalf("failed to set C; %s", err)
	}

	if value, err := memory.Get(5); err != nil || value != defaultItem {
		t.Errorf("not-existing value is not reported as not-existing; err=%s", err)
	}
	if value, err := memory.Get(0); err != nil || value != A {
		t.Errorf("reading written A returned different value")
	}
	if value, err := memory.Get(1); err != nil || value != B {
		t.Errorf("reading written B returned different value")
	}
	if value, err := memory.Get(2); err != nil || value != C {
		t.Errorf("reading written C returned different value")
	}
}

func TestStoringToArbitraryPosition(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "file-based-store-test")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}

	defaultItem := common.Value{}
	memory, err := NewStore[common.Value](tmpDir, common.ValueSerializer{}, defaultItem, 8, 3)
	if err != nil {
		t.Fatalf("unable to create store; %s", err)
	}
	defer memory.Close()

	err = memory.Set(5, A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}
	err = memory.Set(4, B)
	if err != nil {
		t.Fatalf("failed to set B; %s", err)
	}
	err = memory.Set(9, C)
	if err != nil {
		t.Fatalf("failed to set C; %s", err)
	}

	if value, err := memory.Get(1); err != nil || value != defaultItem {
		t.Errorf("not-existing value is not reported as not-existing")
	}
	if value, err := memory.Get(5); err != nil || value != A {
		t.Errorf("reading written A returned different value")
	}
	if value, err := memory.Get(4); err != nil || value != B {
		t.Errorf("reading written B returned different value")
	}
	if value, err := memory.Get(9); err != nil || value != C {
		t.Errorf("reading written C returned different value")
	}
}

func TestHashingInFileStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "file-based-store-test")
	if err != nil {
		t.Fatalf("unable to create testing db directory")
	}

	defaultItem := common.Value{}
	memory, err := NewStore[common.Value](tmpDir, common.ValueSerializer{}, defaultItem, 8, 3)
	if err != nil {
		t.Fatalf("unable to create store; %s", err)
	}
	defer memory.Close()

	initialHast, err := memory.GetStateHash()
	if err != nil {
		t.Fatalf("failed to hash; %s", err)
	}

	err = memory.Set(0, A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}

	newHash, err := memory.GetStateHash()
	if err != nil {
		t.Fatalf("failed to hash; %s", err)
	}
	if initialHast == newHash {
		t.Errorf("setting into the store have not changed the hash %x %x", initialHast, newHash)
	}
}

package pagedfile

import (
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/backend/store/pagedfile/eviction"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"testing"
)

func TestFileStoreImplements(t *testing.T) {
	var s Store[uint64, common.Value]
	var _ store.Store[uint64, common.Value] = &s
	var _ io.Closer = &s
}

var (
	A = common.Value{0xAA}
	B = common.Value{0xBB}
	C = common.Value{0xCC}
)

func TestStoringIntoFileStore(t *testing.T) {
	path := t.TempDir()
	st := createStore(t, path)

	err := st.Set(0, A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}
	err = st.Set(1, B)
	if err != nil {
		t.Fatalf("failed to set B; %s", err)
	}
	err = st.Set(2, C)
	if err != nil {
		t.Fatalf("failed to set C; %s", err)
	}

	if value, err := st.Get(5); err != nil || value != (common.Value{}) {
		t.Errorf("not-existing value is not reported as not-existing; err=%s", err)
	}
	if value, err := st.Get(0); err != nil || value != A {
		t.Errorf("reading written A returned different value")
	}
	if value, err := st.Get(1); err != nil || value != B {
		t.Errorf("reading written B returned different value")
	}
	if value, err := st.Get(2); err != nil || value != C {
		t.Errorf("reading written C returned different value")
	}
}

func TestStoringToArbitraryPosition(t *testing.T) {
	path := t.TempDir()
	st := createStore(t, path)

	err := st.Set(5, A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}
	err = st.Set(4, B)
	if err != nil {
		t.Fatalf("failed to set B; %s", err)
	}
	err = st.Set(9, C)
	if err != nil {
		t.Fatalf("failed to set C; %s", err)
	}

	if value, err := st.Get(1); err != nil || value != (common.Value{}) {
		t.Errorf("not-existing value is not reported as not-existing")
	}
	if value, err := st.Get(5); err != nil || value != A {
		t.Errorf("reading written A returned different value")
	}
	if value, err := st.Get(4); err != nil || value != B {
		t.Errorf("reading written B returned different value")
	}
	if value, err := st.Get(9); err != nil || value != C {
		t.Errorf("reading written C returned different value")
	}
}

func TestHashingInFileStore(t *testing.T) {
	path := t.TempDir()
	st := createStore(t, path)

	initialHast, err := st.GetStateHash()
	if err != nil {
		t.Fatalf("failed to hash; %s", err)
	}

	err = st.Set(0, A)
	if err != nil {
		t.Fatalf("failed to set A; %s", err)
	}

	newHash, err := st.GetStateHash()
	if err != nil {
		t.Fatalf("failed to hash; %s", err)
	}
	if initialHast == newHash {
		t.Errorf("setting into the store have not changed the hash %x %x", initialHast, newHash)
	}
}

func createStore(t *testing.T, tmpDir string) store.Store[uint32, common.Value] {
	evictionPolicy := eviction.NewRandomEvictionPolicy(4)
	st, err := NewStore[uint32, common.Value](tmpDir, common.ValueSerializer{}, 8*32, 3, 4, evictionPolicy)
	if err != nil {
		t.Fatalf("unable to create st; %s", err)
	}

	t.Cleanup(func() {
		_ = st.Close()
	})

	return st
}

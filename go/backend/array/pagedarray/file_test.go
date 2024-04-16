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

package pagedarray

import (
	"github.com/Fantom-foundation/Carmen/go/backend/array"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"testing"
)

func TestPagedArrayImplements(t *testing.T) {
	var s Array[uint64, common.Value]
	var _ array.Array[uint64, common.Value] = &s
	var _ io.Closer = &s
}

var (
	A = common.Value{0xAA}
	B = common.Value{0xBB}
	C = common.Value{0xCC}
)

func TestStoreInArray(t *testing.T) {
	path := t.TempDir()
	st := createArray(t, path)

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
	st := createArray(t, path)

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

func TestStoringManyItemsIntoPagedFileStore(t *testing.T) {
	path := t.TempDir()
	st := createArray(t, path)

	for i := uint32(0); i < 1000; i++ {
		err := st.Set(i, common.Value{0x12, byte(i)})
		if err != nil {
			t.Fatalf("failed to set item %d; %s", i, err)
		}
	}

	for i := uint32(0); i < 1000; i++ {
		value, err := st.Get(i)
		if err != nil {
			t.Fatalf("failed to get item %d; %s", i, err)
		}
		if value != (common.Value{0x12, byte(i)}) {
			t.Errorf("reading item %d returns unexpected value", i)
		}
	}
}

func createArray(t *testing.T, tmpDir string) *Array[uint32, common.Value] {
	st, err := NewArray[uint32, common.Value](tmpDir, common.ValueSerializer{}, 8*32, 4)
	if err != nil {
		t.Fatalf("unable to create st; %s", err)
	}

	t.Cleanup(func() {
		_ = st.Close()
	})

	return st
}

//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package mpt

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestNodeHashes_ContainsAddedElements(t *testing.T) {
	equal := func(a, b []NodeHash) bool {
		if len(a) != len(b) {
			return false
		}
		for i, cur := range a {
			if !cur.Equal(&b[i]) {
				return false
			}
		}
		return true
	}

	list := NewNodeHashes()
	want := []NodeHash{}
	if got := list.GetHashes(); !equal(want, got) {
		t.Errorf("Invalid list content, wanted %v, got %v", want, got)
	}
	list.Add(EmptyPath(), common.Hash{0})
	want = append(want, NodeHash{EmptyPath(), common.Hash{0}})
	if got := list.GetHashes(); !equal(want, got) {
		t.Errorf("Invalid list content, wanted %v, got %v", want, got)
	}
	list.Add(EmptyPath().Child(1), common.Hash{1})
	want = append(want, NodeHash{EmptyPath().Child(1), common.Hash{1}})
	if got := list.GetHashes(); !equal(want, got) {
		t.Errorf("Invalid list content, wanted %v, got %v", want, got)
	}
	list.Add(EmptyPath().Next(), common.Hash{2})
	want = append(want, NodeHash{EmptyPath().Next(), common.Hash{2}})
	if got := list.GetHashes(); !equal(want, got) {
		t.Errorf("Invalid list content, wanted %v, got %v", want, got)
	}
}

func TestNodeHashes_RecycledListsAreEmpty(t *testing.T) {
	for i := 0; i < 100; i++ {
		list := NewNodeHashes()
		if size := len(list.GetHashes()); size != 0 {
			t.Errorf("new node list is not empty, size: %d", size)
		}
		list.Add(EmptyPath(), common.Hash{})
		if size := len(list.GetHashes()); size != 1 {
			t.Errorf("invalid size after adding one element: %d", size)
		}
		list.Release()
	}
}

func BenchmarkNodeHashes_LiveCycle(b *testing.B) {
	for i := 0; i < b.N; i++ {
		list := NewNodeHashes()
		list.Add(EmptyPath(), common.Hash{})
		list.Release()
	}
}

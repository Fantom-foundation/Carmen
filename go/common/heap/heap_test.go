// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package heap

import (
	"math/rand"
	"slices"
	"testing"
)

func TestHeap_ElementsAreSorted(t *testing.T) {
	const N = 100

	// Create a shuffled list of entries and add them to the queue.
	entries := make([]int, N)
	for i := 0; i < N; i++ {
		entries[i] = i
	}
	rand.Shuffle(len(entries), func(i, j int) {
		entries[i], entries[j] = entries[j], entries[i]
	})
	queue := New(func(a, b int) int {
		return b - a
	})
	for _, e := range entries {
		queue.Add(e)
	}

	// Pop elements from the queue and check that they are sorted.
	for i := range entries {
		b, ok := queue.Peek()
		if !ok {
			t.Fatal("expected to peek an element")
		}
		if want, got := i, b; want != got {
			t.Errorf("expected to peek element with number %d, got %v", want, got)
		}

		b, ok = queue.Pop()
		if !ok {
			t.Fatal("expected to pop an element")
		}
		if want, got := i, b; want != got {
			t.Errorf("expected to pop element with number %d, got %v", want, got)
		}
	}

	if _, ok := queue.Peek(); ok {
		t.Fatal("expected to peek no more elements")
	}

	if _, ok := queue.Pop(); ok {
		t.Fatal("expected to pop no more elements")
	}
}

func TestHeap_ZeroHeapCanBeUsedToStoreAndRetrieveElements(t *testing.T) {
	queue := Heap[int]{}

	for i := 0; i < 10; i++ {
		queue.Add(i)
	}

	retrieved := []int{}
	for cur, ok := queue.Pop(); ok; cur, ok = queue.Pop() {
		retrieved = append(retrieved, cur)
	}

	if want, got := 10, len(retrieved); want != got {
		t.Fatalf("expected to get %d elements, got %d", want, got)
	}

	slices.Sort(retrieved)
	for i, cur := range retrieved {
		if want, got := i, cur; want != got {
			t.Errorf("expected to get element %d, got %d", want, got)
		}
	}
}

func TestHeap_ContainsCanLocateElements(t *testing.T) {
	queue := Heap[int]{}

	for i := 0; i < 10; i++ {
		queue.Add(i)
	}

	for i := 0; i < 10; i++ {
		if !queue.ContainsFunc(func(cur int) bool { return cur == i }) {
			t.Fatalf("expected to find element %d", i)
		}
	}

	for i := 10; i < 15; i++ {
		if queue.ContainsFunc(func(cur int) bool { return cur == i }) {
			t.Fatalf("expected not to find element %d", i)
		}
	}

	if !queue.ContainsFunc(func(cur int) bool { return cur < 5 }) {
		t.Fatalf("expected to find element less than 5")
	}
}

// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: <TBD>
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

// Package heap provides a generic implementation of a priority queue.
package heap

import (
	"slices"
)

// Heap is a generic implementation of a priority queue. The zero value of Heap
// is an empty heap where each element has the same priority. To customize
// the priority of elements, a heap should be created using the New function.
// Heap instances are not safe for concurrent access.
type Heap[T any] struct {
	data  []T
	order func(a, b T) int
}

// New creates a new heap with the given comparison function. The peek
// of the resulting heap is the element with the highest priority with respect
// to the provided comparison function. To gain a min-heap, the comparison
// function should be inverted.
func New[T any](compare func(a, b T) int) Heap[T] {
	return Heap[T]{
		order: compare,
	}
}

// Add inserts an element into the heap. The element is placed in the
// the order of elements maintained by the heap according to its priority
// relative to other elements. The complexity of this operation is
// O(log n), where n is the number of elements in the heap.
func (q *Heap[T]) Add(element T) {
	q.data = append(q.data, element)
	for i := len(q.data) - 1; i > 0; {
		j := (i - 1) / 2
		if q.compare(q.data[j], q.data[i]) >= 0 {
			break
		}
		q.data[i], q.data[j] = q.data[j], q.data[i]
		i = j
	}
}

// Peek returns the element with the highest priority in the heap. If the
// heap is empty, a zero value is returned along with a false flag.
// The complexity of this operation is O(1).
func (q *Heap[T]) Peek() (T, bool) {
	if len(q.data) == 0 {
		var zero T
		return zero, false
	}
	return q.data[0], true
}

// Pop removes and returns the element with the highest priority in the heap.
// If the heap is empty, a zero value is returned along with a false flag.
// The complexity of this operation is O(log n), where n is the number of
// elements in the heap.
func (q *Heap[T]) Pop() (T, bool) {
	if len(q.data) == 0 {
		var zero T
		return zero, false
	}
	b := q.data[0]
	q.data[0] = q.data[len(q.data)-1]
	q.data = q.data[:len(q.data)-1]
	for i := 0; ; {
		j := 2*i + 1
		if j >= len(q.data) {
			break
		}
		if j+1 < len(q.data) && q.compare(q.data[j+1], q.data[j]) > 0 {
			j++
		}
		if q.compare(q.data[i], q.data[j]) >= 0 {
			break
		}
		q.data[i], q.data[j] = q.data[j], q.data[i]
		i = j
	}
	return b, true
}

// ContainsFunc returns true if the heap contains an element that satisfies
// the given predicate. The complexity of this operation is O(n), where n
// is the number of elements in the heap.
func (q *Heap[T]) ContainsFunc(predicate func(T) bool) bool {
	return slices.ContainsFunc(q.data, predicate)
}

func (q *Heap[T]) compare(a, b T) int {
	if q.order == nil {
		return 0
	}
	return q.order(a, b)
}

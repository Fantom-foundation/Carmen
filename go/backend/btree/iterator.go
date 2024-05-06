// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package btree

// Iterator of BTree elements
type Iterator[K any] struct {
	start, end K // start and end key to iterate
	visitChild bool
	nestStack  nestStack[K]
}

// newIterator creates a new iterator for input key range.
// The range is [start;end)
func newIterator[K any](start, end K, n node[K]) *Iterator[K] {
	startIndex, _ := n.findItem(start)
	endIndex, _ := n.findItem(end)
	it := &Iterator[K]{start, end, true, make([]nestCtx[K], 0, 100)}
	it.nestStack.push(nestCtx[K]{startIndex, endIndex, n})
	return it
}

// HasNext returns true if there is a next key within the range of this iterator
func (it *Iterator[K]) HasNext() (exists bool) {
	// have a look at current level first
	exists = it.currentLevel().currentNode.hasNext(it)

	// if there is no more items in this level, pop parent level
	if !exists && it.nestStack.size() > 1 {
		it.nestStack.pop()
		it.visitChild = false
		exists = it.HasNext()
	}

	return
}

// Next returns a next element. HasNext() should be called to find out
// if there is a next key, otherwise the returned key should not be used.
func (it *Iterator[K]) Next() (k K) {
	// have a look at current level first
	if it.HasNext() {
		k = it.currentLevel().currentNode.next(it)
	} else if it.nestStack.size() > 0 {
		// if there is no more items in this level, pop parent level
		it.nestStack.pop()
		it.visitChild = false
		k = it.Next()
	}

	return
}

func (it *Iterator[K]) currentLevel() *nestCtx[K] {
	return it.nestStack.peek()
}

func (it *Iterator[K]) pushLevel(startIndex, endIndex int, childNode node[K]) {
	it.nestStack.push(nestCtx[K]{startIndex, endIndex, childNode})
}

func (it *Iterator[K]) popLevel() {
	it.nestStack.pop()
}

// nestStack contains contexts of nested calls used while iterating the tree by the iterator
// to remember properties of parent nodes.
type nestStack[K any] []nestCtx[K]

func (s nestStack[K]) size() int {
	return len(s)
}
func (s nestStack[K]) peek() *nestCtx[K] {
	return &s[len(s)-1]
}

func (s *nestStack[K]) push(c nestCtx[K]) {
	*s = append(*s, c)
}

func (s *nestStack[K]) pop() {
	ss := *s
	*s = ss[0 : len(ss)-1]
}

// nestCtx is a nesting context using to store values for recursive iteration of a tree by the iterator.
type nestCtx[K any] struct {
	currentIndex, endIndex int
	currentNode            node[K]
}

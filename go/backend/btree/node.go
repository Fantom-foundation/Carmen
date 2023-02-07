package btree

type node[K any] interface {
	ForEacher[K]

	// insert finds an in-order position of the key and inserts it in this node.
	// When the key already exits, nothing happens.
	// When the key is inserted and its capacity does not exceed, it is added into this leaf.
	// When the capacity exceeds, this node is split into two, this one (i.e. the "left" node)
	// and a new one (i.e. the "right" node), and the keys are distributed between these two nodes.
	// Keys are split in the middle, and the middle value is returned. When the number
	// of keys is even, the right node has one key less  than the left one.
	// The right node, the middle key, and a split flag are returned.
	insert(key K) (right node[K], middle K, split bool)

	// contains returns true when the input key exists in this node or children
	contains(key K) bool

	// findItem finds a key in the list, if it exists.
	// It returns the index of the key that was found, and it returns true.
	// If the key does not exist, it returns false and the index is equal to the last
	// visited position in the list, traversed using binary search.
	// The index is increased by one when the last visited key was lower than the input key
	// so the new key may be inserted after this key.
	// It means the index can be used as a position to insert the key in the list.
	findItem(key K) (index int, exists bool)

	//next moves position of the input iterator, and returns next key using the iterator.
	next(iterator *Iterator[K]) (k K)

	//hasNext returns true if next item exits.
	hasNext(iterator *Iterator[K]) bool
}

type ForEacher[K any] interface {
	// ForEach iterates ordered keys of the node including possible children
	ForEach(callback func(k K))
}

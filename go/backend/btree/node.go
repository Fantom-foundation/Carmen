package btree

type Node[K any] interface {

	// Insert finds an in-order position of the key and inserts it in this node.
	// When the key already exits, nothing happens.
	// When the key is inserted and its capacity does not exceed, it is added into this leaf.
	// When the capacity exceeds, this node is split into two, this one (i.e. the "left" node)
	// and a new one (i.e. the "right" node), and the keys are distributed between these two nodes.
	// Keys are split in the middle, and the middle value is returned. When the number
	// of keys is even, the right node has one key less  than the left one.
	// The right node, the middle key, and a split flag are returned.
	Insert(key K) (right Node[K], middle K, split bool)

	// Contains returns true when the input key exists in this node or children
	Contains(key K) bool

	// ForEach iterates ordered keys of the node including possible children
	ForEach(callback func(k K))
}

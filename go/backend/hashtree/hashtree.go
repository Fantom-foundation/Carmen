package hashtree

import "github.com/Fantom-foundation/Carmen/go/common"

// HashTree implementation allows for computing (merkle) hash root out of set of input pages.
type HashTree interface {

	// MarkUpdated marks a page as changed to signal its hash needs to be computed
	MarkUpdated(page int)

	// HashRoot computes the hash root of the (merkle) tree.
	HashRoot() (out common.Hash, err error)

	// GetPageHash provides a hash of the tree node.
	GetPageHash(page int) (hash common.Hash, err error)

	// GetBranchingFactor provides the tree branching factor
	GetBranchingFactor() int

	// Reset removes the HashTree content
	Reset() error

	// provides the size of the hash-tree in memory in bytes
	common.MemoryFootprintProvider
}

// PageProvider is a source of pages for the HashTree
type PageProvider interface {
	GetPage(page int) ([]byte, error)
}

// Factory creates a new instance of the HashTree
type Factory interface {

	// Create creates a new instance of hash tree with given branching factor and page provider
	Create(pageProvider PageProvider) HashTree
}

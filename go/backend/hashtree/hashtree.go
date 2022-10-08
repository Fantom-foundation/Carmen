package hashtree

import "github.com/Fantom-foundation/Carmen/go/common"

// HashTree implementation allows for computing (merkle) hash root out of set of input pages.
type HashTree interface {

	// MarkUpdated marks a page as changed to signal its hash needs to be computed
	MarkUpdated(page int)

	// HashRoot computes the hash root of the (merkle) tree.
	HashRoot() (out common.Hash, err error)
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

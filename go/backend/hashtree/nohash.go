package hashtree

import "github.com/Fantom-foundation/Carmen/go/common"

// NoHash is a trivial HashTree interface implementation, which does not do any hashing.
// It returns zero hash for any store/depot content.
type NoHash struct{}

// NoHashFactory is a NoHash factory.
type NoHashFactory struct{}

// GetNoHashFactory creates a new instance of NoHash.
func GetNoHashFactory() *NoHashFactory {
	return &NoHashFactory{}
}

// Create creates a new instance of the HashTree
func (f *NoHashFactory) Create(pageProvider PageProvider) HashTree {
	return &NoHash{}
}

// MarkUpdated marks a page as changed
func (ht *NoHash) MarkUpdated(page int) {}

// HashRoot provides the hash in the root of the hashing tree
func (ht *NoHash) HashRoot() (out common.Hash, err error) {
	return common.Hash{}, nil
}

// GetMemoryFootprint provides the size of the hash-tree in memory in bytes
func (ht *NoHash) GetMemoryFootprint() uintptr {
	return 0
}

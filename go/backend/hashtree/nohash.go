// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package hashtree

import (
	"github.com/Fantom-foundation/Carmen/go/common"
)

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

// GetPageHash provides a hash of the tree node.
func (ht *NoHash) GetPageHash(page int) (hash common.Hash, err error) {
	return common.Hash{}, nil
}

// GetBranchingFactor provides the tree branching factor
func (ht *NoHash) GetBranchingFactor() int {
	return 0
}

// Reset removes the hashtree content
func (ht *NoHash) Reset() error {
	return nil
}

// GetMemoryFootprint provides the size of the hash-tree in memory in bytes
func (ht *NoHash) GetMemoryFootprint() *common.MemoryFootprint {
	return common.NewMemoryFootprint(0)
}

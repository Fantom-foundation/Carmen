//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package mpt

import (
	"sync"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// NodeHash is a utility type linking a path in an MPT to a hash. Its main
// use case is to transfer computed hashes from LiveDB instances to archives
// to avoid double-computation and thus improve efficiency.
type NodeHash struct {
	Path NodePath
	Hash common.Hash
}

// Equal returns true if the given NodeHash is the same as this one.
func (h *NodeHash) Equal(other *NodeHash) bool {
	return h == other || (h.Hash == other.Hash && h.Path.Equal(other.Path))
}

// NodeHashes provides a recyclable list of NodeHash instances. NodeHashes
// should be created using NewNodeHashes() and released by calling its Release
// method. It is not an error to fail to release an instance, but it may harm
// performance and increase memory usage temporarily.
type NodeHashes struct {
	hashes []NodeHash
}

// nodeHashPool is a pool for the repeated reuse of hash lists
var nodeHashPool = sync.Pool{New: func() any {
	return &NodeHashes{hashes: make([]NodeHash, 0, 4096)}
},
}

// NewNodeHashes obtains an empty instance to be owned by the caller.
func NewNodeHashes() *NodeHashes {
	return nodeHashPool.Get().(*NodeHashes)
}

// Add adds an entry to this node hash collection.
func (h *NodeHashes) Add(path NodePath, hash common.Hash) {
	h.hashes = append(h.hashes, NodeHash{path, hash})
}

// GetHashes retains a (shared) view on the retained hashes.
func (h *NodeHashes) GetHashes() []NodeHash {
	return h.hashes
}

// Release frees internal resources for future reuse and invalidates this object.
// Release must not be called more than once on a valid NodeHashes instance.
func (h *NodeHashes) Release() {
	h.hashes = h.hashes[:0]
	nodeHashPool.Put(h)
}

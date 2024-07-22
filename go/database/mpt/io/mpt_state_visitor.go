// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package io

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
)

// mptStateVisitor is a wrapper around Trie which determines functions necessary to export a LiveDB genesis.
type mptStateVisitor interface {
	// Visit allows for travertines the whole trie under the input root.
	Visit(visitor mpt.NodeVisitor) error
	// GetHash returns hash of the current Trie.
	GetHash() (common.Hash, error)
	// GetCodeForHash returns byte code for given hash.
	GetCodeForHash(common.Hash) []byte
}

type exportableArchiveTrie struct {
	trie  *mpt.ArchiveTrie
	block uint64
	codes map[common.Hash][]byte
}

func (e exportableArchiveTrie) Visit(visitor mpt.NodeVisitor) error {
	return e.trie.VisitTrie(e.block, visitor)
}

func (e exportableArchiveTrie) GetHash() (common.Hash, error) {
	return e.trie.GetHash(e.block)
}

func (e exportableArchiveTrie) GetCodeForHash(hash common.Hash) []byte {
	if e.codes == nil || len(e.codes) == 0 {
		e.codes = e.trie.GetCodes()
	}
	return e.codes[hash]
}

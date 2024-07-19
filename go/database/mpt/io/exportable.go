package io

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
)

type exportable interface {
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
	return e.trie.VisitTrie(visitor, e.block)
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

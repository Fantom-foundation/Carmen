package mpt

import (
	"context"
	"io"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/genesis"
	"github.com/Fantom-foundation/Carmen/go/common/interrupt"
)

// TrieVisitor is an interface for Tries that allows for visiting the Trie nodes
// and furthermore getting its properties such as a root hash and contract codes.
type TrieVisitor interface {
	// Visit allows for traverse the whole trie.
	Visit(visitor NodeVisitor) error
	// GetHash returns the hash of the represented Trie.
	GetHash() (common.Hash, error)
	// GetCodeForHash returns byte code for given hash.
	GetCodeForHash(common.Hash) []byte
}

// NewExportableArchiveTrie allows exporting LiveDB genesis for given block from an ArchiveTrie.
func NewExportableArchiveTrie(trie *ArchiveTrie, block uint64) TrieVisitor {
	return exportableArchiveTrie{trie: trie, block: block}
}

type exportableArchiveTrie struct {
	trie  *ArchiveTrie
	block uint64
	codes map[common.Hash][]byte
}

func (e exportableArchiveTrie) Visit(visitor NodeVisitor) error {
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

// ExportLive exports given db into out.
func ExportLive(ctx context.Context, db TrieVisitor, out io.Writer) (common.Hash, error) {
	// Start with the magic number.
	if _, err := out.Write(genesis.StateMagicNumber); err != nil {
		return common.Hash{}, err
	}

	// Add a version number.
	if _, err := out.Write([]byte{genesis.FormatVersion}); err != nil {
		return common.Hash{}, err
	}

	// Continue with the full state hash.
	hash, err := db.GetHash()
	if err != nil {
		return common.Hash{}, err
	}
	if _, err := out.Write([]byte{byte('H'), byte(genesis.EthereumHash)}); err != nil {
		return common.Hash{}, err
	}
	if _, err := out.Write(hash[:]); err != nil {
		return common.Hash{}, err
	}

	// Write out codes.
	codes, err := getReferencedCodes(db)
	if err != nil {
		return common.Hash{}, err
	}
	if err := genesis.WriteCodes(codes, out); err != nil {
		return common.Hash{}, err
	}

	// Write out all accounts and values.
	visitor := exportVisitor{out: out, ctx: ctx}
	if err := db.Visit(&visitor); err != nil || visitor.err != nil {
		return common.Hash{}, err
	}

	return hash, nil
}

// exportVisitor is an internal utility used by the Export function to write
// account and value node information to a given output writer.
type exportVisitor struct {
	out io.Writer
	err error
	ctx context.Context
}

func (e *exportVisitor) Visit(node Node, _ NodeInfo) VisitResponse {
	// outside call to interrupt
	if interrupt.IsCancelled(e.ctx) {
		e.err = interrupt.ErrCanceled
		return VisitResponseAbort
	}
	switch n := node.(type) {
	case *AccountNode:
		addr := n.Address()
		info := n.Info()
		if _, err := e.out.Write([]byte{byte('A')}); err != nil {
			e.err = err
			return VisitResponseAbort
		}
		if _, err := e.out.Write(addr[:]); err != nil {
			e.err = err
			return VisitResponseAbort
		}
		b := info.Balance.Bytes32()
		if _, err := e.out.Write(b[:]); err != nil {
			e.err = err
			return VisitResponseAbort
		}
		if _, err := e.out.Write(info.Nonce[:]); err != nil {
			e.err = err
			return VisitResponseAbort
		}
		if _, err := e.out.Write(info.CodeHash[:]); err != nil {
			e.err = err
			return VisitResponseAbort
		}
	case *ValueNode:
		key := n.Key()
		value := n.Value()
		if _, err := e.out.Write([]byte{byte('S')}); err != nil {
			e.err = err
			return VisitResponseAbort
		}
		if _, err := e.out.Write(key[:]); err != nil {
			e.err = err
			return VisitResponseAbort
		}
		if _, err := e.out.Write(value[:]); err != nil {
			e.err = err
			return VisitResponseAbort
		}
	}
	return VisitResponseContinue
}

// getReferencedCodes returns a map of codes referenced by accounts in the
// given database. The map is indexed by the code hash.
func getReferencedCodes(db TrieVisitor) (map[common.Hash][]byte, error) {
	codes := make(map[common.Hash][]byte)
	err := db.Visit(MakeVisitor(func(node Node, info NodeInfo) VisitResponse {
		if n, ok := node.(*AccountNode); ok {
			codeHash := n.Info().CodeHash
			code := db.GetCodeForHash(codeHash)
			if len(code) > 0 {
				codes[codeHash] = code
			}
			return VisitResponsePrune // < no need to visit the storage trie
		}
		return VisitResponseContinue
	}))
	return codes, err
}

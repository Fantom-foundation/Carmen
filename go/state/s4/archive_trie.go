package s4

import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// TODO: make thread safe.

type ArchiveTrie struct {
	head  *S4State // the current head-state
	roots []NodeId // the roots of individual blocks indexed by block height
}

func OpenArchiveTrie(directory string) (archive.Archive, error) {
	state, err := OpenGoFileState(directory)
	if err != nil {
		return nil, err
	}
	// TODO: load block root list
	return &ArchiveTrie{
		head: state,
	}, nil
}

func (a *ArchiveTrie) Add(block uint64, update common.Update) error {
	if uint64(len(a.roots)) > block {
		return fmt.Errorf("block %d already present", block)
	}

	// Mark skipped blocks as having no changes.
	for uint64(len(a.roots)) < block {
		a.roots = append(a.roots, a.head.trie.root)
	}

	// Apply all the changes of the update.
	// TODO: refactor update infrastructure to use applyUpdate
	for _, addr := range update.DeletedAccounts {
		if err := a.head.DeleteAccount(addr); err != nil {
			return err
		}
	}
	for _, addr := range update.CreatedAccounts {
		if err := a.head.CreateAccount(addr); err != nil {
			return err
		}
	}
	for _, change := range update.Balances {
		if err := a.head.SetBalance(change.Account, change.Balance); err != nil {
			return err
		}
	}
	for _, change := range update.Nonces {
		if err := a.head.SetNonce(change.Account, change.Nonce); err != nil {
			return err
		}
	}
	for _, change := range update.Codes {
		if err := a.head.SetCode(change.Account, change.Code); err != nil {
			return err
		}
	}
	for _, change := range update.Slots {
		if err := a.head.SetStorage(change.Account, change.Key, change.Value); err != nil {
			return err
		}
	}

	// Freez new state.
	trie := a.head.trie
	forest := trie.forest
	root, err := forest.getNode(trie.root)
	if err != nil {
		return err
	}
	if err := root.Freeze(forest); err != nil {
		return err
	}

	// Refresh hashes.
	_, err = a.head.GetHash()
	if err != nil {
		return err
	}

	// Save new root node.
	a.roots = append(a.roots, trie.root)
	return nil
}

func (a *ArchiveTrie) GetLastBlockHeight() (block uint64, err error) {
	if len(a.roots) == 0 {
		return 0, fmt.Errorf("no block in archive")
	}
	return uint64(len(a.roots) - 1), nil
}

func (a *ArchiveTrie) Exists(block uint64, account common.Address) (exists bool, err error) {
	if block >= uint64(len(a.roots)) {
		return false, fmt.Errorf("invalid block: %d >= %d", block, len(a.roots))
	}
	root := a.roots[block]
	view := getTrieView(root, a.head.trie.forest)
	_, exists, err = view.GetAccountInfo(account)
	return exists, err
}

func (a *ArchiveTrie) GetBalance(block uint64, account common.Address) (balance common.Balance, err error) {
	if block >= uint64(len(a.roots)) {
		return common.Balance{}, fmt.Errorf("invalid block: %d >= %d", block, len(a.roots))
	}
	root := a.roots[block]
	view := getTrieView(root, a.head.trie.forest)
	info, _, err := view.GetAccountInfo(account)
	if err != nil {
		return common.Balance{}, err
	}
	return info.Balance, nil
}

func (a *ArchiveTrie) GetCode(block uint64, account common.Address) (code []byte, err error) {
	if block >= uint64(len(a.roots)) {
		return nil, fmt.Errorf("invalid block: %d >= %d", block, len(a.roots))
	}
	root := a.roots[block]
	view := getTrieView(root, a.head.trie.forest)
	info, _, err := view.GetAccountInfo(account)
	if err != nil {
		return nil, err
	}
	return a.head.code[info.CodeHash], nil
}

func (a *ArchiveTrie) GetNonce(block uint64, account common.Address) (nonce common.Nonce, err error) {
	if block >= uint64(len(a.roots)) {
		return common.Nonce{}, fmt.Errorf("invalid block: %d >= %d", block, len(a.roots))
	}
	root := a.roots[block]
	view := getTrieView(root, a.head.trie.forest)
	info, _, err := view.GetAccountInfo(account)
	if err != nil {
		return common.Nonce{}, err
	}
	return info.Nonce, nil
}

func (a *ArchiveTrie) GetStorage(block uint64, account common.Address, slot common.Key) (value common.Value, err error) {
	if block >= uint64(len(a.roots)) {
		return common.Value{}, fmt.Errorf("invalid block: %d >= %d", block, len(a.roots))
	}
	root := a.roots[block]
	view := getTrieView(root, a.head.trie.forest)
	return view.GetValue(account, slot)
}

func (a *ArchiveTrie) GetAccountHash(block uint64, account common.Address) (common.Hash, error) {
	panic("not implemented")
}

func (a *ArchiveTrie) GetHash(block uint64) (hash common.Hash, err error) {
	if block >= uint64(len(a.roots)) {
		return common.Hash{}, fmt.Errorf("invalid block: %d >= %d", block, len(a.roots))
	}
	root := a.roots[block]
	view := getTrieView(root, a.head.trie.forest)
	return view.GetHash()
}

func (s *ArchiveTrie) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	mf.AddChild("head", s.head.GetMemoryFootprint())
	mf.AddChild("roots", common.NewMemoryFootprint(uintptr(len(s.roots))*unsafe.Sizeof(NodeId(0))))
	return mf
}

func (a *ArchiveTrie) Dump() {
	for i, root := range a.roots {
		fmt.Printf("\nBlock %d:\n", i)
		view := getTrieView(root, a.head.trie.forest)
		view.Dump()
		fmt.Printf("\n")
	}
}

func (a *ArchiveTrie) Flush() error {
	return a.head.Flush()
}

func (a *ArchiveTrie) Close() error {
	return errors.Join(
		a.Flush(),
		a.head.Close(),
	)
}

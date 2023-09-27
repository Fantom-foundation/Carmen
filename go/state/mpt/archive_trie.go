package mpt

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// TODO: make thread safe.

// ArchiveTrie retains a per-block history of the state trie. Each state is
// a trie in a Forest of which the root node is retained. Updates can only
// be applied through the `Add` method, according to the `archive.Archive“
// interface, which this type is implementing.
//
// Its main task is to keep track of state rootes and to freeze the head
// state after each block.
type ArchiveTrie struct {
	head     *MptState // the current head-state
	roots    []Root    // the roots of individual blocks indexed by block height
	rootFile string    // the file storing the list of roots
}

func OpenArchiveTrie(directory string, config MptConfig) (archive.Archive, error) {
	rootfile := directory + "/roots.dat"
	roots, err := loadRoots(rootfile)
	if err != nil {
		return nil, err
	}
	forest, err := OpenFileForest(directory, config, Archive)
	if err != nil {
		return nil, err
	}
	head, err := makeTrie(directory, forest)
	if err != nil {
		forest.Close()
		return nil, err
	}
	state, err := newMptState(directory, head)
	if err != nil {
		head.Close()
		return nil, err
	}
	return &ArchiveTrie{
		head:     state,
		roots:    roots,
		rootFile: rootfile,
	}, nil
}

func VerifyArchive(directory string, config MptConfig, observer VerificationObserver) error {
	roots, err := loadRoots(directory + "/roots.dat")
	if err != nil {
		return err
	}
	if len(roots) == 0 {
		return nil
	}
	return VerifyFileForest(directory, config, roots, observer)
}

func (a *ArchiveTrie) Add(block uint64, update common.Update) error {
	if uint64(len(a.roots)) > block {
		return fmt.Errorf("block %d already present", block)
	}

	// Mark skipped blocks as having no changes.
	if uint64(len(a.roots)) < block {
		lastHash, err := a.head.trie.GetHash()
		if err != nil {
			return err
		}
		for uint64(len(a.roots)) < block {
			a.roots = append(a.roots, Root{a.head.trie.root, lastHash})
		}
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

	// Freeze new state.
	trie := a.head.trie
	if err := trie.forest.Freeze(trie.root); err != nil {
		return err
	}

	// Refresh hashes.
	hash, err := a.head.GetHash()
	if err != nil {
		return err
	}

	// Save new root node.
	a.roots = append(a.roots, Root{trie.root, hash})
	return nil
}

func (a *ArchiveTrie) GetBlockHeight() (block uint64, empty bool, err error) {
	if len(a.roots) == 0 {
		return 0, true, nil
	}
	return uint64(len(a.roots) - 1), false, nil
}

func (a *ArchiveTrie) Exists(block uint64, account common.Address) (exists bool, err error) {
	if block >= uint64(len(a.roots)) {
		return false, fmt.Errorf("invalid block: %d >= %d", block, len(a.roots))
	}
	root := a.roots[block]
	view := getTrieView(root.nodeId, a.head.trie.forest)
	_, exists, err = view.GetAccountInfo(account)
	return exists, err
}

func (a *ArchiveTrie) GetBalance(block uint64, account common.Address) (balance common.Balance, err error) {
	if block >= uint64(len(a.roots)) {
		return common.Balance{}, fmt.Errorf("invalid block: %d >= %d", block, len(a.roots))
	}
	root := a.roots[block]
	view := getTrieView(root.nodeId, a.head.trie.forest)
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
	view := getTrieView(root.nodeId, a.head.trie.forest)
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
	view := getTrieView(root.nodeId, a.head.trie.forest)
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
	view := getTrieView(root.nodeId, a.head.trie.forest)
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
	view := getTrieView(root.nodeId, a.head.trie.forest)
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
		fmt.Printf("\nBlock %d: %x\n", i, root.hash)
		view := getTrieView(root.nodeId, a.head.trie.forest)
		view.Dump()
		fmt.Printf("\n")
	}
}

func (a *ArchiveTrie) Flush() error {
	return errors.Join(
		a.head.Flush(),
		storeRoots(a.rootFile, a.roots),
	)
}

func (a *ArchiveTrie) Close() error {
	return errors.Join(
		a.Flush(),
		a.head.Close(),
	)
}

// ---- Reading and Writing Root Node ID Lists ----

func loadRoots(filename string) ([]Root, error) {
	// If there is no file, initialize and return an empty list.
	if _, err := os.Stat(filename); err != nil {
		return nil, nil
	}

	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := bufio.NewReader(f)

	res := []Root{}
	var id [4]byte
	var hash common.Hash
	for {
		if num, err := reader.Read(id[:]); err != nil {
			if err == io.EOF {
				return res, nil
			}
			return nil, err
		} else if num != len(id) {
			return nil, fmt.Errorf("invalid hash file")
		}

		if num, err := reader.Read(hash[:]); err != nil {
			return nil, err
		} else if num != len(hash) {
			return nil, fmt.Errorf("invalid hash file")
		}

		id := NodeId(binary.BigEndian.Uint32(id[:]))
		res = append(res, Root{id, hash})
	}
}

func storeRoots(filename string, roots []Root) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	writer := bufio.NewWriter(f)

	// Simple file format: [<node-id>]*
	var buffer [4]byte
	for _, root := range roots {
		binary.BigEndian.PutUint32(buffer[:], uint32(root.nodeId))
		if _, err := writer.Write(buffer[:]); err != nil {
			return err
		}
		if _, err := writer.Write(root.hash[:]); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return f.Close()
}

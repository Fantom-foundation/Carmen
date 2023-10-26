package mpt

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
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
// Its main task is to keep track of state roots and to freeze the head
// state after each block.
type ArchiveTrie struct {
	head       *MptState  // the current head-state
	roots      []Root     // the roots of individual blocks indexed by block height
	rootsMutex sync.Mutex // protecting access to the roots list
	rootFile   string     // the file storing the list of roots
	addMutex   sync.Mutex // a mutex to make sure that at any time only one thread is adding new blocks
}

func OpenArchiveTrie(directory string, config MptConfig) (archive.Archive, error) {
	rootfile := directory + "/roots.dat"
	roots, err := loadRoots(rootfile)
	if err != nil {
		return nil, err
	}
	forest, err := OpenFileForest(directory, config, Immutable)
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

func (a *ArchiveTrie) Add(block uint64, update common.Update, hint any) error {
	precomputedHashes, _ := hint.([]nodeHash)

	a.addMutex.Lock()
	defer a.addMutex.Unlock()

	a.rootsMutex.Lock()
	if uint64(len(a.roots)) > block {
		a.rootsMutex.Unlock()
		return fmt.Errorf("block %d already present", block)
	}

	// Mark skipped blocks as having no changes.
	if uint64(len(a.roots)) < block {
		lastHash, err := a.head.GetHash()
		if err != nil {
			a.rootsMutex.Unlock()
			return err
		}
		for uint64(len(a.roots)) < block {
			a.roots = append(a.roots, Root{a.head.trie.root, lastHash})
		}
	}
	a.rootsMutex.Unlock()

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
	var err error
	var hash common.Hash
	if precomputedHashes == nil {
		hash, _, err = a.head.trie.UpdateHashes()
	} else {
		err = a.head.trie.setHashes(precomputedHashes)
		if err == nil {
			hash, err = a.head.GetHash()
		}
	}
	if err != nil {
		return err
	}

	// Save new root node.
	a.rootsMutex.Lock()
	a.roots = append(a.roots, Root{trie.root, hash})
	a.rootsMutex.Unlock()
	return nil
}

func (a *ArchiveTrie) GetBlockHeight() (block uint64, empty bool, err error) {
	a.rootsMutex.Lock()
	length := uint64(len(a.roots))
	a.rootsMutex.Unlock()
	if length == 0 {
		return 0, true, nil
	}
	return length - 1, false, nil
}

func (a *ArchiveTrie) Exists(block uint64, account common.Address) (exists bool, err error) {
	view, err := a.getView(block)
	if err != nil {
		return false, err
	}
	_, exists, err = view.GetAccountInfo(account)
	return exists, err
}

func (a *ArchiveTrie) GetBalance(block uint64, account common.Address) (balance common.Balance, err error) {
	view, err := a.getView(block)
	if err != nil {
		return common.Balance{}, err
	}
	info, _, err := view.GetAccountInfo(account)
	if err != nil {
		return common.Balance{}, err
	}
	return info.Balance, nil
}

func (a *ArchiveTrie) GetCode(block uint64, account common.Address) (code []byte, err error) {
	view, err := a.getView(block)
	if err != nil {
		return nil, err
	}
	info, _, err := view.GetAccountInfo(account)
	if err != nil {
		return nil, err
	}
	return a.head.GetCodeForHash(info.CodeHash), nil
}

func (a *ArchiveTrie) GetNonce(block uint64, account common.Address) (nonce common.Nonce, err error) {
	view, err := a.getView(block)
	if err != nil {
		return common.Nonce{}, err
	}
	info, _, err := view.GetAccountInfo(account)
	if err != nil {
		return common.Nonce{}, err
	}
	return info.Nonce, nil
}

func (a *ArchiveTrie) GetStorage(block uint64, account common.Address, slot common.Key) (value common.Value, err error) {
	view, err := a.getView(block)
	if err != nil {
		return common.Value{}, err
	}
	return view.GetValue(account, slot)
}

func (a *ArchiveTrie) GetAccountHash(block uint64, account common.Address) (common.Hash, error) {
	panic("not implemented")
}

func (a *ArchiveTrie) GetHash(block uint64) (hash common.Hash, err error) {
	a.rootsMutex.Lock()
	length := uint64(len(a.roots))
	if block >= length {
		a.rootsMutex.Unlock()
		return common.Hash{}, fmt.Errorf("invalid block: %d >= %d", block, length)
	}
	res := a.roots[block].hash
	a.rootsMutex.Unlock()
	return res, nil
}

func (a *ArchiveTrie) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*a))
	mf.AddChild("head", a.head.GetMemoryFootprint())
	a.rootsMutex.Lock()
	mf.AddChild("roots", common.NewMemoryFootprint(uintptr(len(a.roots))*unsafe.Sizeof(NodeId(0))))
	a.rootsMutex.Unlock()
	return mf
}

func (a *ArchiveTrie) Dump() {
	a.rootsMutex.Lock()
	defer a.rootsMutex.Unlock()
	for i, root := range a.roots {
		fmt.Printf("\nBlock %d: %x\n", i, root.hash)
		view := getTrieView(root.nodeId, a.head.trie.forest)
		view.Dump()
		fmt.Printf("\n")
	}
}

func (a *ArchiveTrie) Flush() error {
	a.rootsMutex.Lock()
	defer a.rootsMutex.Unlock()
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

func (a *ArchiveTrie) getView(block uint64) (*LiveTrie, error) {
	a.rootsMutex.Lock()
	length := uint64(len(a.roots))
	if block >= length {
		a.rootsMutex.Unlock()
		return nil, fmt.Errorf("invalid block: %d >= %d", block, length)
	}
	rootId := a.roots[block].nodeId
	a.rootsMutex.Unlock()
	return getTrieView(rootId, a.head.trie.forest), nil
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
	return loadRootsFrom(reader)
}

func loadRootsFrom(reader io.Reader) ([]Root, error) {
	res := []Root{}
	var id [4]byte
	var hash common.Hash
	for {
		if _, err := io.ReadFull(reader, id[:]); err != nil {
			if err == io.EOF {
				return res, nil
			}
			return nil, fmt.Errorf("invalid root file format: %v", err)
		}

		if _, err := io.ReadFull(reader, hash[:]); err != nil {
			return nil, fmt.Errorf("invalid root file format: %v", err)
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
	if err := storeRootsTo(writer, roots); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return f.Close()
}

func storeRootsTo(writer io.Writer, roots []Root) error {
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
	return nil
}

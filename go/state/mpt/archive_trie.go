package mpt

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// ArchiveTrie retains a per-block history of the state trie. Each state is
// a trie in a Forest of which the root node is retained. Updates can only
// be applied through the `Add` method, according to the `archive.Archive“
// interface, which this type is implementing.
//
// Its main task is to keep track of state roots and to freeze the head
// state after each block.
type ArchiveTrie struct {
	head         LiveState // the current head-state
	forest       Database  // global forest with all versions of LiveState
	nodeSource   NodeSource
	roots        []Root     // the roots of individual blocks indexed by block height
	rootsMutex   sync.Mutex // protecting access to the roots list
	rootFile     string     // the file storing the list of roots
	addMutex     sync.Mutex // a mutex to make sure that at any time only one thread is adding new blocks
	errorMutex   sync.RWMutex
	archiveError error // a non-nil error will be stored here should it occur during any archive operation
}

func OpenArchiveTrie(directory string, config MptConfig, cacheCapacity int) (*ArchiveTrie, error) {
	lock, err := openStateDirectory(directory)
	if err != nil {
		return nil, err
	}
	rootfile := directory + "/roots.dat"
	roots, err := loadRoots(rootfile)
	if err != nil {
		return nil, err
	}
	forestConfig := ForestConfig{Mode: Immutable, CacheCapacity: cacheCapacity}
	forest, err := OpenFileForest(directory, config, forestConfig)
	if err != nil {
		return nil, err
	}
	head, err := makeTrie(directory, forest)
	if err != nil {
		forest.Close()
		return nil, err
	}
	state, err := newMptState(directory, lock, head)
	if err != nil {
		head.Close()
		return nil, err
	}
	return &ArchiveTrie{
		head:       state,
		forest:     forest,
		nodeSource: forest,
		roots:      roots,
		rootFile:   rootfile,
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
	if err := a.CheckErrors(); err != nil {
		return err
	}

	precomputedHashes, _ := hint.(*NodeHashes)

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
			return a.addError(err)
		}
		for uint64(len(a.roots)) < block {
			a.roots = append(a.roots, Root{a.head.Root(), lastHash})
		}
	}
	a.rootsMutex.Unlock()

	// Apply all the changes of the update.
	// TODO: refactor update infrastructure to use applyUpdate
	for _, addr := range update.DeletedAccounts {
		if err := a.head.DeleteAccount(addr); err != nil {
			return a.addError(err)
		}
	}
	for _, addr := range update.CreatedAccounts {
		if err := a.head.CreateAccount(addr); err != nil {
			return a.addError(err)
		}
	}
	for _, change := range update.Balances {
		if err := a.head.SetBalance(change.Account, change.Balance); err != nil {
			return a.addError(err)
		}
	}
	for _, change := range update.Nonces {
		if err := a.head.SetNonce(change.Account, change.Nonce); err != nil {
			return a.addError(err)
		}
	}
	for _, change := range update.Codes {
		if err := a.head.SetCode(change.Account, change.Code); err != nil {
			return a.addError(err)
		}
	}
	for _, change := range update.Slots {
		if err := a.head.SetStorage(change.Account, change.Key, change.Value); err != nil {
			return a.addError(err)
		}
	}

	// Freeze new state.
	root := a.head.Root()
	if err := a.forest.Freeze(&root); err != nil {
		return a.addError(err)
	}

	// Refresh hashes.
	var err error
	var hash common.Hash
	if precomputedHashes == nil {
		var hashes *NodeHashes
		hash, hashes, err = a.head.UpdateHashes()
		if hashes != nil {
			hashes.Release()
		}
	} else {
		err = a.head.setHashes(precomputedHashes)
		if err == nil {
			hash, err = a.head.GetHash()
		}
	}
	if err != nil {
		return a.addError(err)
	}

	// Save new root node.
	a.rootsMutex.Lock()
	a.roots = append(a.roots, Root{a.head.Root(), hash})
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
	if err != nil {
		return false, a.addError(err)
	}
	return exists, err
}

func (a *ArchiveTrie) GetBalance(block uint64, account common.Address) (balance common.Balance, err error) {
	view, err := a.getView(block)
	if err != nil {
		return common.Balance{}, err
	}
	info, _, err := view.GetAccountInfo(account)
	if err != nil {
		return common.Balance{}, a.addError(err)
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
		return nil, a.addError(err)
	}
	return a.head.GetCodeForHash(info.CodeHash), nil
}

func (a *ArchiveTrie) GetCodes() (map[common.Hash][]byte, error) {
	return a.head.GetCodes()
}

func (a *ArchiveTrie) GetNonce(block uint64, account common.Address) (nonce common.Nonce, err error) {
	view, err := a.getView(block)
	if err != nil {
		return common.Nonce{}, err
	}
	info, _, err := view.GetAccountInfo(account)
	if err != nil {
		return common.Nonce{}, a.addError(err)
	}
	return info.Nonce, nil
}

func (a *ArchiveTrie) GetStorage(block uint64, account common.Address, slot common.Key) (value common.Value, err error) {
	view, err := a.getView(block)
	if err != nil {
		return common.Value{}, a.addError(err)
	}
	return view.GetValue(account, slot)
}

func (a *ArchiveTrie) GetAccountHash(block uint64, account common.Address) (common.Hash, error) {
	return common.Hash{}, fmt.Errorf("not implemented")
}

func (a *ArchiveTrie) GetHash(block uint64) (hash common.Hash, err error) {
	a.rootsMutex.Lock()
	length := uint64(len(a.roots))
	if block >= length {
		a.rootsMutex.Unlock()
		return common.Hash{}, fmt.Errorf("invalid block: %d >= %d", block, length)
	}
	res := a.roots[block].Hash
	a.rootsMutex.Unlock()
	return res, nil
}

// GetDiff computes the difference between the given source and target blocks.
func (a *ArchiveTrie) GetDiff(srcBlock, trgBlock uint64) (Diff, error) {
	a.rootsMutex.Lock()
	if srcBlock >= uint64(len(a.roots)) {
		a.rootsMutex.Unlock()
		return Diff{}, fmt.Errorf("source block %d not present in archive, highest block is %d", srcBlock, len(a.roots)-1)
	}
	if trgBlock >= uint64(len(a.roots)) {
		a.rootsMutex.Unlock()
		return Diff{}, fmt.Errorf("target block %d not present in archive, highest block is %d", trgBlock, len(a.roots)-1)
	}
	before := a.roots[srcBlock].NodeRef
	after := a.roots[trgBlock].NodeRef
	a.rootsMutex.Unlock()
	return GetDiff(a.nodeSource, &before, &after)
}

// GetDiffForBlock computes the diff introduced by the given block compared to its
// predecessor. Note that this enables access to the changes introduced by block 0.
func (a *ArchiveTrie) GetDiffForBlock(block uint64) (Diff, error) {
	if block == 0 {
		a.rootsMutex.Lock()
		if len(a.roots) == 0 {
			a.rootsMutex.Unlock()
			return Diff{}, fmt.Errorf("archive is empty, no diff present for block 0")
		}
		after := a.roots[0].NodeRef
		a.rootsMutex.Unlock()
		return GetDiff(a.nodeSource, &emptyNodeReference, &after)
	}
	return a.GetDiff(block-1, block)
}

func (a *ArchiveTrie) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*a))
	mf.AddChild("head", a.head.GetMemoryFootprint())
	a.rootsMutex.Lock()
	mf.AddChild("roots", common.NewMemoryFootprint(uintptr(len(a.roots))*unsafe.Sizeof(NodeId(0))))
	a.rootsMutex.Unlock()
	return mf
}

func (a *ArchiveTrie) Check() error {
	roots := make([]*NodeReference, len(a.roots))
	for i := 0; i < len(a.roots); i++ {
		roots[i] = &a.roots[i].NodeRef
	}
	return errors.Join(
		a.CheckErrors(),
		a.forest.CheckAll(roots))
}

func (a *ArchiveTrie) Dump() {
	a.rootsMutex.Lock()
	defer a.rootsMutex.Unlock()
	for i, root := range a.roots {
		fmt.Printf("\nBlock %d: %x\n", i, root.Hash)
		view := getTrieView(root.NodeRef, a.forest)
		view.Dump()
		fmt.Printf("\n")
	}
}

func (a *ArchiveTrie) Flush() error {
	a.rootsMutex.Lock()
	defer a.rootsMutex.Unlock()
	return errors.Join(
		a.CheckErrors(),
		a.head.Flush(),
		StoreRoots(a.rootFile, a.roots),
	)
}

func (a *ArchiveTrie) Close() error {
	return a.head.closeWithError(a.Flush())
}

func (a *ArchiveTrie) getView(block uint64) (*LiveTrie, error) {
	if err := a.CheckErrors(); err != nil {
		return nil, err
	}

	a.rootsMutex.Lock()
	length := uint64(len(a.roots))
	if block >= length {
		a.rootsMutex.Unlock()
		return nil, fmt.Errorf("invalid block: %d >= %d", block, length)
	}
	rootRef := a.roots[block].NodeRef
	a.rootsMutex.Unlock()
	return getTrieView(rootRef, a.forest), nil
}

// CheckErrors returns a non-nil error should any error
// happen during any operation in this archive.
// In particular, updating this archive or getting
// values out of it may fail, and in this case,
// the error is stored and returned in this method.
// Further calls to this archive produce the same
// error as this method returns.
func (a *ArchiveTrie) CheckErrors() error {
	a.errorMutex.RLock()
	defer a.errorMutex.RUnlock()
	return a.archiveError
}

func (a *ArchiveTrie) addError(err error) error {
	a.errorMutex.Lock()
	defer a.errorMutex.Unlock()
	a.archiveError = errors.Join(a.archiveError, err)
	return a.archiveError
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
	encoder := NodeIdEncoder{}
	buffer := make([]byte, encoder.GetEncodedSize())
	var hash common.Hash
	for {
		if _, err := io.ReadFull(reader, buffer); err != nil {
			if err == io.EOF {
				return res, nil
			}
			return nil, fmt.Errorf("invalid root file format: %v", err)
		}

		if _, err := io.ReadFull(reader, hash[:]); err != nil {
			return nil, fmt.Errorf("invalid root file format: %v", err)
		}

		var id NodeId
		encoder.Load(buffer, &id)
		res = append(res, Root{NewNodeReference(id), hash})
	}
}

func StoreRoots(filename string, roots []Root) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(f)
	return errors.Join(
		storeRootsTo(writer, roots),
		writer.Flush(),
		f.Close())
}

func storeRootsTo(writer io.Writer, roots []Root) error {
	// Simple file format: [<node-id><state-hash>]*
	encoder := NodeIdEncoder{}
	buffer := make([]byte, encoder.GetEncodedSize())
	for _, root := range roots {
		encoder.Store(buffer, &root.NodeRef.id)
		if _, err := writer.Write(buffer[:]); err != nil {
			return err
		}
		if _, err := writer.Write(root.Hash[:]); err != nil {
			return err
		}
	}
	return nil
}

// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package mpt

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"github.com/Fantom-foundation/Carmen/go/common/witness"

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
	roots        rootList   // the roots of individual blocks indexed by block height
	rootsMutex   sync.Mutex // protecting access to the roots list
	rootFile     string     // the file storing the list of roots
	addMutex     sync.Mutex // a mutex to make sure that at any time only one thread is adding new blocks
	errorMutex   sync.RWMutex
	archiveError error // a non-nil error will be stored here should it occur during any archive operation

	// Check-point support for DB healing.
	checkpointCoordinator utils.CheckpointCoordinator
	checkpointMetadata    checkpointMetadata
	directory             string // TODO: remove this field
}

func OpenArchiveTrie(directory string, config MptConfig, cacheConfig NodeCacheConfig) (*ArchiveTrie, error) {
	lock, err := openStateDirectory(directory)
	if err != nil {
		return nil, err
	}
	rootfile := filepath.Join(directory, "roots.dat")
	roots, err := loadRoots(rootfile)
	if err != nil {
		return nil, err
	}
	checkpoint, err := loadCheckPointMetaData(directory)
	if err != nil {
		return nil, err
	}

	forestConfig := ForestConfig{Mode: Immutable, NodeCacheConfig: cacheConfig}
	forest, err := OpenFileForest(directory, config, forestConfig)
	if err != nil {
		return nil, err
	}
	head, err := makeTrie(directory, forest)
	if err != nil {
		return nil, errors.Join(err, forest.Close())
	}
	state, err := newMptState(directory, lock, head)
	if err != nil {
		return nil, errors.Join(err, head.Close())
	}

	checkpointDir := filepath.Join(directory, "checkpoint")
	coordinator, err := utils.NewCheckpointCoordinator(
		checkpointDir,
		forest.accounts,
		forest.branches,
		forest.extensions,
		forest.values,
		// TODO: add support for the following
		//  - roots
		//  - codes
		//  - metadata
	)
	if err != nil {
		return nil, errors.Join(err, head.Close())
	}

	return &ArchiveTrie{
		head:                  state,
		forest:                forest,
		nodeSource:            forest,
		roots:                 roots,
		rootFile:              rootfile,
		checkpointCoordinator: coordinator,
		checkpointMetadata:    checkpoint,
		directory:             directory,
	}, nil
}

// VerifyArchiveTrie validates file-based archive stored in the given directory.
// If the test passes, the data stored in the respective directory
// can be considered a valid archive database of the given configuration.
func VerifyArchiveTrie(directory string, config MptConfig, observer VerificationObserver) error {
	roots, err := loadRoots(directory + "/roots.dat")
	if err != nil {
		return err
	}
	if roots.length() == 0 {
		return nil
	}
	return VerifyMptState(directory, config, roots.roots, observer)
}

func (a *ArchiveTrie) Add(block uint64, update common.Update, hint any) error {
	if err := a.CheckErrors(); err != nil {
		return err
	}

	precomputedHashes, _ := hint.(*NodeHashes)

	a.addMutex.Lock()
	defer a.addMutex.Unlock()

	a.rootsMutex.Lock()
	if uint64(a.roots.length()) > block {
		a.rootsMutex.Unlock()
		return fmt.Errorf("block %d already present", block)
	}

	// Mark skipped blocks as having no changes.
	if uint64(a.roots.length()) < block {
		lastHash, err := a.head.GetHash()
		if err != nil {
			a.rootsMutex.Unlock()
			return a.addError(err)
		}
		for uint64(a.roots.length()) < block {
			a.roots.append(Root{a.head.Root(), lastHash})
		}
	}
	a.rootsMutex.Unlock()

	// Apply all the changes of the update.
	if err := update.ApplyTo(a.head); err != nil {
		return a.addError(err)
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
	a.roots.append(Root{a.head.Root(), hash})
	a.rootsMutex.Unlock()

	// Create a new checkpoint.
	if block%10 == 0 {
		if err := a.CreateCheckpoint(); err != nil {
			return err
		}
	}

	return nil
}

func (a *ArchiveTrie) GetBlockHeight() (block uint64, empty bool, err error) {
	a.rootsMutex.Lock()
	length := uint64(a.roots.length())
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
	length := uint64(a.roots.length())
	if block >= length {
		a.rootsMutex.Unlock()
		return common.Hash{}, fmt.Errorf("invalid block: %d >= %d", block, length)
	}
	res := a.roots.get(block).Hash
	a.rootsMutex.Unlock()
	return res, nil
}

func (a *ArchiveTrie) CreateWitnessProof(block uint64, address common.Address, keys ...common.Key) (witness.Proof, error) {
	if a.nodeSource.getConfig().Name != "S5-Archive" {
		return nil, archive.ErrWitnessProofNotSupported
	}
	a.rootsMutex.Lock()
	ref := a.roots.roots[block].NodeRef
	a.rootsMutex.Unlock()
	return CreateWitnessProof(a.nodeSource, &ref, address, keys...)
}

// GetDiff computes the difference between the given source and target blocks.
func (a *ArchiveTrie) GetDiff(srcBlock, trgBlock uint64) (Diff, error) {
	a.rootsMutex.Lock()
	if srcBlock >= uint64(a.roots.length()) {
		a.rootsMutex.Unlock()
		return Diff{}, fmt.Errorf("source block %d not present in archive, highest block is %d", srcBlock, a.roots.length()-1)
	}
	if trgBlock >= uint64(a.roots.length()) {
		a.rootsMutex.Unlock()
		return Diff{}, fmt.Errorf("target block %d not present in archive, highest block is %d", trgBlock, a.roots.length()-1)
	}
	before := a.roots.get(srcBlock).NodeRef
	after := a.roots.get(trgBlock).NodeRef
	a.rootsMutex.Unlock()
	return GetDiff(a.nodeSource, &before, &after)
}

// GetDiffForBlock computes the diff introduced by the given block compared to its
// predecessor. Note that this enables access to the changes introduced by block 0.
func (a *ArchiveTrie) GetDiffForBlock(block uint64) (Diff, error) {
	if block == 0 {
		a.rootsMutex.Lock()
		if a.roots.length() == 0 {
			a.rootsMutex.Unlock()
			return Diff{}, fmt.Errorf("archive is empty, no diff present for block 0")
		}
		after := a.roots.get(0).NodeRef
		a.rootsMutex.Unlock()
		return GetDiff(a.nodeSource, &emptyNodeReference, &after)
	}
	return a.GetDiff(block-1, block)
}

func (a *ArchiveTrie) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*a))
	mf.AddChild("head", a.head.GetMemoryFootprint())
	a.rootsMutex.Lock()
	mf.AddChild("roots", common.NewMemoryFootprint(uintptr(a.roots.length())*unsafe.Sizeof(NodeId(0))))
	a.rootsMutex.Unlock()
	return mf
}

func (a *ArchiveTrie) Check() error {
	roots := make([]*NodeReference, a.roots.length())
	for i := 0; i < a.roots.length(); i++ {
		roots[i] = &a.roots.roots[i].NodeRef
	}
	return errors.Join(
		a.CheckErrors(),
		a.forest.CheckAll(roots))
}

func (a *ArchiveTrie) Dump() {
	a.rootsMutex.Lock()
	defer a.rootsMutex.Unlock()
	for i, root := range a.roots.roots {
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
		a.roots.storeRoots(),
	)
}

func (a *ArchiveTrie) Close() error {
	return errors.Join(
		a.CheckErrors(),
		a.head.closeWithError(a.Flush()))
}

func (a *ArchiveTrie) GetCheckpointBlock() (uint64, error) {
	if a.checkpointMetadata.Block != nil {
		return *a.checkpointMetadata.Block, nil
	}
	return 0, fmt.Errorf("archive has no checkpoint")
}

func (a *ArchiveTrie) CreateCheckpoint() error {
	// Before the checkpoint can be created, all data needs
	// to be flushed to the underlying storage.
	if err := a.Flush(); err != nil {
		return err
	}
	// The creation of the checkpoint makes the current
	// state recoverable in case of a crash.
	_, err := a.checkpointCoordinator.CreateCheckpoint()

	// TODO: this update should be part of the checkpoint coordinator
	if err == nil {
		block := uint64(a.roots.length()) - 1
		a.checkpointMetadata.Block = &block
		if err := a.checkpointMetadata.store(a.directory); err != nil {
			return err
		}
	}

	return err
}

func RestoreCheckpoint(directory string, config MptConfig) error {
	// If needed, remove the dirty flags from the directory. This
	// indicates a potential corrupted archive state due to a non-clean
	// archive shutdown -- which is what this restoration is supposed to fix.
	dirty, err := isDirty(directory)
	if err != nil {
		return fmt.Errorf("failed to access directory %v: %v", directory, err)
	}
	if dirty {
		if err := markClean(directory); err != nil {
			return fmt.Errorf("failed to mark directory %v clean: %v", directory, err)
		}
	}

	archive, err := OpenArchiveTrie(directory, config, NodeCacheConfig{Capacity: 1})
	if err == nil {
		err = errors.Join(
			archive.RestoreCheckpoint(),
			archive.Close(),
		)
	}

	// If the restoration failed, mark the directory as dirty.
	if err != nil && dirty {
		err = errors.Join(err, markDirty(directory))
	}
	return err
}

func (a *ArchiveTrie) RestoreCheckpoint() error {
	lastBlock, err := a.GetCheckpointBlock()
	if err != nil {
		return err
	}
	return errors.Join(
		a.roots.truncate(lastBlock),
		a.checkpointCoordinator.Restore(),
	)
}

func (a *ArchiveTrie) getView(block uint64) (*LiveTrie, error) {
	if err := a.CheckErrors(); err != nil {
		return nil, err
	}

	a.rootsMutex.Lock()
	length := uint64(a.roots.length())
	if block >= length {
		a.rootsMutex.Unlock()
		return nil, fmt.Errorf("invalid block: %d >= %d", block, length)
	}
	rootRef := a.roots.roots[block].NodeRef
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

// rootList is a utility type managing an in-memory copy of the list of roots
// of an archive and its synchronization with an on-disk file copy.
type rootList struct {
	roots          []Root
	filename       string
	numRootsInFile int
}

func (l *rootList) length() int {
	return len(l.roots)
}

func (l *rootList) get(block uint64) Root {
	return l.roots[block]
}

func (l *rootList) append(r Root) {
	l.roots = append(l.roots, r)
}

func (l *rootList) truncate(block uint64) error {
	length := block + 1
	l.roots = l.roots[:length]
	if l.numRootsInFile <= int(length) {
		return nil
	}
	l.numRootsInFile = int(length)
	return os.Truncate(l.filename, int64(length*uint64(NodeIdEncoder{}.GetEncodedSize()+32)))
}

func loadRoots(filename string) (rootList, error) {
	// If there is no file, initialize and return an empty list.
	if _, err := os.Stat(filename); err != nil {
		return rootList{filename: filename}, nil
	}

	f, err := os.Open(filename)
	if err != nil {
		return rootList{}, err
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	roots, err := loadRootsFrom(reader)
	if err != nil {
		return rootList{}, err
	}
	return rootList{
		roots:          roots,
		filename:       filename,
		numRootsInFile: len(roots),
	}, nil
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
	list := rootList{roots: roots, filename: filename}
	return list.storeRoots()
}

func (l *rootList) storeRoots() error {
	toBeWritten := l.roots[l.numRootsInFile:]
	if l.numRootsInFile > 0 && len(toBeWritten) == 0 {
		return nil
	}

	f, err := os.OpenFile(l.filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(f)
	res := errors.Join(
		storeRootsTo(writer, toBeWritten),
		writer.Flush(),
		f.Close(),
	)
	if res == nil {
		l.numRootsInFile = len(l.roots)
	}
	return res
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

// --- Checkpoint Metadata ---

// TODO: wrap checkpoint metadata into a type supporting the two-phase participation protocol.

type checkpointMetadata struct {
	Commit utils.Checkpoint
	Block  *uint64
}

func loadCheckPointMetaData(directory string) (checkpointMetadata, error) {
	filename := filepath.Join(directory, "checkpoint.json")

	// If there is no file, initialize and return default metadata.
	if _, err := os.Stat(filename); err != nil {
		return checkpointMetadata{}, nil
	}

	// If the file exists, parse it and return its content.
	data, err := os.ReadFile(filename)
	if err != nil {
		return checkpointMetadata{}, err
	}

	var meta checkpointMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return meta, err
	}
	return meta, nil
}

func (m *checkpointMetadata) store(directory string) error {
	filename := filepath.Join(directory, "checkpoint.json")
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0600)
}

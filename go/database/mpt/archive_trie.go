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
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"github.com/Fantom-foundation/Carmen/go/backend/utils/checkpoint"
	"github.com/Fantom-foundation/Carmen/go/common/witness"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
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
	roots        *rootList  // the roots of individual blocks indexed by block height
	rootsMutex   sync.Mutex // protecting access to the roots list
	addMutex     sync.Mutex // a mutex to make sure that at any time only one thread is adding new blocks
	errorMutex   sync.RWMutex
	archiveError error // a non-nil error will be stored here should it occur during any archive operation

	// Check-point support for DB healing.
	checkpointCoordinator checkpoint.Coordinator
	checkpointInterval    int
	checkpointPeriod      time.Duration
	lastCheckpointTime    time.Time
}

// ArchiveConfig is the configuration for the archive trie.
type ArchiveConfig struct {
	// The number of blocks after which the latest a checkpoint is created.
	CheckpointInterval int
	// The system-time period after which the latest a checkpoint is created.
	CheckpointPeriod time.Duration
}

const (
	fileNameArchiveCheckpointDirectory      = "checkpoint"
	fileNameArchiveRoots                    = "roots.dat"
	fileNameArchiveRootsCheckpointDirectory = "roots"
	fileNameArchiveRootsCommittedCheckpoint = "committed.json"
	fileNameArchiveRootsPreparedCheckpoint  = "prepare.json"
)

func OpenArchiveTrie(
	directory string,
	config MptConfig,
	cacheConfig NodeCacheConfig,
	archiveConfig ArchiveConfig,
) (*ArchiveTrie, error) {
	lock, err := openStateDirectory(directory)
	if err != nil {
		return nil, err
	}
	roots, err := loadRoots(directory)
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

	root := NewNodeReference(EmptyId())
	if roots.length() > 0 {
		root = roots.get(uint64(roots.length() - 1)).NodeRef
	}
	head.root = root

	state, err := newMptState(directory, lock, head)
	if err != nil {
		return nil, errors.Join(err, head.Close())
	}

	checkpointDir := filepath.Join(directory, fileNameArchiveCheckpointDirectory)
	coordinator, err := checkpoint.NewCoordinator(
		checkpointDir,
		forest.accounts,
		forest.branches,
		forest.extensions,
		forest.values,
		state.codes,
		roots,
	)
	if err != nil {
		return nil, errors.Join(err, head.Close())
	}

	// Load the checkpointing configuration and set
	// default values.
	checkpointInterval := archiveConfig.CheckpointInterval
	if checkpointInterval <= 0 {
		checkpointInterval = 1_000_000
	}
	checkpointPeriod := archiveConfig.CheckpointPeriod
	if checkpointPeriod <= 0 {
		checkpointPeriod = 10 * time.Minute
	}

	// Pick a random time in the past to introduce an offset
	// between archive instances started at roughly the same time.
	lastCheckpointTime := time.Now()
	lastCheckpointTime = lastCheckpointTime.Add(time.Duration(-1 * float64(checkpointPeriod) * rand.Float64()))

	return &ArchiveTrie{
		head:                  state,
		forest:                forest,
		nodeSource:            forest,
		roots:                 roots,
		checkpointCoordinator: coordinator,
		checkpointInterval:    checkpointInterval,
		checkpointPeriod:      checkpointPeriod,
		lastCheckpointTime:    lastCheckpointTime,
	}, nil
}

// VerifyArchiveTrie validates file-based archive stored in the given directory.
// If the test passes, the data stored in the respective directory
// can be considered a valid archive database of the given configuration.
func VerifyArchiveTrie(directory string, config MptConfig, observer VerificationObserver) error {
	roots, err := loadRoots(directory)
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
	previousRootsLength := a.roots.length()
	if uint64(previousRootsLength) > block {
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

	// Create a new checkpoint if we crossed an interval boundary.
	shouldCheckpoint := false
	if previousRootsLength == 0 {
		shouldCheckpoint = block >= uint64(a.checkpointInterval)
	} else {
		oldCheckpointInterval := (previousRootsLength - 1) / a.checkpointInterval
		newCheckpointInterval := int(block) / a.checkpointInterval
		shouldCheckpoint = oldCheckpointInterval != newCheckpointInterval
	}
	shouldCheckpoint = shouldCheckpoint || time.Since(a.lastCheckpointTime) > a.checkpointPeriod
	if shouldCheckpoint {
		if err := a.createCheckpoint(); err != nil {
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

func (a *ArchiveTrie) GetBalance(block uint64, account common.Address) (balance amount.Amount, err error) {
	view, err := a.getView(block)
	if err != nil {
		return amount.New(), err
	}
	info, _, err := view.GetAccountInfo(account)
	if err != nil {
		return amount.New(), a.addError(err)
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
	return a.GetCodeForHash(info.CodeHash), nil
}

func (a *ArchiveTrie) GetCodeForHash(hash common.Hash) []byte {
	return a.head.GetCodeForHash(hash)
}

func (a *ArchiveTrie) GetCodes() map[common.Hash][]byte {
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
	if !a.nodeSource.getConfig().UseHashedPaths {
		return nil, archive.ErrWitnessProofNotSupported
	}

	view, err := a.getView(block)
	if err != nil {
		return nil, err
	}

	return view.CreateWitnessProof(address, keys...)
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

func (a *ArchiveTrie) VisitTrie(block uint64, visitor NodeVisitor) error {
	view, err := a.getView(block)
	if err != nil {
		return err
	}
	return view.VisitTrie(visitor)
}

func (a *ArchiveTrie) Close() error {
	return errors.Join(
		a.CheckErrors(),
		a.head.closeWithError(a.Flush()))
}

func (a *ArchiveTrie) createCheckpoint() error {
	// Before the checkpoint can be created, all data needs
	// to be flushed to the underlying storage.
	if err := a.Flush(); err != nil {
		return err
	}
	// The creation of the checkpoint makes the current
	// state recoverable in case of a crash.
	_, err := a.checkpointCoordinator.CreateCheckpoint()
	if err == nil {
		a.lastCheckpointTime = time.Now()
	}
	return err
}

func GetCheckpointBlock(dir string) (uint64, error) {
	checkpointDir := filepath.Join(dir, fileNameArchiveCheckpointDirectory)
	coordinator, err := checkpoint.NewCoordinator(checkpointDir)
	if err != nil {
		return 0, err
	}
	cp := coordinator.GetCurrentCheckpoint()
	restorer := getRootListRestorer(dir)
	numRoots, err := restorer.getNumRootsInCheckpoint(cp)
	return uint64(numRoots - 1), err
}

func RestoreBlockHeight(directory string, config MptConfig, block uint64) (err error) {

	// Make sure access to the directory is exclusive.
	lock, err := LockDirectory(directory)
	if err != nil {
		return fmt.Errorf("failed to get exclusive access to directory: %v", err)
	}
	defer lock.Release()

	// Check available block height -- stop recovery if there are not enough blocks.
	checkpointHeight, err := GetCheckpointBlock(directory)
	if err != nil {
		return fmt.Errorf("failed to get checkpoint height: %v", err)
	}
	if block > uint64(checkpointHeight) {
		return fmt.Errorf("block %d is beyond the last checkpoint height of %d", block, checkpointHeight)
	}

	// Mark this directory as dirty at least for the duration of the recovery.
	if err := markDirty(directory); err != nil {
		return fmt.Errorf("failed to mark directory %s as dirty: %w", directory, err)
	}
	defer func() {
		// Only remove dirty flag is the recovery was successful.
		if err == nil {
			err = markClean(directory)
		}
	}()

	// Restore the last checkpoint created by the archive.
	rootRestorer := getRootListRestorer(directory)
	accountsDir, branchesDir, extensionsDir, valuesDir := getForestDirectories(directory)
	restorers := []checkpoint.Restorer{
		file.GetRestorer(accountsDir),
		file.GetRestorer(branchesDir),
		file.GetRestorer(extensionsDir),
		file.GetRestorer(valuesDir),
		getCodeRestorer(directory),
		rootRestorer,
	}

	checkpointDir := filepath.Join(directory, "checkpoint")
	if err := checkpoint.Restore(checkpointDir, restorers...); err != nil {
		return fmt.Errorf("failed to restore checkpoint: %w", err)
	}

	// After the checkpoint, restore the block height.
	return rootRestorer.truncate(int(block + 1))
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
	filename       string // < the file storing the list of roots
	directory      string // < the directory for checkpoint data
	numRootsInFile int

	checkpoint checkpoint.Checkpoint
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

func loadRoots(archiveDirectory string) (*rootList, error) {
	filename := filepath.Join(archiveDirectory, fileNameArchiveRoots)
	directory := filepath.Join(archiveDirectory, fileNameArchiveRootsCheckpointDirectory)

	// Create the directory for commit files if it does not exist.
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	// If there is no file, initialize and return an empty list.
	if _, err := os.Stat(filename); err != nil {
		return &rootList{
			filename:  filename,
			directory: directory,
		}, nil
	}

	committedCheckpointFile := filepath.Join(directory, fileNameArchiveRootsCommittedCheckpoint)
	checkpointData, err := readRootListCheckpointData(committedCheckpointFile)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	roots, err := loadRootsFrom(reader)
	if err != nil {
		return nil, err
	}
	return &rootList{
		roots:          roots,
		filename:       filename,
		directory:      directory,
		numRootsInFile: len(roots),
		checkpoint:     checkpointData.Checkpoint,
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

func (l *rootList) GuaranteeCheckpoint(checkpoint checkpoint.Checkpoint) error {
	if l.checkpoint == checkpoint {
		return nil
	}
	if l.checkpoint+1 == checkpoint {
		pendingFile := filepath.Join(l.directory, fileNameArchiveRootsPreparedCheckpoint)
		if _, err := os.Stat(pendingFile); err == nil {
			return l.Commit(checkpoint)
		}
	}
	return fmt.Errorf("unable to guarantee checkpoint %v, current checkpoint is %v", checkpoint, l.checkpoint)
}

func (l *rootList) Prepare(checkpoint checkpoint.Checkpoint) error {
	if l.checkpoint+1 != checkpoint {
		return fmt.Errorf("checkpoint mismatch, expected %v, got %v", l.checkpoint+1, checkpoint)
	}
	pendingFile := filepath.Join(l.directory, fileNameArchiveRootsPreparedCheckpoint)
	return writeRootListCheckpointData(pendingFile, rootListCheckpointData{
		Checkpoint: checkpoint,
		NumRoots:   l.length(),
	})
}

func (l *rootList) Commit(checkpoint checkpoint.Checkpoint) error {
	if l.checkpoint+1 != checkpoint {
		return fmt.Errorf("checkpoint mismatch, expected %v, got %v", l.checkpoint+1, checkpoint)
	}
	committedFile := filepath.Join(l.directory, fileNameArchiveRootsCommittedCheckpoint)
	pendingFile := filepath.Join(l.directory, fileNameArchiveRootsPreparedCheckpoint)
	meta, err := readRootListCheckpointData(pendingFile)
	if err != nil {
		return err
	}
	if meta.Checkpoint != checkpoint {
		return fmt.Errorf("checkpoint mismatch, prepared %v, committed %v", meta.Checkpoint, checkpoint)
	}
	l.checkpoint = checkpoint
	return os.Rename(pendingFile, committedFile)
}

func (l *rootList) Abort(checkpoint checkpoint.Checkpoint) error {
	if l.checkpoint+1 != checkpoint {
		return fmt.Errorf("checkpoint mismatch, expected %v, got %v", l.checkpoint+1, checkpoint)
	}
	pendingFile := filepath.Join(l.directory, fileNameArchiveRootsPreparedCheckpoint)
	return os.Remove(pendingFile)
}

type rootListRestorer struct {
	rootsFile string
	directory string
}

func getRootListRestorer(archiveDir string) rootListRestorer {
	return rootListRestorer{
		rootsFile: filepath.Join(archiveDir, fileNameArchiveRoots),
		directory: filepath.Join(archiveDir, fileNameArchiveRootsCheckpointDirectory),
	}
}

func (r rootListRestorer) Restore(checkpoint checkpoint.Checkpoint) error {
	meta, err := readRootListCheckpointData(filepath.Join(r.directory, fileNameArchiveRootsCommittedCheckpoint))
	if err != nil {
		return err
	}

	// If the given checkpoint is one step in the future, check whether there is a pending checkpoint.
	if meta.Checkpoint+1 == checkpoint {
		pending, err := readRootListCheckpointData(filepath.Join(r.directory, fileNameArchiveRootsPreparedCheckpoint))
		if err == nil && pending.Checkpoint == checkpoint {
			meta = pending
		}
	}

	if meta.Checkpoint != checkpoint {
		return fmt.Errorf("unknown checkpoint, have %v, wanted %v", meta.Checkpoint, checkpoint)
	}

	return truncateRootsFile(r.rootsFile, meta.NumRoots)
}

func (r rootListRestorer) getNumRootsInCheckpoint(checkpoint checkpoint.Checkpoint) (int, error) {
	meta, err := utils.ReadJsonFile[rootListCheckpointData](filepath.Join(r.directory, fileNameArchiveRootsCommittedCheckpoint))
	if err != nil {
		return 0, err
	}
	if meta.Checkpoint == checkpoint {
		return meta.NumRoots, nil
	}
	if meta.Checkpoint+1 == checkpoint {
		pending, err := utils.ReadJsonFile[rootListCheckpointData](filepath.Join(r.directory, fileNameArchiveRootsPreparedCheckpoint))
		if err == nil && pending.Checkpoint == checkpoint {
			return pending.NumRoots, nil
		}
	}
	return 0, fmt.Errorf("checkpoint %v not found", checkpoint)
}

func (r rootListRestorer) truncate(length int) error {
	committed := filepath.Join(r.directory, fileNameArchiveRootsCommittedCheckpoint)
	meta, err := utils.ReadJsonFile[rootListCheckpointData](committed)
	if err != nil {
		return err
	}
	if meta.NumRoots < length {
		return fmt.Errorf("cannot truncate to %d, only %d roots available", length, meta.NumRoots)
	}
	meta.NumRoots = length
	return errors.Join(
		writeRootListCheckpointData(committed, meta),
		truncateRootsFile(r.rootsFile, length),
	)
}

func truncateRootsFile(path string, length int) error {
	state, err := os.Stat(path)
	if err != nil {
		return err
	}
	entrySize := int64(NodeIdEncoder{}.GetEncodedSize() + 32)
	sourceLength := state.Size()
	targetLength := int64(length) * entrySize
	if sourceLength < targetLength {
		return fmt.Errorf("cannot truncate root file to %d elements, only %d elements available", targetLength/entrySize, sourceLength/entrySize)
	}
	return os.Truncate(path, targetLength)
}

type rootListCheckpointData struct {
	Checkpoint checkpoint.Checkpoint
	NumRoots   int
}

func readRootListCheckpointData(file string) (rootListCheckpointData, error) {
	_, err := os.Stat(file)
	if os.IsNotExist(err) {
		return rootListCheckpointData{}, nil
	}
	return utils.ReadJsonFile[rootListCheckpointData](file)
}

func writeRootListCheckpointData(file string, data rootListCheckpointData) error {
	return utils.WriteJsonFile(file, data)
}

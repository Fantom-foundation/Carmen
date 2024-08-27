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
	"errors"
	"fmt"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
	"github.com/Fantom-foundation/Carmen/go/state"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
)

//go:generate mockgen -source state.go -destination state_mocks.go -package mpt

// Database is a global single access point to Merkle-Patricia-Trie (MPT) data.
// It contains public methods to retrieve information about accounts
// and storage slots, furthermore, it allows for  MPT nodes hashing.
// MPT database maintains a DAG of MPT trees that can be each accessed
// via a single root node.
// The database is an appendable storage, where the current state is modified,
// accessible via the current root node, and eventually sealed and appended
// to the historical database.
// Root nodes allow for traversing the respective MPT tree hierarchy
// to access the history.
// A Freeze method is provided to seal the current MPT so that further updates
// will take place on a new version of the MPT.
// The Current MPT tree may be modified, and the modifications are destructive,
// until the tree is sealed by Freeze.
type Database interface {
	common.FlushAndCloser
	common.MemoryFootprintProvider

	// GetAccountInfo retrieves account information for input root and account address.
	GetAccountInfo(rootRef *NodeReference, addr common.Address) (AccountInfo, bool, error)

	// SetAccountInfo sets the input account into the storage under the input root and the address.
	SetAccountInfo(rootRef *NodeReference, addr common.Address, info AccountInfo) (NodeReference, error)

	// GetValue retrieves storage slot for input root, account address, and storage key.
	GetValue(rootRef *NodeReference, addr common.Address, key common.Key) (common.Value, error)

	// SetValue sets storage slot for input root, account address, and storage key.
	SetValue(rootRef *NodeReference, addr common.Address, key common.Key, value common.Value) (NodeReference, error)

	// ClearStorage removes all storage slots for the input address and the root.
	ClearStorage(rootRef *NodeReference, addr common.Address) (NodeReference, error)

	// HasEmptyStorage returns true if account has empty storage.
	HasEmptyStorage(rootRef *NodeReference, addr common.Address) (bool, error)

	// Freeze seals current trie, preventing further updates to it.
	Freeze(ref *NodeReference) error

	// VisitTrie allows for travertines the whole trie under the input root
	VisitTrie(rootRef *NodeReference, visitor NodeVisitor) error

	// Dump provides a debug print of the whole trie under the input root
	Dump(rootRef *NodeReference)

	// Check verifies internal invariants of the Trie instance. If the trie is
	// self-consistent, nil is returned and the Trie is ready to be accessed. If
	// errors are detected, the Trie is to be considered in an invalid state and
	// the behavior of all other operations is undefined.
	Check(rootRef *NodeReference) error

	// CheckAll verifies internal invariants of a set of Trie instances rooted by
	// the given nodes. It is a generalization of the Check() function.
	CheckAll(rootRefs []*NodeReference) error

	// CheckErrors returns an error that might have been
	// encountered on this forest in the past.
	// If the result is not empty, this
	// Forest is to be considered corrupted and should be discarded.
	CheckErrors() error

	updateHashesFor(ref *NodeReference) (common.Hash, *NodeHashes, error)
	setHashesFor(root *NodeReference, hashes *NodeHashes) error
}

// LiveState represents a single  Merkle-Patricia-Trie (MPT) view to the Database
// as it was accessed for a single root.
// It allows for reading and updating state
// of accounts, storage slots, and codes.
// Access to the data is provided via a set of getters,
// while the update is provides via a single Apply function.
type LiveState interface {
	common.UpdateTarget
	common.MemoryFootprintProvider
	state.LiveDB

	// GetHash provides hash root of this MPT.
	// The hash is recomputed if it is not available.
	GetHash() (hash common.Hash, err error)

	// GetCodeForHash retrieves bytecode stored
	// under the input hash.
	GetCodeForHash(hash common.Hash) []byte

	// GetCodes retrieves all codes and their hashes.
	GetCodes() map[common.Hash][]byte

	// UpdateHashes recomputes hash root of this trie.
	UpdateHashes() (common.Hash, *NodeHashes, error)

	// Root provides root of this trie.
	Root() NodeReference

	closeWithError(externalError error) error
	setHashes(hashes *NodeHashes) error
}

// MptState implementation of a state utilizes an MPT based data structure. While
// functionally equivalent to the Ethereum State MPT, hashes are computed using
// a configurable algorithm.
//
// The main role of the MptState is to provide an adapter between a LiveTrie and
// Carmen's State interface. Also, it retains an index of contract codes.
type MptState struct {
	directory string
	lock      common.LockFile
	trie      *LiveTrie
	codes     *codes
}

func newMptState(directory string, lock common.LockFile, trie *LiveTrie) (*MptState, error) {
	codes, err := openCodes(directory)
	if err != nil {
		return nil, err
	}
	return &MptState{
		directory: directory,
		lock:      lock,
		trie:      trie,
		codes:     codes,
	}, nil
}

func openStateDirectory(directory string) (common.LockFile, error) {
	lock, err := LockDirectory(directory)
	if err != nil {
		return nil, err
	}
	if err := tryMarkDirty(directory); err != nil {
		return nil, errors.Join(err, lock.Release())
	}

	return lock, nil
}

func tryMarkDirty(directory string) error {
	dirty, err := isDirty(directory)
	if err != nil {
		return err
	}
	if dirty {
		return fmt.Errorf("unable to open %s, content is dirty, likely corrupted", directory)
	}
	return markDirty(directory)
}

// OpenGoMemoryState loads state information from the given directory and
// creates a Trie entirely retained in memory.
func OpenGoMemoryState(directory string, config MptConfig, cacheConfig NodeCacheConfig) (*MptState, error) {
	lock, err := openStateDirectory(directory)
	if err != nil {
		return nil, err
	}
	trie, err := OpenInMemoryLiveTrie(directory, config, cacheConfig)
	if err != nil {
		return nil, err
	}
	return newMptState(directory, lock, trie)
}

func OpenGoFileState(directory string, config MptConfig, cacheConfig NodeCacheConfig) (*MptState, error) {
	lock, err := openStateDirectory(directory)
	if err != nil {
		return nil, err
	}
	trie, err := OpenFileLiveTrie(directory, config, cacheConfig)
	if err != nil {
		return nil, err
	}
	return newMptState(directory, lock, trie)
}

func (s *MptState) CreateAccount(address common.Address) (err error) {
	_, exists, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return err
	}
	if exists {
		// For existing accounts, only clear the storage, preserve the rest.
		return s.trie.ClearStorage(address)
	}
	// Create account with hash of empty code.
	return s.trie.SetAccountInfo(address, AccountInfo{
		CodeHash: emptyCodeHash,
	})
}

func (s *MptState) Exists(address common.Address) (bool, error) {
	_, exists, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (s *MptState) DeleteAccount(address common.Address) error {
	return s.trie.SetAccountInfo(address, AccountInfo{})
}

func (s *MptState) GetBalance(address common.Address) (balance amount.Amount, err error) {
	info, exists, err := s.trie.GetAccountInfo(address)
	if !exists || err != nil {
		return amount.New(), err
	}
	return info.Balance, nil
}

func (s *MptState) SetBalance(address common.Address, balance amount.Amount) (err error) {
	info, exists, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return err
	}
	if info.Balance == balance {
		return nil
	}
	info.Balance = balance
	if !exists {
		info.CodeHash = emptyCodeHash
	}
	return s.trie.SetAccountInfo(address, info)
}

func (s *MptState) GetNonce(address common.Address) (nonce common.Nonce, err error) {
	info, _, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return common.Nonce{}, err
	}
	return info.Nonce, nil
}

func (s *MptState) SetNonce(address common.Address, nonce common.Nonce) (err error) {
	info, exists, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return err
	}
	if info.Nonce == nonce {
		return nil
	}
	info.Nonce = nonce
	if !exists {
		info.CodeHash = emptyCodeHash
	}
	return s.trie.SetAccountInfo(address, info)
}

func (s *MptState) GetStorage(address common.Address, key common.Key) (value common.Value, err error) {
	return s.trie.GetValue(address, key)
}

func (s *MptState) SetStorage(address common.Address, key common.Key, value common.Value) error {
	return s.trie.SetValue(address, key, value)
}

func (s *MptState) HasEmptyStorage(address common.Address) (bool, error) {
	return s.trie.HasEmptyStorage(address)
}
func (s *MptState) GetCode(address common.Address) (value []byte, err error) {
	info, exists, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return s.GetCodeForHash(info.CodeHash), nil
}

func (s *MptState) GetCodeForHash(hash common.Hash) []byte {
	return s.codes.getCodeForHash(hash)
}

func (s *MptState) GetCodeSize(address common.Address) (size int, err error) {
	code, err := s.GetCode(address)
	if err != nil {
		return 0, err
	}
	return len(code), err
}

func (s *MptState) SetCode(address common.Address, code []byte) (err error) {
	info, exists, err := s.trie.GetAccountInfo(address)
	if err != nil {
		return err
	}
	if !exists && len(code) == 0 {
		return nil
	}
	codeHash := s.codes.add(code)
	info.CodeHash = codeHash
	return s.trie.SetAccountInfo(address, info)
}

func (s *MptState) GetCodeHash(address common.Address) (hash common.Hash, err error) {
	info, exists, err := s.trie.GetAccountInfo(address)
	if !exists || err != nil {
		return emptyCodeHash, err
	}
	return info.CodeHash, nil
}

func (s *MptState) GetRootId() NodeId {
	return s.trie.root.Id()
}

func (s *MptState) GetHash() (hash common.Hash, err error) {
	hash, hints, err := s.trie.UpdateHashes()
	if hints != nil {
		hints.Release()
	}
	return hash, err
}

func (s *MptState) Apply(block uint64, update common.Update) (archiveUpdateHints common.Releaser, err error) {
	if err := update.ApplyTo(s); err != nil {
		return nil, err
	}
	_, hints, err := s.trie.UpdateHashes()
	return hints, err
}

func (s *MptState) Visit(visitor NodeVisitor, _ bool) error {
	return s.trie.VisitTrie(visitor)
}

func (s *MptState) GetCodes() map[common.Hash][]byte {
	return s.codes.getCodes()
}

// Flush codes and state trie
func (s *MptState) Flush() error {
	return errors.Join(
		s.codes.Flush(),
		s.trie.forest.CheckErrors(),
		s.trie.Flush(),
	)
}

func (s *MptState) Close() error {
	return s.closeWithError(nil)
}

func (s *MptState) closeWithError(externalError error) error {
	// Only if the state can be successfully closed, the directory is to
	// be marked as clean. Otherwise, the dirty flag needs to be retained.
	err := errors.Join(
		externalError,
		s.Flush(),
		s.trie.Close(),
	)
	if err == nil {
		err = markClean(s.directory)
	}
	return errors.Join(
		err,
		s.lock.Release(),
	)
}

func (s *MptState) GetSnapshotableComponents() []backend.Snapshotable {
	//panic("not implemented")
	return nil
}

func (s *MptState) RunPostRestoreTasks() error {
	//panic("not implemented")
	return nil
}

// GetMemoryFootprint provides sizes of individual components of the state in the memory
func (s *MptState) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	mf.AddChild("trie", s.trie.GetMemoryFootprint())
	mf.AddChild("codes", s.codes.GetMemoryFootprint())
	return mf
}

func (s *MptState) UpdateHashes() (common.Hash, *NodeHashes, error) {
	return s.trie.UpdateHashes()
}

func (s *MptState) Root() NodeReference {
	return s.trie.root
}

func (s *MptState) setHashes(hashes *NodeHashes) error {
	return s.trie.setHashes(hashes)
}

// EstimatePerNodeMemoryUsage returns an estimated upper bound for the
// amount of memory used per MPT node. This values is provided to facilitate
// a conversion between memory limits expressed in bytes and MPT cache
// sizes defined by the number of stored nodes.
func EstimatePerNodeMemoryUsage() int {

	// The largest node is the BranchNode with ~944 bytes, which is
	// likely allocated into 1 KB memory slots. Thus, a memory usage
	// of 1 KB is used for the notes
	maxNodeSize := 1 << 10

	// Additionally, every node in the node cache needs a owner slot
	// and a NodeID/ownerPosition entry pair in the index of the cache.
	nodeCacheSlotSize := unsafe.Sizeof(nodeOwner{}) +
		unsafe.Sizeof(NodeId(0)) +
		unsafe.Sizeof(ownerPosition(0)) +
		unsafe.Sizeof(shared.Shared[Node]{})

	return maxNodeSize + int(nodeCacheSlotSize)
}

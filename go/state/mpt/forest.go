package mpt

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
)

type StorageMode bool

const (
	// Archive is the mode of an archive or a read-only state on the disk.
	// All nodes written to disk will be finalized and never updated again.
	Archive StorageMode = true
	// Live is the mode of an archive in which the state on the disk can be
	// modified through destructive updates.
	Live StorageMode = false
)

func (m StorageMode) String() string {
	switch m {
	case Archive:
		return "Archive"
	case Live:
		return "Live"
	default:
		return "?"
	}
}

// Forest is a utility node managing nodes for one or more Tries.
// It provides the common foundation for the Live and Archive Tries.
type Forest struct {
	config MptConfig

	// The stock containers managing individual node types.
	branches   stock.Stock[uint64, BranchNode]
	extensions stock.Stock[uint64, ExtensionNode]
	accounts   stock.Stock[uint64, AccountNode]
	values     stock.Stock[uint64, ValueNode]

	// Indicates whether all values in the stock should be considered
	// frozen, and thus immutable as required for the archive case or
	// mutable, as for the live-db-only case.
	storageMode StorageMode

	// A unified cache for all node types.
	nodeCache *common.Cache[NodeId, Node]

	// The set of dirty nodes. Nodes are dirty if there in-memory
	// state does not match their on-disk content.
	dirty map[NodeId]struct{}

	// A store for hashes.
	hashes      HashStore
	dirtyHashes map[NodeId]struct{}

	keyHasher     *common.CachedHasher[common.Key]
	addressHasher *common.CachedHasher[common.Address]
}

// The number of elements to retain in the node cache.
const cacheCapacity = 10_000_000

// The number of hashes retained in the cache of the addresses or caches keys
const hashesCacheCapacity = 100_000

func OpenInMemoryForest(directory string, config MptConfig, mode StorageMode) (*Forest, error) {
	success := false
	accountEncoder, branchEncoder, extensionEncoder, valueEncoder := getEncoder(config)
	branches, err := memory.OpenStock[uint64, BranchNode](branchEncoder, directory+"/branches")
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			branches.Close()
		}
	}()
	extensions, err := memory.OpenStock[uint64, ExtensionNode](extensionEncoder, directory+"/extensions")
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			extensions.Close()
		}
	}()
	accounts, err := memory.OpenStock[uint64, AccountNode](accountEncoder, directory+"/accounts")
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			accounts.Close()
		}
	}()
	values, err := memory.OpenStock[uint64, ValueNode](valueEncoder, directory+"/values")
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			values.Close()
		}
	}()
	hashes, err := OpenInMemoryHashStore(directory + "/hashes")
	if err != nil {
		return nil, err
	}
	success = true
	return makeForest(config, directory, branches, extensions, accounts, values, hashes, mode)
}

func OpenFileForest(directory string, config MptConfig, mode StorageMode) (*Forest, error) {
	success := false
	accountEncoder, branchEncoder, extensionEncoder, valueEncoder := getEncoder(config)
	branches, err := file.OpenStock[uint64, BranchNode](branchEncoder, directory+"/branches")
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			branches.Close()
		}
	}()
	extensions, err := file.OpenStock[uint64, ExtensionNode](extensionEncoder, directory+"/extensions")
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			extensions.Close()
		}
	}()
	accounts, err := file.OpenStock[uint64, AccountNode](accountEncoder, directory+"/accounts")
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			accounts.Close()
		}
	}()
	values, err := file.OpenStock[uint64, ValueNode](valueEncoder, directory+"/values")
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			values.Close()
		}
	}()
	hashes, err := OpenFileBasedHashStore(directory + "/hashes")
	if err != nil {
		return nil, err
	}
	success = true
	return makeForest(config, directory, branches, extensions, accounts, values, hashes, mode)
}

func makeForest(
	config MptConfig,
	directory string,
	branches stock.Stock[uint64, BranchNode],
	extensions stock.Stock[uint64, ExtensionNode],
	accounts stock.Stock[uint64, AccountNode],
	values stock.Stock[uint64, ValueNode],
	hashes HashStore,
	mode StorageMode,
) (*Forest, error) {
	return &Forest{
		config:        config,
		branches:      branches,
		extensions:    extensions,
		accounts:      accounts,
		values:        values,
		storageMode:   mode,
		nodeCache:     common.NewCache[NodeId, Node](cacheCapacity),
		dirty:         map[NodeId]struct{}{},
		hashes:        hashes,
		dirtyHashes:   map[NodeId]struct{}{},
		keyHasher:     common.NewCachedHasher[common.Key](hashesCacheCapacity, common.KeySerializer{}),
		addressHasher: common.NewCachedHasher[common.Address](hashesCacheCapacity, common.AddressSerializer{}),
	}, nil
}

func (s *Forest) GetAccountInfo(rootId NodeId, addr common.Address) (AccountInfo, bool, error) {
	root, err := s.getNode(rootId)
	if err != nil {
		return AccountInfo{}, false, err
	}
	path := AddressToNibblePath(addr, s)
	return root.GetAccount(s, addr, path[:])
}

func (s *Forest) SetAccountInfo(rootId NodeId, addr common.Address, info AccountInfo) (NodeId, error) {
	root, err := s.getNode(rootId)
	if err != nil {
		return NodeId(0), err
	}
	path := AddressToNibblePath(addr, s)
	newRoot, _, err := root.SetAccount(s, rootId, addr, path[:], info)
	if err != nil {
		return NodeId(0), err
	}
	return newRoot, nil
}

func (s *Forest) GetValue(rootId NodeId, addr common.Address, key common.Key) (common.Value, error) {
	root, err := s.getNode(rootId)
	if err != nil {
		return common.Value{}, err
	}
	path := AddressToNibblePath(addr, s)
	value, _, err := root.GetSlot(s, addr, path[:], key)
	return value, err
}

func (s *Forest) SetValue(rootId NodeId, addr common.Address, key common.Key, value common.Value) (NodeId, error) {
	root, err := s.getNode(rootId)
	if err != nil {
		return NodeId(0), err
	}
	path := AddressToNibblePath(addr, s)
	newRoot, _, err := root.SetSlot(s, rootId, addr, path[:], key, value)
	if err != nil {
		return NodeId(0), err
	}
	return newRoot, nil
}

func (s *Forest) ClearStorage(rootId NodeId, addr common.Address) error {
	root, err := s.getNode(rootId)
	if err != nil {
		return err
	}
	path := AddressToNibblePath(addr, s)
	_, _, err = root.ClearStorage(s, rootId, addr, path[:])
	return err
}

func (s *Forest) getHashFor(id NodeId) (common.Hash, error) {
	// The empty node is never dirty and needs to be handled explicitly.
	if id.IsEmpty() {
		return s.config.Hasher.GetHash(EmptyNode{}, s, s)
	}
	// Non-dirty hashes can be taken from the store.
	if _, dirty := s.dirtyHashes[id]; !dirty {
		return s.hashes.Get(id)
	}

	// Dirty hashes need to be re-freshed.
	node, err := s.getNode(id)
	if err != nil {
		return common.Hash{}, err
	}
	hash, err := s.config.Hasher.GetHash(node, s, s)
	if err != nil {
		return common.Hash{}, err
	}
	if err := s.hashes.Set(id, hash); err != nil {
		return hash, err
	}
	delete(s.dirtyHashes, id)
	return hash, nil
}

func (s *Forest) hashKey(key common.Key) common.Hash {
	return s.keyHasher.Hash(key)
}

func (s *Forest) hashAddress(address common.Address) common.Hash {
	return s.addressHasher.Hash(address)
}

func (f *Forest) Freeze(id NodeId) error {
	if f.storageMode != Archive {
		return fmt.Errorf("node-freezing only supported in archive mode")
	}
	root, err := f.getNode(id)
	if err != nil {
		return err
	}
	return root.Freeze(f)
}

func (s *Forest) Flush() error {
	// Flush dirty keys in order (to avoid excessive seeking).
	ids := make([]NodeId, len(s.dirty))
	for id := range s.dirty {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var errs = []error{}
	for _, id := range ids {
		node, present := s.nodeCache.Get(id)
		if present {
			if err := s.flush(id, node); err != nil {
				errs = append(errs, err)
			}
		} else {
			errs = append(errs, fmt.Errorf("missing dirty node %v in node cache", id))
		}
	}
	s.dirty = map[NodeId]struct{}{}

	// Update hashes for dirty nodes.
	dirty := make([]NodeId, len(s.dirtyHashes))
	for id := range s.dirtyHashes {
		dirty = append(dirty, id)
	}
	sort.Slice(dirty, func(i, j int) bool { return dirty[i] < dirty[j] })
	for _, id := range dirty {
		if _, err := s.getHashFor(id); err != nil {
			errs = append(errs, err)
		}
	}
	s.dirtyHashes = map[NodeId]struct{}{}

	return errors.Join(
		errors.Join(errs...),
		s.accounts.Flush(),
		s.branches.Flush(),
		s.extensions.Flush(),
		s.values.Flush(),
		s.hashes.Flush(),
	)
}

func (s *Forest) Close() error {
	return errors.Join(
		s.Flush(),
		s.accounts.Close(),
		s.branches.Close(),
		s.extensions.Close(),
		s.values.Close(),
		s.hashes.Close(),
	)
}

// GetMemoryFootprint provides sizes of individual components of the state in the memory
func (s *Forest) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	mf.AddChild("accounts", s.accounts.GetMemoryFootprint())
	mf.AddChild("branches", s.branches.GetMemoryFootprint())
	mf.AddChild("extensions", s.extensions.GetMemoryFootprint())
	mf.AddChild("values", s.values.GetMemoryFootprint())
	mf.AddChild("cache", s.nodeCache.GetDynamicMemoryFootprint(func(node Node) uintptr {
		if _, ok := node.(*AccountNode); ok {
			return unsafe.Sizeof(AccountNode{})
		}
		if _, ok := node.(*BranchNode); ok {
			return unsafe.Sizeof(BranchNode{})
		}
		if _, ok := node.(EmptyNode); ok {
			return unsafe.Sizeof(EmptyNode{})
		}
		if _, ok := node.(*ExtensionNode); ok {
			return unsafe.Sizeof(ExtensionNode{})
		}
		if _, ok := node.(*ValueNode); ok {
			return unsafe.Sizeof(ValueNode{})
		}
		panic(fmt.Sprintf("unexpected node type: %v", reflect.TypeOf(node)))
	}))
	mf.AddChild("hashes", s.hashes.GetMemoryFootprint())
	mf.AddChild("dirtyHashes", common.NewMemoryFootprint(uintptr(len(s.dirtyHashes))*unsafe.Sizeof(NodeId(0))))
	mf.AddChild("hashedKeysCache", s.keyHasher.GetMemoryFootprint())
	mf.AddChild("hashedAddressesCache", s.addressHasher.GetMemoryFootprint())
	return mf
}

// Dump prints the content of the Trie to the console. Mainly intended for debugging.
func (s *Forest) Dump(rootId NodeId) {
	root, err := s.getNode(rootId)
	if err != nil {
		fmt.Printf("Failed to fetch root: %v", err)
	} else {
		root.Dump(s, rootId, "")
	}
}

// Check verifies internal invariants of the Trie instance. If the trie is
// self-consistent, nil is returned and the Trie is read to be accessed. If
// errors are detected, the Trie is to be considered in an invalid state and
// the behaviour of all other operations is undefined.
func (s *Forest) Check(rootId NodeId) error {
	root, err := s.getNode(rootId)
	if err != nil {
		return err
	}
	return root.Check(s, make([]Nibble, 0, common.AddressSize*2))
}

// -- NodeManager interface --

func (s *Forest) getConfig() MptConfig {
	return s.config
}

func (s *Forest) getNode(id NodeId) (Node, error) {
	// Start by checking the node cache.
	res, found := s.nodeCache.Get(id)
	if found {
		return res, nil
	}

	// Load the node from peristent storage.
	var node Node
	var err error
	if id.IsValue() {
		value, e := s.values.Get(id.Index())
		node, err = &value, e
	} else if id.IsAccount() {
		value, e := s.accounts.Get(id.Index())
		node, err = &value, e
	} else if id.IsBranch() {
		value, e := s.branches.Get(id.Index())
		node, err = &value, e
	} else if id.IsExtension() {
		value, e := s.extensions.Get(id.Index())
		node, err = &value, e
	} else if id.IsEmpty() {
		node = EmptyNode{}
	} else {
		err = fmt.Errorf("unknown node ID: %v", id)
	}
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, fmt.Errorf("no node with ID %d in storage", id)
	}

	// Everything that is loaded from an archive is to be considered
	// frozen, and thus immutable.
	if s.storageMode == Archive {
		node.MarkFrozen()
	}

	if err := s.addToCache(id, node); err != nil {
		return nil, err
	}
	return node, nil
}

func (s *Forest) addToCache(id NodeId, node Node) error {
	if evictedId, evictedNode, evicted := s.nodeCache.Set(id, node); evicted {
		// TODO: perform flush asynchroniously.
		if err := s.flush(evictedId, evictedNode); err != nil {
			return err
		}
	}
	return nil
}

func (s *Forest) flush(id NodeId, node Node) error {
	// Note: flushing nodes in Archive mode will implicitly freeze them,
	// since after the reload they will be considered frozen. This may
	// cause temporary states between updates to be accidentially frozen,
	// leaving unreferenced nodes in the archive, but it is not causing
	// correctness issues. However, if the node-cache size is sufficiently
	// large, such cases should be rare. Nevertheless, a warning is
	// printed here to get informed if this changes in the future.
	if s.storageMode == Archive && !node.IsFrozen() {
		log.Printf("WARNING: non-frozen node flushed to disk causing implicit freeze")
	}

	if _, dirty := s.dirty[id]; !dirty {
		return nil
	}
	delete(s.dirty, id)
	if id.IsValue() {
		return s.values.Set(id.Index(), *node.(*ValueNode))
	} else if id.IsAccount() {
		return s.accounts.Set(id.Index(), *node.(*AccountNode))
	} else if id.IsBranch() {
		return s.branches.Set(id.Index(), *node.(*BranchNode))
	} else if id.IsExtension() {
		return s.extensions.Set(id.Index(), *node.(*ExtensionNode))
	} else if id.IsEmpty() {
		return nil
	} else {
		return fmt.Errorf("unknown node ID: %v", id)
	}
}

func (s *Forest) createAccount() (NodeId, *AccountNode, error) {
	i, err := s.accounts.New()
	if err != nil {
		return 0, nil, err
	}
	id := AccountId(i)
	node := new(AccountNode)
	if err := s.addToCache(id, node); err != nil {
		return 0, nil, err
	}
	return id, node, err
}

func (s *Forest) createBranch() (NodeId, *BranchNode, error) {
	i, err := s.branches.New()
	if err != nil {
		return 0, nil, err
	}
	id := BranchId(i)
	node := new(BranchNode)
	if err := s.addToCache(id, node); err != nil {
		return 0, nil, err
	}
	return id, node, err
}

func (s *Forest) createExtension() (NodeId, *ExtensionNode, error) {
	i, err := s.extensions.New()
	if err != nil {
		return 0, nil, err
	}
	id := ExtensionId(i)
	node := new(ExtensionNode)
	if err := s.addToCache(id, node); err != nil {
		return 0, nil, err
	}
	return id, node, err
}

func (s *Forest) createValue() (NodeId, *ValueNode, error) {
	i, err := s.values.New()
	if err != nil {
		return 0, nil, err
	}
	id := ValueId(i)
	node := new(ValueNode)
	if err := s.addToCache(id, node); err != nil {
		return 0, nil, err
	}
	return id, node, err
}

func (s *Forest) update(id NodeId, node Node) error {
	// all needed here is to register the modfied node as dirty
	s.dirty[id] = struct{}{}
	// ... and to invalidate the nodes hash.
	s.invalidateHash(id)
	return nil
}

func (s *Forest) invalidateHash(id NodeId) {
	// by adding it to the dirty hashes set the hash will be
	// re-evaluated the next time.
	if !id.IsEmpty() {
		s.dirtyHashes[id] = struct{}{}
	}
}

func (s *Forest) release(id NodeId) error {
	s.nodeCache.Remove(id)
	delete(s.dirty, id)
	delete(s.dirtyHashes, id)
	if id.IsAccount() {
		return s.accounts.Delete(id.Index())
	}
	if id.IsBranch() {
		return s.branches.Delete(id.Index())
	}
	if id.IsExtension() {
		return s.extensions.Delete(id.Index())
	}
	if id.IsValue() {
		return s.values.Delete(id.Index())
	}
	return fmt.Errorf("unable to release node %v", id)
}

func getEncoder(config MptConfig) (
	stock.ValueEncoder[AccountNode],
	stock.ValueEncoder[BranchNode],
	stock.ValueEncoder[ExtensionNode],
	stock.ValueEncoder[ValueNode],
) {
	if config.TrackSuffixLengthsInLeafNodes {
		return AccountNodeWithPathLengthEncoder{},
			BranchNodeEncoder{},
			ExtensionNodeEncoder{},
			ValueNodeWithPathLengthEncoder{}
	}
	return AccountNodeEncoder{},
		BranchNodeEncoder{},
		ExtensionNodeEncoder{},
		ValueNodeEncoder{}
}

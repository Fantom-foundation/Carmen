package mpt

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"sync"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/memory"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/synced"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
)

type StorageMode bool

const (
	// Immutable is the mode of an archive or a read-only state on the disk.
	// All nodes written to disk will be finalized and never updated again.
	Immutable StorageMode = true
	// Mutable is the mode of a LiveDB in which the state on the disk can be
	// modified through destructive updates.
	Mutable StorageMode = false
)

// printWarningDefaultNodeFreezing allows for printing a warning that a node is going to be frozen
// as a consequence of its flushing to the disk.
const printWarningDefaultNodeFreezing = false

func (m StorageMode) String() string {
	switch m {
	case Immutable:
		return "Immutable"
	case Mutable:
		return "Mutable"
	default:
		return "?"
	}
}

// Root is used to identify and verify root nodes of trees in forests.
type Root struct {
	NodeId NodeId
	Hash   common.Hash
}

// Forest is a utility node managing nodes for one or more Tries.
// It provides the common foundation for the Live and Archive Tries.
//
// Forests are thread safe. Thus, read and write operations may be
// conducted concurrently.
// TODO: rename to DAG ... since it is not really a Forest
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
	nodeCache      *common.Cache[NodeId, *shared.Shared[Node]]
	nodeCacheMutex sync.Mutex

	// The set of dirty nodes. Nodes are dirty if there in-memory
	// state does not match their on-disk content.
	dirty      map[NodeId]struct{}
	dirtyMutex sync.Mutex

	// The hasher managing node hashes for this forest.
	hasher hasher

	// Cached hashers for keys and addresses (thread safe).
	keyHasher     *common.CachedHasher[common.Key]
	addressHasher *common.CachedHasher[common.Address]

	// A buffer for asynchronously writing nodes to files.
	writeBuffer WriteBuffer
}

// The number of elements to retain in the node cache.
const cacheCapacity = 10_000_000

// The number of hashes retained in the cache of the addresses or caches keys
const hashesCacheCapacity = 100_000

func OpenInMemoryForest(directory string, config MptConfig, mode StorageMode) (*Forest, error) {
	if _, err := checkForestMetadata(directory, config, mode); err != nil {
		return nil, err
	}

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
	success = true
	return makeForest(config, directory, branches, extensions, accounts, values, mode)
}

func OpenFileForest(directory string, config MptConfig, mode StorageMode) (*Forest, error) {
	if _, err := checkForestMetadata(directory, config, mode); err != nil {
		return nil, err
	}

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
	success = true
	return makeForest(config, directory, branches, extensions, accounts, values, mode)
}

func checkForestMetadata(directory string, config MptConfig, mode StorageMode) (ForestMetadata, error) {
	path := directory + "/forest.json"
	meta, present, err := ReadForestMetadata(path)
	if err != nil {
		return meta, err
	}

	// Check present metadata to match expected configuration.
	if present {
		if want, got := config.Name, meta.Configuration; want != got {
			return meta, fmt.Errorf("unexpected MPT configuration in directory, wanted %v, got %v", want, got)
		}
		if want, got := StorageMode(mode == Mutable), StorageMode(meta.Mutable); want != got {
			return meta, fmt.Errorf("unexpected MPT storage mode in directory, wanted %v, got %v", want, got)
		}
		return meta, nil
	}

	// Write metadata to disk to create new forest.
	meta = ForestMetadata{
		Configuration: config.Name,
		Mutable:       mode == Mutable,
	}

	// Update on-disk meta-data.
	metadata, err := json.Marshal(meta)
	if err != nil {
		return meta, err
	}
	if err := os.WriteFile(path, metadata, 0600); err != nil {
		return meta, err
	}

	return meta, nil
}

func makeForest(
	config MptConfig,
	directory string,
	branches stock.Stock[uint64, BranchNode],
	extensions stock.Stock[uint64, ExtensionNode],
	accounts stock.Stock[uint64, AccountNode],
	values stock.Stock[uint64, ValueNode],
	mode StorageMode,
) (*Forest, error) {
	res := &Forest{
		config:        config,
		branches:      synced.Sync(branches),
		extensions:    synced.Sync(extensions),
		accounts:      synced.Sync(accounts),
		values:        synced.Sync(values),
		storageMode:   mode,
		nodeCache:     common.NewCache[NodeId, *shared.Shared[Node]](cacheCapacity),
		dirty:         map[NodeId]struct{}{},
		hasher:        config.Hashing.createHasher(),
		keyHasher:     common.NewCachedHasher[common.Key](hashesCacheCapacity, common.KeySerializer{}),
		addressHasher: common.NewCachedHasher[common.Address](hashesCacheCapacity, common.AddressSerializer{}),
	}
	res.writeBuffer = makeWriteBuffer(writeBufferSink{res}, 1024)
	return res, nil
}

func (s *Forest) GetAccountInfo(rootId NodeId, addr common.Address) (AccountInfo, bool, error) {
	handle, err := s.getReadAccess(rootId)
	if err != nil {
		return AccountInfo{}, false, err
	}
	defer handle.Release()
	path := AddressToNibblePath(addr, s)
	return handle.Get().GetAccount(s, addr, path[:])
}

func (s *Forest) SetAccountInfo(rootId NodeId, addr common.Address, info AccountInfo) (NodeId, error) {
	root, err := s.getWriteAccess(rootId)
	if err != nil {
		return NodeId(0), err
	}
	defer root.Release()
	path := AddressToNibblePath(addr, s)
	newRoot, _, err := root.Get().SetAccount(s, rootId, root, addr, path[:], info)
	if err != nil {
		return NodeId(0), err
	}
	return newRoot, nil
}

func (s *Forest) GetValue(rootId NodeId, addr common.Address, key common.Key) (common.Value, error) {
	root, err := s.getReadAccess(rootId)
	if err != nil {
		return common.Value{}, err
	}
	defer root.Release()
	path := AddressToNibblePath(addr, s)
	value, _, err := root.Get().GetSlot(s, addr, path[:], key)
	return value, err
}

func (s *Forest) SetValue(rootId NodeId, addr common.Address, key common.Key, value common.Value) (NodeId, error) {
	root, err := s.getWriteAccess(rootId)
	if err != nil {
		return NodeId(0), err
	}
	defer root.Release()
	path := AddressToNibblePath(addr, s)
	newRoot, _, err := root.Get().SetSlot(s, rootId, root, addr, path[:], key, value)
	if err != nil {
		return NodeId(0), err
	}
	return newRoot, nil
}

func (s *Forest) ClearStorage(rootId NodeId, addr common.Address) error {
	root, err := s.getWriteAccess(rootId)
	if err != nil {
		return err
	}
	defer root.Release()
	path := AddressToNibblePath(addr, s)
	_, _, err = root.Get().ClearStorage(s, rootId, root, addr, path[:])
	return err
}

func (s *Forest) VisitTrie(rootId NodeId, visitor NodeVisitor) error {
	root, err := s.getViewAccess(rootId)
	if err != nil {
		return err
	}
	defer root.Release()
	_, err = root.Get().Visit(s, rootId, 0, visitor)
	return err
}

func (s *Forest) updateHashesFor(id NodeId) (common.Hash, []nodeHash, error) {
	return s.hasher.updateHashes(id, s)
}

func (s *Forest) setHashesFor(id NodeId, hashes []nodeHash) error {
	for _, cur := range hashes {
		write, err := s.getMutableNodeByPath(id, cur.path)
		if err != nil {
			return err
		}
		write.Get().SetHash(cur.hash)
		write.Release()
	}
	return nil
}

func (s *Forest) getHashFor(id NodeId) (common.Hash, error) {
	return s.hasher.getHash(id, s)
}

func (s *Forest) hashKey(key common.Key) common.Hash {
	return s.keyHasher.Hash(key)
}

func (s *Forest) hashAddress(address common.Address) common.Hash {
	return s.addressHasher.Hash(address)
}

func (f *Forest) Freeze(id NodeId) error {
	if f.storageMode != Immutable {
		return fmt.Errorf("node-freezing only supported in archive mode")
	}
	root, err := f.getWriteAccess(id)
	if err != nil {
		return err
	}
	defer root.Release()
	return root.Get().Freeze(f, root)
}

func (s *Forest) Flush() error {
	// Get snapshot of set of dirty Node IDs.
	s.dirtyMutex.Lock()
	ids := make([]NodeId, 0, len(s.dirty))
	for id := range s.dirty {
		ids = append(ids, id)
	}
	s.dirty = map[NodeId]struct{}{}
	s.dirtyMutex.Unlock()

	// Flush dirty keys in order (to avoid excessive seeking).
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var errs = []error{}
	for _, id := range ids {
		s.nodeCacheMutex.Lock()
		node, present := s.nodeCache.Get(id)
		s.nodeCacheMutex.Unlock()
		if present {
			handle := node.GetReadHandle()
			err := s.flushNode(id, handle.Get())
			handle.Release()
			if err != nil {
				errs = append(errs, err)
			}
		} else {
			errs = append(errs, fmt.Errorf("missing dirty node %v in node cache", id))
		}
	}

	return errors.Join(
		errors.Join(errs...),
		s.writeBuffer.Flush(),
		s.accounts.Flush(),
		s.branches.Flush(),
		s.extensions.Flush(),
		s.values.Flush(),
	)
}

func (s *Forest) Close() error {
	return errors.Join(
		s.Flush(),
		s.writeBuffer.Close(),
		s.accounts.Close(),
		s.branches.Close(),
		s.extensions.Close(),
		s.values.Close(),
	)
}

// GetMemoryFootprint provides sizes of individual components of the state in the memory
func (s *Forest) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	mf.AddChild("accounts", s.accounts.GetMemoryFootprint())
	mf.AddChild("branches", s.branches.GetMemoryFootprint())
	mf.AddChild("extensions", s.extensions.GetMemoryFootprint())
	mf.AddChild("values", s.values.GetMemoryFootprint())
	s.nodeCacheMutex.Lock()
	defer s.nodeCacheMutex.Unlock()
	mf.AddChild("cache", s.nodeCache.GetDynamicMemoryFootprint(func(node *shared.Shared[Node]) uintptr {
		handle := node.GetReadHandle()
		defer handle.Release()
		if _, ok := handle.Get().(*AccountNode); ok {
			return unsafe.Sizeof(AccountNode{})
		}
		if _, ok := handle.Get().(*BranchNode); ok {
			return unsafe.Sizeof(BranchNode{})
		}
		if _, ok := handle.Get().(EmptyNode); ok {
			return unsafe.Sizeof(EmptyNode{})
		}
		if _, ok := handle.Get().(*ExtensionNode); ok {
			return unsafe.Sizeof(ExtensionNode{})
		}
		if _, ok := handle.Get().(*ValueNode); ok {
			return unsafe.Sizeof(ValueNode{})
		}
		panic(fmt.Sprintf("unexpected node type: %v", reflect.TypeOf(node)))
	}))
	mf.AddChild("hashedKeysCache", s.keyHasher.GetMemoryFootprint())
	mf.AddChild("hashedAddressesCache", s.addressHasher.GetMemoryFootprint())
	return mf
}

// Dump prints the content of the Trie to the console. Mainly intended for debugging.
func (s *Forest) Dump(rootId NodeId) {
	root, err := s.getViewAccess(rootId)
	if err != nil {
		fmt.Printf("Failed to fetch root: %v", err)
		return
	}
	defer root.Release()
	root.Get().Dump(s, rootId, "")
}

// Check verifies internal invariants of the Trie instance. If the trie is
// self-consistent, nil is returned and the Trie is read to be accessed. If
// errors are detected, the Trie is to be considered in an invalid state and
// the behavior of all other operations is undefined.
func (s *Forest) Check(rootId NodeId) error {
	root, err := s.getViewAccess(rootId)
	if err != nil {
		return err
	}
	defer root.Release()
	return root.Get().Check(s, rootId, make([]Nibble, 0, common.AddressSize*2))
}

// -- NodeManager interface --

func (s *Forest) getConfig() MptConfig {
	return s.config
}

func (s *Forest) getSharedNode(id NodeId) (*shared.Shared[Node], error) {
	// Start by checking the node cache.
	s.nodeCacheMutex.Lock()
	defer s.nodeCacheMutex.Unlock()
	res, found := s.nodeCache.Get(id)
	if found {
		return res, nil
	}

	// Check whether the node is in the write buffer.
	// Note: although Cancel is thread safe, it is important to make sure
	// that this part is only run by a single thread to avoid one thread
	// recovering a node from the buffer and another fetching it from the
	// storage. This synchronization is currently ensured by the
	// nodeCacheMutex acquired above and held until the end of the function.
	res, found = s.writeBuffer.Cancel(id)
	if found {
		s.addToCacheHoldingCacheMutexLock(id, res)
		// Since the write was canceled, the fetched node is
		// still dirty (only dirty nodes are in the buffer).
		s.dirtyMutex.Lock()
		s.dirty[id] = struct{}{}
		s.dirtyMutex.Unlock()
		return res, nil
	}

	// Load the node from persistent storage.
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
	if s.storageMode == Immutable {
		node.MarkFrozen()
	}

	instance := shared.MakeShared[Node](node)
	s.addToCacheHoldingCacheMutexLock(id, instance)
	return instance, nil
}

func (s *Forest) getReadAccess(id NodeId) (shared.ReadHandle[Node], error) {
	instance, err := s.getSharedNode(id)
	if err != nil {
		return shared.ReadHandle[Node]{}, err
	}
	return instance.GetReadHandle(), nil
}

func (s *Forest) getViewAccess(id NodeId) (shared.ViewHandle[Node], error) {
	instance, err := s.getSharedNode(id)
	if err != nil {
		return shared.ViewHandle[Node]{}, err
	}
	return instance.GetViewHandle(), nil
}

func (s *Forest) getHashAccess(id NodeId) (shared.HashHandle[Node], error) {
	instance, err := s.getSharedNode(id)
	if err != nil {
		return shared.HashHandle[Node]{}, err
	}
	return instance.GetHashHandle(), nil
}

func (s *Forest) getWriteAccess(id NodeId) (shared.WriteHandle[Node], error) {
	instance, err := s.getSharedNode(id)
	if err != nil {
		return shared.WriteHandle[Node]{}, err
	}
	return instance.GetWriteHandle(), nil
}

func (s *Forest) getMutableNodeByPath(root NodeId, path NodePath) (shared.WriteHandle[Node], error) {
	// Navigate down the trie using read access.
	next := root
	last := shared.ReadHandle[Node]{}
	lastValid := false
	for i := 0; i < path.Length(); i++ {
		cur, err := s.getReadAccess(next)
		if lastValid {
			last.Release()
		}
		if err != nil {
			return shared.WriteHandle[Node]{}, err
		}
		last = cur
		lastValid = true
		switch n := cur.Get().(type) {
		case *BranchNode:
			next = n.children[path.Get(byte(i))]
		case *AccountNode:
			next = n.storage
		case *ExtensionNode:
			next = n.next
		default:
			if lastValid {
				last.Release()
			}
			return shared.WriteHandle[Node]{}, fmt.Errorf("no node for path: %v", path)
		}
	}

	// The last step requires write access.
	res, err := s.getWriteAccess(next)
	if lastValid {
		last.Release()
	}
	return res, err
}

func (s *Forest) addToCache(id NodeId, node *shared.Shared[Node]) {
	s.nodeCacheMutex.Lock()
	defer s.nodeCacheMutex.Unlock()
	s.addToCacheHoldingCacheMutexLock(id, node)
}

func (s *Forest) addToCacheHoldingCacheMutexLock(id NodeId, node *shared.Shared[Node]) {
	if s.nodeCacheMutex.TryLock() {
		panic("caller must hold node cache lock")
	}
	evictedId, evictedNode, evicted := s.nodeCache.Set(id, node)
	if !evicted {
		return
	}

	// Clean nodes can be ignored, dirty nodes need to be written.
	s.dirtyMutex.Lock()
	_, dirty := s.dirty[evictedId]
	if !dirty {
		s.dirtyMutex.Unlock()
		return
	}
	delete(s.dirty, evictedId)
	s.dirtyMutex.Unlock()

	// Enqueue evicted node for asynchronous write to file.
	s.writeBuffer.Add(evictedId, evictedNode)
}

func (s *Forest) flushNode(id NodeId, node Node) error {
	// Note: flushing nodes in Archive mode will implicitly freeze them,
	// since after the reload they will be considered frozen. This may
	// cause temporary states between updates to be accidentally frozen,
	// leaving unreferenced nodes in the archive, but it is not causing
	// correctness issues. However, if the node-cache size is sufficiently
	// large, such cases should be rare. Nevertheless, a warning is
	// printed here to get informed if this changes in the future.
	if printWarningDefaultNodeFreezing && s.storageMode == Immutable && !node.IsFrozen() {
		log.Printf("WARNING: non-frozen node flushed to disk causing implicit freeze")
	}

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

func (s *Forest) createAccount() (NodeId, shared.WriteHandle[Node], error) {
	i, err := s.accounts.New()
	if err != nil {
		return 0, shared.WriteHandle[Node]{}, err
	}
	id := AccountId(i)
	node := new(AccountNode)
	instance := shared.MakeShared[Node](node)
	s.addToCache(id, instance)
	return id, instance.GetWriteHandle(), err
}

func (s *Forest) createBranch() (NodeId, shared.WriteHandle[Node], error) {
	i, err := s.branches.New()
	if err != nil {
		return 0, shared.WriteHandle[Node]{}, err
	}
	id := BranchId(i)
	node := new(BranchNode)
	instance := shared.MakeShared[Node](node)
	s.addToCache(id, instance)
	return id, instance.GetWriteHandle(), err
}

func (s *Forest) createExtension() (NodeId, shared.WriteHandle[Node], error) {
	i, err := s.extensions.New()
	if err != nil {
		return 0, shared.WriteHandle[Node]{}, err
	}
	id := ExtensionId(i)
	node := new(ExtensionNode)
	instance := shared.MakeShared[Node](node)
	s.addToCache(id, instance)
	return id, instance.GetWriteHandle(), err
}

func (s *Forest) createValue() (NodeId, shared.WriteHandle[Node], error) {
	i, err := s.values.New()
	if err != nil {
		return 0, shared.WriteHandle[Node]{}, err
	}
	id := ValueId(i)
	node := new(ValueNode)
	instance := shared.MakeShared[Node](node)
	s.addToCache(id, instance)
	return id, instance.GetWriteHandle(), err
}

func (s *Forest) update(id NodeId, node shared.WriteHandle[Node]) error {
	// all needed here is to register the modified node as dirty
	s.dirtyMutex.Lock()
	s.dirty[id] = struct{}{}
	s.dirtyMutex.Unlock()
	return nil
}

func (s *Forest) updateHash(id NodeId, node shared.HashHandle[Node]) error {
	// all needed here is to register the modified node as dirty
	s.dirtyMutex.Lock()
	s.dirty[id] = struct{}{}
	s.dirtyMutex.Unlock()
	return nil
}

func (s *Forest) release(id NodeId) error {
	s.nodeCacheMutex.Lock()
	s.nodeCache.Remove(id)
	s.nodeCacheMutex.Unlock()

	s.dirtyMutex.Lock()
	delete(s.dirty, id)
	s.dirtyMutex.Unlock()

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
	switch config.HashStorageLocation {
	case HashStoredWithParent:
		if config.TrackSuffixLengthsInLeafNodes {
			return AccountNodeWithPathLengthEncoderWithChildHash{},
				BranchNodeEncoderWithChildHashes{},
				ExtensionNodeEncoderWithChildHash{},
				ValueNodeWithPathLengthEncoderWithoutNodeHash{}
		}
		return AccountNodeEncoderWithChildHash{},
			BranchNodeEncoderWithChildHashes{},
			ExtensionNodeEncoderWithChildHash{},
			ValueNodeEncoderWithoutNodeHash{}
	case HashStoredWithNode:
		if config.TrackSuffixLengthsInLeafNodes {
			return AccountNodeWithPathLengthEncoderWithNodeHash{},
				BranchNodeEncoderWithNodeHash{},
				ExtensionNodeEncoderWithNodeHash{},
				ValueNodeWithPathLengthEncoderWithNodeHash{}
		}
		return AccountNodeEncoderWithNodeHash{},
			BranchNodeEncoderWithNodeHash{},
			ExtensionNodeEncoderWithNodeHash{},
			ValueNodeEncoderWithNodeHash{}
	default:
		panic(fmt.Sprintf("unknown mode: %v", config.HashStorageLocation))
	}

}

type writeBufferSink struct {
	forest *Forest
}

func (s writeBufferSink) Write(id NodeId, handle shared.ViewHandle[Node]) error {
	return s.forest.flushNode(id, handle.Get())
}

// -- Forest metadata --

// ForestMetadata is the helper type to read and write metadata from/to the disk.
type ForestMetadata struct {
	Configuration string
	Mutable       bool
}

// ReadForestMetadata parses the content of the given file if it exists or returns
// a default-initialized metadata struct if there is no such file.
func ReadForestMetadata(filename string) (ForestMetadata, bool, error) {

	// If there is no file, initialize and return default metadata.
	if _, err := os.Stat(filename); err != nil {
		return ForestMetadata{}, false, nil
	}

	// If the file exists, parse it and return its content.
	data, err := os.ReadFile(filename)
	if err != nil {
		return ForestMetadata{}, false, err
	}

	var meta ForestMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return meta, false, err
	}
	return meta, true, nil
}

package mpt

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
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
	NodeRef NodeReference
	Hash    common.Hash
}

// ForestConfig summarizes forest instance configuration options that affect
// the functional and non-functional properties of a forest but do not change
// the on-disk format.
type ForestConfig struct {
	Mode          StorageMode // whether to perform destructive or constructive updates
	CacheCapacity int         // the maximum number of nodes retained in memory
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
	nodeCache NodeCache

	// The hasher managing node hashes for this forest.
	hasher hasher

	// Cached hashers for keys and addresses (thread safe).
	keyHasher     CachedHasher[common.Key]
	addressHasher CachedHasher[common.Address]

	// A buffer for asynchronously writing nodes to files.
	writeBuffer WriteBuffer

	// A mutex required by the 'getSharedNode' function to synchronize access.
	getSharedNodeMutex sync.Mutex

	// Utilities to manage a background worker releasing nodes.
	releaseQueue chan<- NodeId   // send EmptyId to trigger sync signal
	releaseSync  <-chan struct{} // signaled whenever the release worker reaches a sync point
	releaseError <-chan error    // errors detected by the release worker
	releaseDone  <-chan struct{} // closed when the release worker is done
}

func OpenInMemoryForest(directory string, mptConfig MptConfig, forestConfig ForestConfig) (*Forest, error) {
	if _, err := checkForestMetadata(directory, mptConfig, forestConfig.Mode); err != nil {
		return nil, err
	}

	success := false
	accountEncoder, branchEncoder, extensionEncoder, valueEncoder := getEncoder(mptConfig)
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
	return makeForest(mptConfig, directory, branches, extensions, accounts, values, forestConfig)
}

func OpenFileForest(directory string, mptConfig MptConfig, forestConfig ForestConfig) (*Forest, error) {
	if _, err := checkForestMetadata(directory, mptConfig, forestConfig.Mode); err != nil {
		return nil, err
	}

	success := false
	accountEncoder, branchEncoder, extensionEncoder, valueEncoder := getEncoder(mptConfig)
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
	return makeForest(mptConfig, directory, branches, extensions, accounts, values, forestConfig)
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
	mptConfig MptConfig,
	directory string,
	branches stock.Stock[uint64, BranchNode],
	extensions stock.Stock[uint64, ExtensionNode],
	accounts stock.Stock[uint64, AccountNode],
	values stock.Stock[uint64, ValueNode],
	forestConfig ForestConfig,
) (*Forest, error) {
	releaseQueue := make(chan NodeId, 1<<16) // NodeIds are small and a large buffer increases resilience.
	releaseSync := make(chan struct{})
	releaseError := make(chan error, 1)
	releaseDone := make(chan struct{})

	res := &Forest{
		config:        mptConfig,
		branches:      synced.Sync(branches),
		extensions:    synced.Sync(extensions),
		accounts:      synced.Sync(accounts),
		values:        synced.Sync(values),
		storageMode:   forestConfig.Mode,
		nodeCache:     NewNodeCache(forestConfig.CacheCapacity),
		hasher:        mptConfig.Hashing.createHasher(),
		keyHasher:     NewKeyHasher(),
		addressHasher: NewAddressHasher(),
		releaseQueue:  releaseQueue,
		releaseSync:   releaseSync,
		releaseError:  releaseError,
		releaseDone:   releaseDone,
	}

	// Run a background worker releasing entire tries of nodes on demand.
	go func() {
		defer close(releaseDone)
		defer close(releaseError)
		defer close(releaseSync)
		for id := range releaseQueue {
			if id.IsEmpty() {
				releaseSync <- struct{}{}
			} else {
				ref := NewNodeReference(id)
				handle, err := res.getWriteAccess(&ref)
				if err != nil {
					releaseError <- err
					return
				}
				err = handle.Get().Release(res, &ref, handle)
				handle.Release()
				if err != nil {
					releaseError <- err
					return
				}
			}
		}
	}()

	res.writeBuffer = makeWriteBuffer(writeBufferSink{res}, 1024)
	return res, nil
}

func (s *Forest) GetAccountInfo(rootRef *NodeReference, addr common.Address) (AccountInfo, bool, error) {
	handle, err := s.getReadAccess(rootRef)
	if err != nil {
		return AccountInfo{}, false, err
	}
	defer handle.Release()
	path := AddressToNibblePath(addr, s)
	return handle.Get().GetAccount(s, addr, path[:])
}

func (s *Forest) SetAccountInfo(rootRef *NodeReference, addr common.Address, info AccountInfo) (NodeReference, error) {
	root, err := s.getWriteAccess(rootRef)
	if err != nil {
		return NodeReference{}, err
	}
	defer root.Release()
	path := AddressToNibblePath(addr, s)
	newRoot, _, err := root.Get().SetAccount(s, rootRef, root, addr, path[:], info)
	if err != nil {
		return NodeReference{}, err
	}
	return newRoot, nil
}

func (s *Forest) GetValue(rootRef *NodeReference, addr common.Address, key common.Key) (common.Value, error) {
	root, err := s.getReadAccess(rootRef)
	if err != nil {
		return common.Value{}, err
	}
	defer root.Release()
	path := AddressToNibblePath(addr, s)
	value, _, err := root.Get().GetSlot(s, addr, path[:], key)
	return value, err
}

func (s *Forest) SetValue(rootRef *NodeReference, addr common.Address, key common.Key, value common.Value) (NodeReference, error) {
	root, err := s.getWriteAccess(rootRef)
	if err != nil {
		return NodeReference{}, err
	}
	defer root.Release()
	path := AddressToNibblePath(addr, s)
	newRoot, _, err := root.Get().SetSlot(s, rootRef, root, addr, path[:], key, value)
	if err != nil {
		return NodeReference{}, err
	}
	return newRoot, nil
}

func (s *Forest) ClearStorage(rootRef *NodeReference, addr common.Address) error {
	root, err := s.getWriteAccess(rootRef)
	if err != nil {
		return err
	}
	defer root.Release()
	path := AddressToNibblePath(addr, s)
	_, _, err = root.Get().ClearStorage(s, rootRef, root, addr, path[:])
	return err
}

func (s *Forest) VisitTrie(rootRef *NodeReference, visitor NodeVisitor) error {
	root, err := s.getViewAccess(rootRef)
	if err != nil {
		return err
	}
	defer root.Release()
	_, err = root.Get().Visit(s, rootRef, 0, visitor)
	return err
}

func (s *Forest) updateHashesFor(ref *NodeReference) (common.Hash, *NodeHashes, error) {
	return s.hasher.updateHashes(ref, s)
}

func (s *Forest) setHashesFor(root *NodeReference, hashes *NodeHashes) error {
	for _, cur := range hashes.GetHashes() {
		write, err := s.getMutableNodeByPath(root, cur.Path)
		if err != nil {
			return err
		}
		write.Get().SetHash(cur.Hash)
		write.Release()
	}
	return nil
}

func (s *Forest) getHashFor(ref *NodeReference) (common.Hash, error) {
	return s.hasher.getHash(ref, s)
}

func (s *Forest) hashKey(key common.Key) common.Hash {
	hash, _ := s.keyHasher.Hash(key)
	return hash
}

func (s *Forest) hashAddress(address common.Address) common.Hash {
	hash, _ := s.addressHasher.Hash(address)
	return hash
}

func (f *Forest) Freeze(ref *NodeReference) error {
	if f.storageMode != Immutable {
		return fmt.Errorf("node-freezing only supported in archive mode")
	}
	root, err := f.getWriteAccess(ref)
	if err != nil {
		return err
	}
	defer root.Release()
	return root.Get().Freeze(f, root)
}

func (s *Forest) Flush() error {
	// Wait for releaser to finish its current tasks.
	s.releaseQueue <- EmptyId() // signals a sync request
	<-s.releaseSync

	// Consume potential release errors.
	errs := []error{s.collectReleaseWorkerErrors()}

	// Get snapshot of set of dirty Node IDs.
	ids := make([]NodeId, 0, 1<<16)
	s.nodeCache.ForEach(func(id NodeId, node *shared.Shared[Node]) {
		handle := node.GetViewHandle()
		dirty := handle.Get().IsDirty()
		handle.Release()
		if dirty {
			ids = append(ids, id)
		}
	})

	// Flush dirty keys in order (to avoid excessive seeking).
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		ref := NewNodeReference(id)
		node, present := s.nodeCache.Get(&ref)
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
	var errs []error
	errs = append(errs, s.Flush())

	// shut down release worker
	close(s.releaseQueue)
	<-s.releaseDone

	// Consume potential release errors.
	errs = append(errs, s.collectReleaseWorkerErrors())

	return errors.Join(
		errors.Join(errs...),
		s.writeBuffer.Close(),
		s.accounts.Close(),
		s.branches.Close(),
		s.extensions.Close(),
		s.values.Close(),
	)
}

func (s *Forest) collectReleaseWorkerErrors() error {
	var errs []error
loop:
	for {
		select {
		case err, open := <-s.releaseError:
			if !open {
				break loop
			}
			if err != nil {
				errs = append(errs, err)
			}
		default:
			break loop
		}
	}
	return errors.Join(errs...)
}

// GetMemoryFootprint provides sizes of individual components of the state in the memory
func (s *Forest) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	mf.AddChild("accounts", s.accounts.GetMemoryFootprint())
	mf.AddChild("branches", s.branches.GetMemoryFootprint())
	mf.AddChild("extensions", s.extensions.GetMemoryFootprint())
	mf.AddChild("values", s.values.GetMemoryFootprint())
	mf.AddChild("cache", s.nodeCache.GetMemoryFootprint())
	mf.AddChild("hashedKeysCache", s.keyHasher.GetMemoryFootprint())
	mf.AddChild("hashedAddressesCache", s.addressHasher.GetMemoryFootprint())
	return mf
}

// Dump prints the content of the Trie to the console. Mainly intended for debugging.
func (s *Forest) Dump(rootRef *NodeReference) {
	root, err := s.getViewAccess(rootRef)
	if err != nil {
		fmt.Printf("Failed to fetch root: %v", err)
		return
	}
	defer root.Release()
	root.Get().Dump(s, rootRef, "")
}

// Check verifies internal invariants of the Trie instance. If the trie is
// self-consistent, nil is returned and the Trie is ready to be accessed. If
// errors are detected, the Trie is to be considered in an invalid state and
// the behavior of all other operations is undefined.
func (s *Forest) Check(rootRef *NodeReference) error {
	return s.CheckAll([]*NodeReference{rootRef})
}

// CheckAll verifies internal invariants of a set of Trie instances rooted by
// the given nodes. It is a generalization of the Check() function.
func (s *Forest) CheckAll(rootRefs []*NodeReference) error {
	return CheckForest(s, rootRefs)
}

// -- NodeManager interface --

func (s *Forest) getConfig() MptConfig {
	return s.config
}

func (s *Forest) getSharedNode(ref *NodeReference) (*shared.Shared[Node], error) {
	res, found := s.nodeCache.Get(ref)
	if found {
		return res, nil
	}

	// Check whether the node is in the write buffer.
	// Note: although Cancel is thread safe, it is important to make sure
	// that this part is only run by a single thread to avoid one thread
	// recovering a node from the buffer and another fetching it from the
	// storage. This synchronization is currently ensured by acquiring the
	// getSharedNodeMutex and holding it until the end of the function.
	// Using a global lock that does not differentiate between node IDs may
	// cause performance issues since it is delaying unrelated lookup
	// operations. However, the impact should be small since cache misses
	// should be infrequent enough. Unless it is detected in CPU profiles
	// and traces, this lock should be fine.
	s.getSharedNodeMutex.Lock()
	defer s.getSharedNodeMutex.Unlock()

	id := ref.Id()
	res, found = s.writeBuffer.Cancel(id)
	if found {
		masterCopy, _ := s.addToCache(ref, res)
		if masterCopy != res {
			panic("failed to reinstate element from write buffer")
		}
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

	// Everything loaded from the stock is in sync and thus clean.
	node.MarkClean()

	// Everything that is loaded from an archive is to be considered
	// frozen, and thus immutable.
	if s.storageMode == Immutable {
		node.MarkFrozen()
	}

	// if there has been a concurrent fetch, use the other value
	instance, _ := s.addToCache(ref, shared.MakeShared[Node](node))
	return instance, nil
}

func getAccess[H any](
	f *Forest,
	ref *NodeReference,
	getAccess func(*shared.Shared[Node]) H,
	release func(H),
	def H,
) (H, error) {
	instance, err := f.getSharedNode(ref)
	if err != nil {
		return def, err
	}
	for {
		// Obtain needed access and make sure the instance access was obtained
		// for is still valid (by re-fetching the instance and check that it
		// has not changed). This is not super efficient, and may be improved
		// in the future by merging this functionality into called operations.
		res := getAccess(instance)
		if actual, err := f.getSharedNode(ref); err == nil && actual == instance {
			return res, nil
		} else if err != nil {
			release(res)
			return def, err
		} else {
			release(res)
			instance = actual
		}
	}
}

func (s *Forest) getReadAccess(ref *NodeReference) (shared.ReadHandle[Node], error) {
	return getAccess(s, ref,
		func(s *shared.Shared[Node]) shared.ReadHandle[Node] {
			return s.GetReadHandle()
		},
		func(p shared.ReadHandle[Node]) {
			p.Release()
		},
		shared.ReadHandle[Node]{},
	)
}

func (s *Forest) getViewAccess(ref *NodeReference) (shared.ViewHandle[Node], error) {
	return getAccess(s, ref,
		func(s *shared.Shared[Node]) shared.ViewHandle[Node] {
			return s.GetViewHandle()
		},
		func(p shared.ViewHandle[Node]) {
			p.Release()
		},
		shared.ViewHandle[Node]{},
	)
}

func (s *Forest) getHashAccess(ref *NodeReference) (shared.HashHandle[Node], error) {
	return getAccess(s, ref,
		func(s *shared.Shared[Node]) shared.HashHandle[Node] {
			return s.GetHashHandle()
		},
		func(p shared.HashHandle[Node]) {
			p.Release()
		},
		shared.HashHandle[Node]{},
	)
}

func (f *Forest) getWriteAccess(ref *NodeReference) (shared.WriteHandle[Node], error) {
	return getAccess(f, ref,
		func(s *shared.Shared[Node]) shared.WriteHandle[Node] {
			// When gaining write access to nodes, they need to be touched to make sure
			// modified nodes are at the head of the cache's LRU queue to be evicted last.
			f.nodeCache.Touch(ref)
			return s.GetWriteHandle()
		},
		func(p shared.WriteHandle[Node]) {
			p.Release()
		},
		shared.WriteHandle[Node]{},
	)
}

func (s *Forest) getMutableNodeByPath(root *NodeReference, path NodePath) (shared.WriteHandle[Node], error) {
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
			next = &n.children[path.Get(byte(i))]
		case *AccountNode:
			next = &n.storage
		case *ExtensionNode:
			next = &n.next
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

func (s *Forest) addToCache(ref *NodeReference, node *shared.Shared[Node]) (value *shared.Shared[Node], present bool) {
	current, present, evictedId, evictedNode, evicted := s.nodeCache.GetOrSet(ref, node)
	if present {
		// If a present element is re-used, it needs to be touched to be at the
		// head of the cache's LRU queue -- just like a newly inserted node
		// would be. Methods like createBranch depend on this to be covered here.
		s.nodeCache.Touch(ref)
	}
	if !evicted {
		return current, present
	}

	// Clean nodes can be ignored, dirty nodes need to be written.
	if handle, ok := evictedNode.TryGetViewHandle(); ok {
		dirty := handle.Get().IsDirty()
		handle.Release()
		if !dirty {
			return current, present
		}
	}

	// Enqueue evicted node for asynchronous write to file.
	s.writeBuffer.Add(evictedId, evictedNode)
	return current, present
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

func (s *Forest) createAccount() (NodeReference, shared.WriteHandle[Node], error) {
	i, err := s.accounts.New()
	if err != nil {
		return NodeReference{}, shared.WriteHandle[Node]{}, err
	}
	ref := NewNodeReference(AccountId(i))
	node := new(AccountNode)
	instance, present := s.addToCache(&ref, shared.MakeShared[Node](node))
	if present {
		write := instance.GetWriteHandle()
		*write.Get().(*AccountNode) = *node
		write.Release()
	}
	return ref, instance.GetWriteHandle(), err
}

func (s *Forest) createBranch() (NodeReference, shared.WriteHandle[Node], error) {
	i, err := s.branches.New()
	if err != nil {
		return NodeReference{}, shared.WriteHandle[Node]{}, err
	}
	ref := NewNodeReference(BranchId(i))
	node := new(BranchNode)
	instance, present := s.addToCache(&ref, shared.MakeShared[Node](node))
	if present {
		write := instance.GetWriteHandle()
		*write.Get().(*BranchNode) = *node
		write.Release()
	}
	return ref, instance.GetWriteHandle(), err
}

func (s *Forest) createExtension() (NodeReference, shared.WriteHandle[Node], error) {
	i, err := s.extensions.New()
	if err != nil {
		return NodeReference{}, shared.WriteHandle[Node]{}, err
	}
	ref := NewNodeReference(ExtensionId(i))
	node := new(ExtensionNode)
	instance, present := s.addToCache(&ref, shared.MakeShared[Node](node))
	if present {
		write := instance.GetWriteHandle()
		*write.Get().(*ExtensionNode) = *node
		write.Release()
	}
	return ref, instance.GetWriteHandle(), err
}

func (s *Forest) createValue() (NodeReference, shared.WriteHandle[Node], error) {
	i, err := s.values.New()
	if err != nil {
		return NodeReference{}, shared.WriteHandle[Node]{}, err
	}
	ref := NewNodeReference(ValueId(i))
	node := new(ValueNode)
	instance, present := s.addToCache(&ref, shared.MakeShared[Node](node))
	if present {
		write := instance.GetWriteHandle()
		*write.Get().(*ValueNode) = *node
		write.Release()
	}
	return ref, instance.GetWriteHandle(), err
}

func (s *Forest) release(id NodeId) error {
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

func (s *Forest) releaseTrieAsynchronous(ref NodeReference) {
	id := ref.Id()
	if !id.IsEmpty() { // empty Id is used for signalling sync requests
		s.releaseQueue <- id
	}
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

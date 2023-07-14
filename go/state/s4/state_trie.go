package s4

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
)

type StateTrie struct {
	// The root node of the Trie.
	root NodeId

	// The file name for storing trie metadata.
	metadatafile string

	// The stock containers managing individual node types.
	branches   stock.Stock[uint32, BranchNode]
	extensions stock.Stock[uint32, ExtensionNode]
	accounts   stock.Stock[uint32, AccountNode]
	values     stock.Stock[uint32, ValueNode]

	// A unified cache for all node types.
	nodeCache *common.Cache[NodeId, Node]

	// The set of dirty nodes. Nodes are dirty if there in-memory
	// state does not match their on-disk content.
	dirty map[NodeId]struct{}

	// The hasher used to compute state hashes.
	hasher Hasher

	// A store for hashes.
	hashes      HashStore
	dirtyHashes map[NodeId]struct{}
}

// The number of elements to retain in the node cache.
const cacheCapacity = 100_000_000

func OpenInMemoryTrie(directory string) (*StateTrie, error) {
	success := false
	branches, err := memory.OpenStock[uint32, BranchNode](BranchNodeEncoder{}, directory+"/branches")
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			branches.Close()
		}
	}()
	extensions, err := memory.OpenStock[uint32, ExtensionNode](ExtensionNodeEncoder{}, directory+"/extensions")
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			extensions.Close()
		}
	}()
	accounts, err := memory.OpenStock[uint32, AccountNode](AccountNodeEncoder{}, directory+"/accounts")
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			accounts.Close()
		}
	}()
	values, err := memory.OpenStock[uint32, ValueNode](ValueNodeEncoder{}, directory+"/values")
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
	return makeTrie(directory, branches, extensions, accounts, values, hashes)
}

func OpenFileTrie(directory string) (*StateTrie, error) {
	success := false
	branches, err := file.OpenStock[uint32, BranchNode](BranchNodeEncoder{}, directory+"/branches")
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			branches.Close()
		}
	}()
	extensions, err := file.OpenStock[uint32, ExtensionNode](ExtensionNodeEncoder{}, directory+"/extensions")
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			extensions.Close()
		}
	}()
	accounts, err := file.OpenStock[uint32, AccountNode](AccountNodeEncoder{}, directory+"/accounts")
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			accounts.Close()
		}
	}()
	values, err := file.OpenStock[uint32, ValueNode](ValueNodeEncoder{}, directory+"/values")
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
	return makeTrie(directory, branches, extensions, accounts, values, hashes)
}

func makeTrie(
	directory string,
	branches stock.Stock[uint32, BranchNode],
	extensions stock.Stock[uint32, ExtensionNode],
	accounts stock.Stock[uint32, AccountNode],
	values stock.Stock[uint32, ValueNode],
	hashes HashStore,
) (*StateTrie, error) {
	// Parse metadata file.
	metadatafile := directory + "/meta.json"
	metadata, err := readMetadata(metadatafile)
	if err != nil {
		return nil, err
	}
	return &StateTrie{
		root:         metadata.RootNode,
		metadatafile: metadatafile,
		branches:     branches,
		extensions:   extensions,
		accounts:     accounts,
		values:       values,
		nodeCache:    common.NewCache[NodeId, Node](cacheCapacity),
		dirty:        map[NodeId]struct{}{},
		hasher:       &DirectHasher{},
		hashes:       hashes,
		dirtyHashes:  map[NodeId]struct{}{},
	}, nil
}

func (s *StateTrie) GetAccountInfo(addr common.Address) (AccountInfo, bool, error) {
	root, err := s.getNode(s.root)
	if err != nil {
		return AccountInfo{}, false, err
	}
	path := addressToNibbles(&addr)
	return root.GetAccount(s, &addr, path[:])
}

func (s *StateTrie) SetAccountInfo(addr common.Address, info AccountInfo) error {
	root, err := s.getNode(s.root)
	if err != nil {
		return err
	}
	path := addressToNibbles(&addr)
	newRoot, _, err := root.SetAccount(s, s.root, &addr, path[:], &info)
	if err != nil {
		return err
	}
	s.root = newRoot
	return nil
}

func (s *StateTrie) GetValue(addr common.Address, key common.Key) (common.Value, error) {
	root, err := s.getNode(s.root)
	if err != nil {
		return common.Value{}, err
	}
	path := addressToNibbles(&addr)
	value, _, err := root.GetSlot(s, &addr, path[:], &key)
	return value, err
}

func (s *StateTrie) SetValue(addr common.Address, key common.Key, value common.Value) error {
	root, err := s.getNode(s.root)
	if err != nil {
		return err
	}
	path := addressToNibbles(&addr)
	newRoot, _, err := root.SetSlot(s, s.root, &addr, path[:], &key, &value)
	if err != nil {
		return err
	}
	s.root = newRoot
	return nil
}

func (s *StateTrie) ClearStorage(addr common.Address) error {
	root, err := s.getNode(s.root)
	if err != nil {
		return err
	}
	path := addressToNibbles(&addr)
	return root.ClearStorage(s, &addr, path[:])
}

func (s *StateTrie) GetHash() (common.Hash, error) {
	return s.GetHashFor(s.root)
}

func (s *StateTrie) GetHashFor(id NodeId) (common.Hash, error) {
	// The empty node is forced to have the empty hash.
	if id.IsEmpty() {
		return common.Hash{}, nil
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
	hash, err := s.hasher.GetHash(node, s)
	if err != nil {
		return common.Hash{}, err
	}
	if err := s.hashes.Set(id, hash); err != nil {
		return hash, err
	}
	delete(s.dirtyHashes, id)
	return hash, nil
}

func (s *StateTrie) Flush() error {
	// Update on-disk meta-data.
	metadata, err := json.Marshal(metadata{
		RootNode: s.root,
	})
	if err != nil {
		return err
	}
	if err := os.WriteFile(s.metadatafile, metadata, 0600); err != nil {
		return err
	}

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

	// Update hashes for dirty nodes.
	dirty := make([]NodeId, len(s.dirty))
	for id := range s.dirty {
		dirty = append(dirty, id)
	}
	for _, id := range dirty {
		if _, err := s.GetHashFor(id); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(
		errors.Join(errs...),
		s.accounts.Flush(),
		s.branches.Flush(),
		s.extensions.Flush(),
		s.values.Flush(),
		s.hashes.Flush(),
	)
}

func (s *StateTrie) Close() error {
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
func (s *StateTrie) GetMemoryFootprint() *common.MemoryFootprint {
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
	return mf
}

// Dump prints the content of the Trie to the console. Mainly intended for debugging.
func (s *StateTrie) Dump() {
	root, err := s.getNode(s.root)
	if err != nil {
		fmt.Printf("Failed to fetch root: %v", err)
	} else {
		root.Dump(s, s.root, "")
	}
}

// Check verifies internal invariants of the Trie instance. If the trie is
// self-consistent, nil is returned and the Trie is read to be accessed. If
// errors are detected, the Trie is to be considered in an invalid state and
// the behaviour of all other operations is undefined.
func (s *StateTrie) Check() error {
	root, err := s.getNode(s.root)
	if err != nil {
		return err
	}
	return root.Check(s, make([]Nibble, 0, common.AddressSize*2))
}

// -- NodeManager interface --

func (s *StateTrie) getNode(id NodeId) (Node, error) {
	// Start by checking the node cache.
	res, found := s.nodeCache.Get(id)
	if found {
		return res, nil
	}

	// Load the node from peristent storage.
	var node Node
	var err error
	if id.IsValue() {
		node, err = s.values.Get(id.Index())
	} else if id.IsAccount() {
		node, err = s.accounts.Get(id.Index())
	} else if id.IsBranch() {
		node, err = s.branches.Get(id.Index())
	} else if id.IsExtension() {
		node, err = s.extensions.Get(id.Index())
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

	if err := s.addToCache(id, node); err != nil {
		return nil, err
	}
	return node, nil
}

func (s *StateTrie) addToCache(id NodeId, node Node) error {
	if evictedId, evictedNode, evicted := s.nodeCache.Set(id, node); evicted {
		// TODO: perform update asynchroniously.
		if err := s.flush(evictedId, evictedNode); err != nil {
			return err
		}
	}
	return nil
}

func (s *StateTrie) flush(id NodeId, node Node) error {
	if _, dirty := s.dirty[id]; !dirty {
		return nil
	}
	delete(s.dirty, id)
	if id.IsValue() {
		return s.values.Set(id.Index(), node.(*ValueNode))
	} else if id.IsAccount() {
		return s.accounts.Set(id.Index(), node.(*AccountNode))
	} else if id.IsBranch() {
		return s.branches.Set(id.Index(), node.(*BranchNode))
	} else if id.IsExtension() {
		return s.extensions.Set(id.Index(), node.(*ExtensionNode))
	} else if id.IsEmpty() {
		return nil
	} else {
		return fmt.Errorf("unknown node ID: %v", id)
	}
}

func (s *StateTrie) createAccount() (NodeId, *AccountNode, error) {
	i, node, err := s.accounts.New()
	if err != nil {
		return 0, nil, err
	}
	id := AccountId(i)
	if err := s.addToCache(id, node); err != nil {
		return 0, nil, err
	}
	return id, node, err
}

func (s *StateTrie) createBranch() (NodeId, *BranchNode, error) {
	i, node, err := s.branches.New()
	if err != nil {
		return 0, nil, err
	}
	id := BranchId(i)
	if err := s.addToCache(id, node); err != nil {
		return 0, nil, err
	}
	return id, node, err
}

func (s *StateTrie) createExtension() (NodeId, *ExtensionNode, error) {
	i, node, err := s.extensions.New()
	if err != nil {
		return 0, nil, err
	}
	id := ExtensionId(i)
	if err := s.addToCache(id, node); err != nil {
		return 0, nil, err
	}
	return id, node, err
}

func (s *StateTrie) createValue() (NodeId, *ValueNode, error) {
	i, node, err := s.values.New()
	if err != nil {
		return 0, nil, err
	}
	id := ValueId(i)
	if err := s.addToCache(id, node); err != nil {
		return 0, nil, err
	}
	return id, node, err
}

func (s *StateTrie) update(id NodeId, node Node) error {
	// all needed here is to register the modfied node as dirty
	s.dirty[id] = struct{}{}
	// ... and to invalidate the nodes hash.
	s.invalidateHash(id)
	return nil
}

func (s *StateTrie) invalidateHash(id NodeId) {
	// by adding it to the dirty hashes set the hash will be
	// re-evaluated the next time.
	if !id.IsEmpty() {
		s.dirtyHashes[id] = struct{}{}
	}
}

func (s *StateTrie) release(id NodeId) error {
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

// metadata is the helper type to read and write metadata from/to the disk.
type metadata struct {
	RootNode NodeId
}

// readMetadata parses the content of the given file if it exists or returns
// a default-initialized metadata struct if there is no such file.
func readMetadata(filename string) (metadata, error) {

	// If there is no file, initialize and return default metadata.
	if _, err := os.Stat(filename); err != nil {
		return metadata{}, nil
	}

	// If the file exists, parse it and return its content.
	data, err := os.ReadFile(filename)
	if err != nil {
		return metadata{}, err
	}

	var meta metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return meta, err
	}
	return meta, nil
}

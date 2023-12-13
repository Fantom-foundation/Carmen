package mpt

//go:generate mockgen -source verification.go -destination verification_mocks.go -package mpt

import (
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
	"github.com/pbnjay/memory"
	"runtime"
	"sort"
)

// VerificationObserver is a listener interface for tracking the progress of the verification
// of a forest. It can, for instance, be implemented by a user interface to keep the user updated
// on current activities.
type VerificationObserver interface {
	StartVerification()
	Progress(msg string)
	EndVerification(res error)
}

// NilVerificationObserver is a trivial implementation of the observer interface above which
// ignores all reported events.
type NilVerificationObserver struct{}

func (NilVerificationObserver) StartVerification()        {}
func (NilVerificationObserver) Progress(msg string)       {}
func (NilVerificationObserver) EndVerification(res error) {}

// VerifyFileForest runs list of validation checks on the forest stored in the given
// directory. These checks include:
//   - all required files are present and can be read
//   - all referenced nodes are present
//   - all hashes are consistent
func VerifyFileForest(directory string, config MptConfig, roots []Root, observer VerificationObserver) (res error) {
	if observer == nil {
		observer = NilVerificationObserver{}
	}
	observer.StartVerification()
	defer func() {
		if r := recover(); r != nil {
			panic(r)
		}
		observer.EndVerification(res)
	}()

	// ------------------------- Meta-Data Checks -----------------------------

	observer.Progress(fmt.Sprintf("Checking forest stored in %s ...", directory))

	// Verify stock data structures.
	observer.Progress("Checking meta-data ...")
	accountEncoder, branchEncoder, extensionEncoder, valueEncoder := getEncoder(config)
	if err := file.VerifyStock[uint64](directory+"/accounts", accountEncoder); err != nil {
		return err
	}
	if err := file.VerifyStock[uint64](directory+"/branches", branchEncoder); err != nil {
		return err
	}
	if err := file.VerifyStock[uint64](directory+"/extensions", extensionEncoder); err != nil {
		return err
	}
	if err := file.VerifyStock[uint64](directory+"/values", valueEncoder); err != nil {
		return err
	}

	// Open stock data structures for content verification.
	observer.Progress("Obtaining read access to files ...")
	source, err := openVerificationNodeSource(directory, config)
	if err != nil {
		return err
	}
	defer source.close()

	// ----------------- First Pass: check Node References --------------------

	// Check that all IDs used to reference other nodes are valid.
	observer.Progress("Checking node references ...")
	checkId := func(ref NodeReference) error {
		if source.isValid(ref.Id()) {
			return nil
		}
		return fmt.Errorf("contains invalid reference to node %v", ref.Id())
	}

	// Check that roots are valid.
	errs := []error{}
	for _, root := range roots {
		if err := checkId(root.NodeRef); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	err = source.forAllInnerNodes(func(node Node) error {
		switch n := node.(type) {
		case *AccountNode:
			return checkId(n.storage)
		case *ExtensionNode:
			return checkId(n.next)
		case *BranchNode:
			errs := []error{}
			for i := 0; i < len(n.children); i++ {
				if err := checkId(n.children[i]); err != nil {
					errs = append(errs, err)
				}
			}
			return errors.Join(errs...)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// -------------------- Further Passes: node hashes -----------------------

	hasher := config.Hashing.createHasher()
	hash := func(node Node) (common.Hash, error) {
		overrideId := ValueId((^uint64(0)) >> 2)
		if _, ok := node.(EmptyNode); ok {
			overrideId = EmptyId()
		}
		source.setNodeOverride(overrideId, node)
		defer source.clearOverride()
		ref := NewNodeReference(overrideId)
		return hasher.getHash(&ref, source)
	}
	emptyNodeHash, err := hash(EmptyNode{})
	if err != nil {
		return fmt.Errorf("failed to hash empty node: %v", err)
	}

	err = verifyHashes(
		"account", source, source.accounts, source.accountIds, emptyNodeHash, roots, observer,
		func(node *AccountNode) (common.Hash, error) { return hash(node) },
		func(node *AccountNode) (common.Hash, bool) { return node.GetHash() },
		func(node Node) (bool, error) { return hasher.isEmbedded(node, source) },
		func(id NodeId) bool { return id.IsAccount() },
		func(node *AccountNode, hashes map[NodeId]embeddedHash) {
			node.storageHash = hashes[node.storage.Id()].hash
			node.storageHashDirty = false
		},
		func(node *AccountNode, ids *nodeIds) {
			ids.Put(node.storage.Id())
		},
	)
	if err != nil {
		return err
	}

	err = verifyHashes(
		"branch", source, source.branches, source.branchIds, emptyNodeHash, roots, observer,
		func(node *BranchNode) (common.Hash, error) { return hash(node) },
		func(node *BranchNode) (common.Hash, bool) { return node.GetHash() },
		func(node Node) (bool, error) { return hasher.isEmbedded(node, source) },
		func(id NodeId) bool { return id.IsBranch() },
		func(node *BranchNode, hashes map[NodeId]embeddedHash) {
			for i := 0; i < 16; i++ {
				child := node.children[i]
				if !child.Id().IsEmpty() && hashes[child.Id()].isEmbedded {
					node.setEmbedded(byte(i), true)
				}
				if child := node.children[i]; !node.isEmbedded(byte(i)) && !child.Id().IsEmpty() {
					hash, found := hashes[child.Id()]
					if !found {
						panic(fmt.Sprintf("missing hash for %v\n", child.Id()))
					}
					node.hashes[i] = hash.hash
				}
			}
			node.dirtyHashes = 0
		},
		func(node *BranchNode, ids *nodeIds) {
			for i := 0; i < 16; i++ {
				// ID may be an embedded child, it will be determined later while hashing
				ids.Put(node.children[i].Id())
			}
		},
	)
	if err != nil {
		return err
	}

	err = verifyHashes(
		"extension", source, source.extensions, source.extensionIds, emptyNodeHash, roots, observer,
		func(node *ExtensionNode) (common.Hash, error) { return hash(node) },
		func(node *ExtensionNode) (common.Hash, bool) { return node.GetHash() },
		func(node Node) (bool, error) { return hasher.isEmbedded(node, source) },
		func(id NodeId) bool { return id.IsExtension() },
		func(node *ExtensionNode, hashes map[NodeId]embeddedHash) {
			h := hashes[node.next.Id()]
			node.nextHash = h.hash
			node.nextHashDirty = false
			node.nextIsEmbedded = h.isEmbedded
		},
		func(node *ExtensionNode, ids *nodeIds) {
			ids.Put(node.next.Id())
		},
	)
	if err != nil {
		return err
	}

	err = verifyHashes(
		"value", source, source.values, source.valueIds, emptyNodeHash, roots, observer,
		func(node *ValueNode) (common.Hash, error) { return hash(node) },
		func(node *ValueNode) (common.Hash, bool) { return node.GetHash() },
		func(node Node) (bool, error) { return hasher.isEmbedded(node, source) },
		func(id NodeId) bool { return id.IsValue() },
		func(*ValueNode, map[NodeId]embeddedHash) {},
		func(node *ValueNode, ids *nodeIds) {},
	)
	if err != nil {
		return err
	}

	// Finally, check roots for Archive node
	if source.getConfig().HashStorageLocation == HashStoredWithNode {
		// Check hashes of roots.
		observer.Progress(fmt.Sprintf("Checking %d root hashes ...", len(roots)))
		refIds := newNodeIds(uint64(len(roots)))
		for _, root := range roots {
			refIds.Put(root.NodeRef.id)
		}
		isEmbedded := func(node Node) (bool, error) { return false, nil } // root node cannot be embedded
		hashes, err := loadNodeHashes(refIds, source, isEmbedded, emptyNodeHash)
		if err != nil {
			return err
		}
		for _, root := range roots {
			want := hashes[root.NodeRef.Id()].hash
			got := root.Hash
			if want != got {
				return fmt.Errorf("inconsistent hash for root node %v, want %v, got %v", root.NodeRef.Id(), want, got)
			}
		}
	}

	return nil
}

func verifyHashes[N any](
	name string,
	source *verificationNodeSource,
	stock stock.Stock[uint64, N],
	ids stock.IndexSet[uint64],
	hashOfEmptyNode common.Hash,
	roots []Root,
	observer VerificationObserver,
	hash func(*N) (common.Hash, error),
	readHash func(*N) (common.Hash, bool),
	isEmbedded func(Node) (bool, error),
	isNodeType func(NodeId) bool,
	fillInChildrenHashes func(*N, map[NodeId]embeddedHash),
	collectChildrenIds func(*N, *nodeIds),
) error {
	mode := source.getConfig().HashStorageLocation
	switch mode {
	case HashStoredWithNode:
		return verifyHashesStoredWithNodes(name, source, stock, ids, hashOfEmptyNode, observer, hash, readHash, isEmbedded, fillInChildrenHashes, collectChildrenIds)
	case HashStoredWithParent:
		return verifyHashesStoredWithParents(name, source, stock, ids, roots, observer, hash, isNodeType)
	default:
		return fmt.Errorf("unknown hash storage location: %v", mode)
	}
}

// nodeIds is a struct that collects NodeIds.
// It allows adding the IDs in the inner map so that duplicities are eliminated.
// It contains additional structure, which is used for exporting collected NodeIDs
// ordered.
type nodeIds struct {
	nodeIds     map[NodeId]struct{}
	nodeIdsKeys []NodeId
}

func newNodeIds(capacity uint64) *nodeIds {
	return &nodeIds{
		nodeIds:     make(map[NodeId]struct{}),
		nodeIdsKeys: make([]NodeId, 0, capacity),
	}
}

// Put adds node ID into this structure if it is not present yet.
func (n *nodeIds) Put(id NodeId) {
	n.nodeIds[id] = struct{}{}
}

func (n *nodeIds) Size() uint64 {
	return uint64(len(n.nodeIds))
}

// DrainToOrderedKeys returns accumulated keys ordered.
// To save memory, this method clears the accumulated keys while coping them into the output slice.
// It means that repeated calls to this method will not return the same keys again.
// The returned slice is re-used for further calls of this method to save on memory allocations,
// i.e. callers should not modify it.
func (n *nodeIds) DrainToOrderedKeys() []NodeId {
	n.nodeIdsKeys = n.nodeIdsKeys[0:0]
	// collect keys ...
	for id := range n.nodeIds {
		n.nodeIdsKeys = append(n.nodeIdsKeys, id)
	}
	n.nodeIds = make(map[NodeId]struct{}) // remove items to save space

	// ... and sort
	sort.Slice(n.nodeIdsKeys, func(i, j int) bool {
		return n.nodeIdsKeys[i] < n.nodeIdsKeys[j]
	})

	return n.nodeIdsKeys
}

// embeddedHash is a pair storing a hash
// and a flag marking this hash actually being an embedded node when true.
type embeddedHash struct {
	hash       common.Hash
	isEmbedded bool
}

// loadNodeHashes loads hashes of nodes from the input map nodeIds.
// This method optimizes I/O access and memory.
// For this reason, it picks nodeIds from the input and drains then to a sorted slice.
// The nodes to be hashes are loaded in sequence then using this sorted slice.
// This method returns hashed nodes for the input ID and a map with embedded node IDs.
func loadNodeHashes(
	nodeIds *nodeIds,
	source *verificationNodeSource,
	isEmbedded func(Node) (bool, error),
	hashOfEmptyNode common.Hash,
) (map[NodeId]embeddedHash, error) {
	fmt.Printf("Before drain: \n")
	printMemoryUsage()

	nodeIdsKeys := nodeIds.DrainToOrderedKeys()

	fmt.Printf("After drain: \n")
	printMemoryUsage()

	// Load hashes from disk
	hashes := make(map[NodeId]embeddedHash, len(nodeIdsKeys)+1)

	fmt.Printf("After hashes allocation \n")
	printMemoryUsage()

	hashes[EmptyId()] = embeddedHash{hashOfEmptyNode, false}
	for _, id := range nodeIdsKeys {
		var node Node
		if id.IsBranch() {
			n, err := source.branches.Get(id.Index())
			if err != nil {
				return nil, err
			}
			node = &n
		} else if id.IsValue() {
			n, err := source.values.Get(id.Index())
			if err != nil {
				return nil, err
			}
			node = &n
		} else if id.IsAccount() {
			n, err := source.accounts.Get(id.Index())
			if err != nil {
				return nil, err
			}
			node = &n
		} else if id.IsExtension() {
			n, err := source.extensions.Get(id.Index())
			if err != nil {
				return nil, err
			}
			node = &n
		}

		if !id.IsEmpty() {
			hash, dirty := node.GetHash()
			if dirty {
				return nil, fmt.Errorf("encountered dirty hash on disk for node %v", id)
			}
			var embedded bool
			if res, err := isEmbedded(node); err != nil {
				return nil, err
			} else if res {
				embedded = true
			}
			hashes[id] = embeddedHash{hash, embedded}
		}
	}

	return hashes, nil
}

// getBatchSize gets the size of batch used for a list of items stored in memory.
// It is computed as 80% of the main memory divided by the input item size.
func getBatchSize(itemSize uint) uint64 {
	return uint64(float64(memory.TotalMemory()) * 0.8 / float64(itemSize))
}

func verifyHashesStoredWithNodes[N any](
	name string,
	source *verificationNodeSource,
	stock stock.Stock[uint64, N],
	ids stock.IndexSet[uint64],
	hashOfEmptyNode common.Hash,
	observer VerificationObserver,
	hash func(*N) (common.Hash, error),
	readHash func(*N) (common.Hash, bool),
	isEmbedded func(Node) (bool, error),
	fillInChildrenHashes func(*N, map[NodeId]embeddedHash),
	collectChildrenIds func(*N, *nodeIds),
) error {
	// Maps:
	// - Hashes with an embedded flag: 8bytes map overhead, 8 bytes NodeID, 32bytes hash, 1byte boolean
	// NodeList:
	// - 8bytes NodeID
	//const itemSize = (8 + 8 + 32 + 1) + 8
	batchSize := getBatchSize(150) // TODO empirically determined item size

	// re-used for each loop to save on allocations
	refIds := newNodeIds(batchSize / 3) // pre-allocate only a fraction of the capacity to prevent huge allocations and GC when not the whole batch is used.

	// check other nodes
	lowerBound := ids.GetLowerBound()
	upperBound := ids.GetLowerBound()
	var batchNum int

	for upperBound < ids.GetUpperBound() {
		batchNum++
		// First step -- loop to collect Ids of node children
		// The number of child references determines the size of this batch
		// because some nodes like Branch can have many children while other nodes like Extension has just one or Value has none.
		// Since the collected Ids may contain duplicities after this step, the size of the actual batch does not have to fully
		// utilize the maximal batch size, but this is cheaper than finding duplicities in each loop.
		observer.Progress(fmt.Sprintf("Getting refeences to children for %ss (batch %d, size: %d)...", name, batchNum, batchSize))
		for refIds.Size() < batchSize && upperBound < ids.GetUpperBound() {
			if !ids.Contains(upperBound) {
				upperBound++
				continue
			}
			node, err := stock.Get(upperBound)
			if err != nil {
				return err
			}
			collectChildrenIds(&node, refIds)
			upperBound++
		}

		printMemoryUsage()

		// Second step - sort IDs and load hashes from the disk
		observer.Progress(fmt.Sprintf("Loading %d child hashes for %ss (batch %d, size: %d)...", refIds.Size(), name, batchNum, batchSize))
		hashes, err := loadNodeHashes(refIds, source, isEmbedded, hashOfEmptyNode)
		if err != nil {
			return err
		}

		printMemoryUsage()

		// Third step - read again the nodes, fill-in collected child hashes, compare hashes
		observer.Progress(fmt.Sprintf("Checking hashes of up to %d %ss (batch %d, size: %d)...", upperBound-lowerBound, name, batchNum, batchSize))
		for i := lowerBound; i < upperBound; i++ {
			if !ids.Contains(i) {
				continue
			}
			node, err := stock.Get(i)
			if err != nil {
				return err
			}
			fillInChildrenHashes(&node, hashes)
			want, err := hash(&node)
			if err != nil {
				return err
			}

			got, dirty := readHash(&node)
			if dirty {
				return fmt.Errorf("encountered dirty hash for node: %v", i)
			}

			if got != want {
				return fmt.Errorf("invalid hash stored for node %v, want %v, got %v", i, want, got)
			}
		}

		printMemoryUsage()
		lowerBound = upperBound // move to next window
	}

	return nil
}

func printMemoryUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v GB\n", bToMb(m.Alloc))
	//fmt.Printf("TotalAlloc = %v GB\n", bToMb(m.TotalAlloc))
	//fmt.Printf("HeapSys = %v GB\n", bToMb(m.HeapSys))
	//fmt.Printf("Sys = %v GB\n", bToMb(m.Sys))
	//fmt.Printf("NumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) float64 {
	return float64(b) / 1024.0 / 1024.0 / 1024.0
}

func verifyHashesStoredWithParents[N any](
	name string,
	source *verificationNodeSource,
	stock stock.Stock[uint64, N],
	ids stock.IndexSet[uint64],
	roots []Root,
	observer VerificationObserver,
	hash func(*N) (common.Hash, error),
	isNodeType func(NodeId) bool,
) error {
	batchSize := getBatchSize(32) // a batch stores 32byte hashes
	// Load nodes of current type from disk
	for batch := ids.GetLowerBound(); batch < ids.GetUpperBound(); batch += batchSize {
		lowerBound := batch
		upperBound := batch + batchSize
		if upperBound > ids.GetUpperBound() {
			upperBound = ids.GetUpperBound()
		}

		observer.Progress(fmt.Sprintf("Hashing up to %d %ss (batch %d of %d)...", upperBound-lowerBound, name, batch/batchSize+1, ids.GetUpperBound()/batchSize+1))
		hashes := make([]common.Hash, upperBound-lowerBound)
		for i := lowerBound; i < upperBound; i++ {
			if ids.Contains(i) {
				node, err := stock.Get(i)
				if err != nil {
					return err
				}
				h, err := hash(&node)
				if err != nil {
					return err
				}
				hashes[i-lowerBound] = h
			}
		}

		// Check hashes of roots.
		checkNodeHash := func(id NodeId, hash common.Hash) error {
			if !isNodeType(id) || id.Index() < lowerBound || id.Index() >= upperBound {
				return nil
			}
			want := hashes[id.Index()-lowerBound]
			if hash == want {
				return nil
			}
			return fmt.Errorf("inconsistent hash for node %v, want %v, got %v", id, want, hash)
		}

		for _, root := range roots {
			if err := checkNodeHash(root.NodeRef.Id(), root.Hash); err != nil {
				return err
			}
		}

		// Check that all nodes referencing other nodes use the right hashes.
		checkContainedHashes := func(node Node) error {
			switch n := node.(type) {
			case *AccountNode:
				return checkNodeHash(n.storage.Id(), n.storageHash)
			case *ExtensionNode:
				if !n.nextIsEmbedded {
					return checkNodeHash(n.next.Id(), n.nextHash)
				}
				return nil
			case *BranchNode:
				{
					errs := []error{}
					for i := 0; i < len(n.children); i++ {
						if !n.isEmbedded(byte(i)) {
							if err := checkNodeHash(n.children[i].Id(), n.hashes[i]); err != nil {
								errs = append(errs, err)
							}
						}
					}
					return errors.Join(errs...)
				}
			}
			return nil
		}

		observer.Progress(fmt.Sprintf("Checking hash references of up to %d %ss ...", upperBound-lowerBound, name))
		if err := source.forAllInnerNodes(checkContainedHashes); err != nil {
			return err
		}
	}

	return nil
}

type verificationNodeSource struct {
	config MptConfig

	// The stock containers managing individual node types.
	branches   stock.Stock[uint64, BranchNode]
	extensions stock.Stock[uint64, ExtensionNode]
	accounts   stock.Stock[uint64, AccountNode]
	values     stock.Stock[uint64, ValueNode]

	// The sets of valid IDs of each type.
	accountIds   stock.IndexSet[uint64]
	branchIds    stock.IndexSet[uint64]
	extensionIds stock.IndexSet[uint64]
	valueIds     stock.IndexSet[uint64]

	// A custom pair of node ID and Node to be overwritten for node resolution.
	overwriteId   NodeId
	overwriteNode Node
}

func openVerificationNodeSource(directory string, config MptConfig) (*verificationNodeSource, error) {
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
	accountIds, err := accounts.GetIds()
	if err != nil {
		return nil, err
	}
	branchIds, err := branches.GetIds()
	if err != nil {
		return nil, err
	}
	extensionIds, err := extensions.GetIds()
	if err != nil {
		return nil, err
	}
	valueIds, err := values.GetIds()
	if err != nil {
		return nil, err
	}
	success = true
	return &verificationNodeSource{
		config:       config,
		accounts:     accounts,
		branches:     branches,
		extensions:   extensions,
		values:       values,
		accountIds:   accountIds,
		branchIds:    branchIds,
		extensionIds: extensionIds,
		valueIds:     valueIds,
	}, nil
}

func (s *verificationNodeSource) getConfig() MptConfig {
	return s.config
}

func (s *verificationNodeSource) getShared(id NodeId) (*shared.Shared[Node], error) {
	var node Node
	var err error
	if s.overwriteId == id && s.overwriteNode != nil {
		node = s.overwriteNode
	} else if id.IsEmpty() {
		node, err = EmptyNode{}, nil
	} else if id.IsAccount() {
		account, e := s.accounts.Get(id.Index())
		node, err = &account, e
	} else if id.IsBranch() {
		branch, e := s.branches.Get(id.Index())
		node, err = &branch, e
	} else if id.IsExtension() {
		ext, e := s.extensions.Get(id.Index())
		node, err = &ext, e
	} else if id.IsValue() {
		value, e := s.values.Get(id.Index())
		node, err = &value, e
	}
	if err != nil {
		return nil, err
	}
	return shared.MakeShared[Node](node), nil
}

func (s *verificationNodeSource) getReadAccess(ref *NodeReference) (shared.ReadHandle[Node], error) {
	node, err := s.getShared(ref.Id())
	if err != nil {
		return shared.ReadHandle[Node]{}, err
	}
	return node.GetReadHandle(), nil
}

func (s *verificationNodeSource) getViewAccess(ref *NodeReference) (shared.ViewHandle[Node], error) {
	node, err := s.getShared(ref.Id())
	if err != nil {
		return shared.ViewHandle[Node]{}, err
	}
	return node.GetViewHandle(), nil
}

func (s *verificationNodeSource) getHashFor(*NodeReference) (common.Hash, error) {
	panic("hash resolution not supported")
}

func (s *verificationNodeSource) hashKey(key common.Key) common.Hash {
	return common.Keccak256(key[:])
}

func (s *verificationNodeSource) hashAddress(address common.Address) common.Hash {
	return common.Keccak256(address[:])
}

func (s *verificationNodeSource) close() error {
	return errors.Join(
		s.accounts.Close(),
		s.branches.Close(),
		s.extensions.Close(),
		s.values.Close(),
	)
}

func (s *verificationNodeSource) isValid(id NodeId) bool {
	if id.IsEmpty() {
		return true
	}
	if id.IsAccount() {
		return s.accountIds.Contains(id.Index())
	}
	if id.IsBranch() {
		return s.branchIds.Contains(id.Index())
	}
	if id.IsExtension() {
		return s.extensionIds.Contains(id.Index())
	}
	if id.IsValue() {
		return s.valueIds.Contains(id.Index())
	}
	return false
}

func (s *verificationNodeSource) setNodeOverride(id NodeId, node Node) {
	s.overwriteId = id
	s.overwriteNode = node
}

func (s *verificationNodeSource) clearOverride() {
	s.overwriteNode = nil
}

func (s *verificationNodeSource) forAllInnerNodes(check func(Node) error) error {
	return s.forNodes(func(_ NodeId, node Node) error { return check(node) }, true, true, true, false)
}

func (s *verificationNodeSource) forAllNodes(check func(NodeId, Node) error) error {
	return s.forNodes(check, true, true, true, true)
}

func (s *verificationNodeSource) forNodes(
	check func(NodeId, Node) error,
	branches, extensions, accounts, values bool,
) error {
	errs := []error{}
	if branches {
		for i := s.branchIds.GetLowerBound(); i < s.branchIds.GetUpperBound(); i++ {
			if s.branchIds.Contains(i) {
				branch, err := s.branches.Get(i)
				if err != nil { // with IO errors => stop immediately
					return err
				}
				if err := check(BranchId(i), &branch); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}

	if extensions {
		for i := s.extensionIds.GetLowerBound(); i < s.extensionIds.GetUpperBound(); i++ {
			if s.extensionIds.Contains(i) {
				extension, err := s.extensions.Get(i)
				if err != nil { // with IO errors => stop immediately
					return err
				}
				if err := check(ExtensionId(i), &extension); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}

	if accounts {
		for i := s.accountIds.GetLowerBound(); i < s.accountIds.GetUpperBound(); i++ {
			if s.accountIds.Contains(i) {
				account, err := s.accounts.Get(i)
				if err != nil { // with IO errors => stop immediately
					return err
				}
				if err := check(AccountId(i), &account); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}

	if values {
		for i := s.valueIds.GetLowerBound(); i < s.valueIds.GetUpperBound(); i++ {
			if s.valueIds.Contains(i) {
				value, err := s.values.Get(i)
				if err != nil { // with IO errors => stop immediately
					return err
				}
				if err := check(ValueId(i), &value); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}
	return errors.Join(errs...)
}

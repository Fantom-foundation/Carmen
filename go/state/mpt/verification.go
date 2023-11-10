package mpt

//go:generate mockgen -source verification.go -destination verification_mocks.go -package mpt

import (
	"errors"
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
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
		func(id NodeId) bool { return id.IsAccount() },
		func(i uint64) NodeId { return AccountId(i) },
		func(node *AccountNode, hashes map[NodeId]common.Hash) {
			node.storageHash = hashes[node.storage.Id()]
			node.storageHashDirty = false
		},
	)
	if err != nil {
		return err
	}

	err = verifyHashes(
		"branch", source, source.branches, source.branchIds, emptyNodeHash, roots, observer,
		func(node *BranchNode) (common.Hash, error) { return hash(node) },
		func(id NodeId) bool { return id.IsBranch() },
		func(i uint64) NodeId { return BranchId(i) },
		func(node *BranchNode, hashes map[NodeId]common.Hash) {
			for i := 0; i < 16; i++ {
				if child := node.children[i]; !node.isEmbedded(byte(i)) && !child.Id().IsEmpty() {
					node.hashes[i] = hashes[child.Id()]
				}
			}
			node.dirtyHashes = 0
		},
	)
	if err != nil {
		return err
	}

	err = verifyHashes(
		"extension", source, source.extensions, source.extensionIds, emptyNodeHash, roots, observer,
		func(node *ExtensionNode) (common.Hash, error) { return hash(node) },
		func(id NodeId) bool { return id.IsExtension() },
		func(i uint64) NodeId { return ExtensionId(i) },
		func(node *ExtensionNode, hashes map[NodeId]common.Hash) {
			node.nextHash = hashes[node.next.Id()]
			node.nextHashDirty = false
		},
	)
	if err != nil {
		return err
	}

	err = verifyHashes(
		"value", source, source.values, source.valueIds, emptyNodeHash, roots, observer,
		func(node *ValueNode) (common.Hash, error) { return hash(node) },
		func(id NodeId) bool { return id.IsValue() },
		func(i uint64) NodeId { return ValueId(i) },
		func(*ValueNode, map[NodeId]common.Hash) {},
	)
	if err != nil {
		return err
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
	isNodeType func(NodeId) bool,
	toNodeId func(uint64) NodeId,
	fillInChildrenHashes func(*N, map[NodeId]common.Hash),
) error {
	mode := source.getConfig().HashStorageLocation
	switch mode {
	case HashStoredWithNode:
		return verifyHashesStoredWithNodes(name, source, stock, ids, roots, observer, hash, hashOfEmptyNode, toNodeId, fillInChildrenHashes)
	case HashStoredWithParent:
		return verifyHashesStoredWithParents(name, source, stock, ids, roots, observer, hash, isNodeType)
	default:
		return fmt.Errorf("unknown hash storage location: %v", mode)
	}
}

func verifyHashesStoredWithNodes[N any](
	name string,
	source *verificationNodeSource,
	stock stock.Stock[uint64, N],
	ids stock.IndexSet[uint64],
	roots []Root,
	observer VerificationObserver,
	hash func(*N) (common.Hash, error),
	hashOfEmptyNode common.Hash,
	toNodeId func(uint64) NodeId,
	fillInChildrenHashes func(*N, map[NodeId]common.Hash),
) error {
	// TODO: perform the following operations in blocks ...

	// Load all hashes from disk
	observer.Progress("Loading all node hashes ...")
	hashes := map[NodeId]common.Hash{}
	hashes[EmptyId()] = hashOfEmptyNode
	err := source.forAllNodes(func(id NodeId, node Node) error {
		hash, dirty := node.GetHash()
		if dirty {
			return fmt.Errorf("encountered dirty hash on disk for node %v", id)
		}
		hashes[id] = hash
		return nil
	})
	if err != nil {
		return fmt.Errorf("error loading node hashes: %v", err)
	}

	// Check hashes of roots.
	observer.Progress(fmt.Sprintf("Checking %d root hashes ...", len(roots)))
	for _, root := range roots {
		want := hashes[root.NodeRef.Id()]
		got := root.Hash
		if want != got {
			return fmt.Errorf("inconsistent hash for root node %v, want %v, got %v", root.NodeRef.Id(), want, got)
		}
	}

	// Load nodes of current type from disk
	lowerBound := ids.GetLowerBound()
	upperBound := ids.GetUpperBound()
	nodes := make([]N, upperBound-lowerBound)
	observer.Progress(fmt.Sprintf("Loading up to %d %ss ...", len(nodes), name))
	for i := lowerBound; i < upperBound; i++ {
		if ids.Contains(i) {
			node, err := stock.Get(i)
			if err != nil {
				return err
			}
			nodes[i-lowerBound] = node
		}
	}

	// Check hashes for loaded nodes.
	observer.Progress(fmt.Sprintf("Checking hashes of up to %d %ss ...", len(nodes), name))
	for i := lowerBound; i < upperBound; i++ {
		if !ids.Contains(i) {
			continue
		}
		fillInChildrenHashes(&nodes[i], hashes)
		want, err := hash(&nodes[i])
		if err != nil {
			return err
		}
		id := toNodeId(i)
		if got := hashes[id]; got != want {
			return fmt.Errorf("invalid hash stored for node %v, want %v, got %v", id, want, got)
		}
	}

	return nil
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
	// TODO: perform the following operations in blocks ...

	// Load nodes of current type from disk
	lowerBound := ids.GetLowerBound()
	upperBound := ids.GetUpperBound()
	nodes := make([]N, upperBound-lowerBound)
	observer.Progress(fmt.Sprintf("Loading up to %d %ss ...", len(nodes), name))
	for i := lowerBound; i < upperBound; i++ {
		if ids.Contains(i) {
			node, err := stock.Get(i)
			if err != nil {
				return err
			}
			nodes[i-lowerBound] = node
		}
	}

	// Compute hashes for loaded nodes.
	observer.Progress(fmt.Sprintf("Hashing up to %d %ss ...", len(nodes), name))
	hashes := make([]common.Hash, len(nodes))
	for i := lowerBound; i < upperBound; i++ {
		if ids.Contains(i) {
			h, err := hash(&nodes[i])
			if err != nil {
				return err
			}
			hashes[i] = h
		}
	}

	// Check hashes of roots.
	checkNodeHash := func(id NodeId, hash common.Hash) error {
		if !isNodeType(id) || id.Index() >= uint64(len(hashes)) {
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

	observer.Progress(fmt.Sprintf("Checking hash references of up to %d %ss ...", len(nodes), name))
	if err := source.forAllInnerNodes(checkContainedHashes); err != nil {
		return err
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

func (s *verificationNodeSource) touch(*NodeReference) {
	// ignored
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

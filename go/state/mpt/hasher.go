package mpt

//go:generate mockgen -source hasher.go -destination hasher_mocks.go -package mpt

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"sync"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt/rlp"
	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
)

// ----------------------------------------------------------------------------
//                             Public Interfaces
// ----------------------------------------------------------------------------

// hashAlgorithm is the type of a configuration toke selecting the algorithm to
// be used for hashing nodes in an MPT. Its main application is to serve as a
// configuration parameter in the MPT Config.
type hashAlgorithm struct {
	Name         string
	createHasher func() hasher
}

// DirectHashing is a simple, fast hashing algorithm which is taking a simple
// serialization of node content or the hashes of referenced nodes to compute
// the hash of individual nodes.
var DirectHashing = hashAlgorithm{
	Name:         "DirectHashing",
	createHasher: makeDirectHasher,
}

// EthereumLikeHashing is an implementation following the specification of the
// State and Storage Trie hashing as defined in Ethereum's yellow paper.
var EthereumLikeHashing = hashAlgorithm{
	Name:         "EthereumLikeHashing",
	createHasher: makeEthereumLikeHasher,
}

// hasher is an entity retaining hashing information for individual nodes,
// computing them as required.
type hasher interface {
	// updateHash refreshes the hash of the given node and all nested nodes.
	updateHashes(root NodeId, nodes NodeManager) (common.Hash, error)

	// getHash computes the hash of the node without modifying it. It is used
	// for debugging, when checking a trie without the intend of modifying it.
	getHash(NodeId, NodeSource) (common.Hash, error)
}

// ----------------------------------------------------------------------------
//                             Direct Hasher
// ----------------------------------------------------------------------------

// makeDirectHasher creates a hasher using a simple, direct node-value hashing
// algorithm that combines the content of individual nodes with the hashes of
// referenced child nodes into a hash for individual nodes.
func makeDirectHasher() hasher {
	return directHasher{}
}

type directHasher struct{}

// updateHashes implements the DirectHasher's hashing algorithm to refresh
// the hashes stored within all nodes reachable from the given node.
func (h directHasher) updateHashes(id NodeId, source NodeManager) (common.Hash, error) {
	hash := common.Hash{}
	if id.IsEmpty() {
		return hash, nil
	}

	// Get write access to the node (hashes may be updated).
	handle, err := source.getMutableNode(id)
	if err != nil {
		return hash, err
	}
	defer handle.Release()

	// TODO: enable this when dirty flag is properly updated
	/*
		hash, dirty := handle.Get().GetHash()
		if !dirty {
			return hash, nil
		}
	*/

	hash, err = h.hash(id, handle.Get(), handle, source)
	if err != nil {
		return hash, err
	}
	handle.Get().SetHash(hash)
	return hash, nil
}

// getHash implements the DirectHasher's hashing algorithm.
func (h directHasher) getHash(id NodeId, source NodeSource) (common.Hash, error) {
	hash := common.Hash{}
	if id.IsEmpty() {
		return hash, nil
	}

	// Get read access to the node (no update is conducted).
	handle, err := source.getNode(id)
	if err != nil {
		return hash, err
	}
	defer handle.Release()
	return h.hash(id, handle.Get(), shared.WriteHandle[Node]{}, nil)
}

// hash is the internal implementation of the direct hasher to compute the hash
// of a given node or to recursively refresh the hashes. If manager is nil, only
// the hash for the given node is computed, without modifying it, otherwise the
// hash of all recursively reachable nodes is refreshed.
func (h directHasher) hash(id NodeId, node Node, handle shared.WriteHandle[Node], manager NodeManager) (common.Hash, error) {
	hash := common.Hash{}

	// Compute a simple hash for the node.
	hasher := sha256.New()
	switch node := node.(type) {
	case *AccountNode:

		// Refresh storage hash if needed.
		if manager != nil && node.storageHashDirty {
			hash, err := h.updateHashes(node.storage, manager)
			if err != nil {
				return hash, err
			}
			node.storageHash = hash
			node.storageHashDirty = false
			manager.update(id, handle)
		}

		hasher.Write([]byte{'A'})
		hasher.Write(node.address[:])
		hasher.Write(node.info.Balance[:])
		hasher.Write(node.info.Nonce[:])
		hasher.Write(node.info.CodeHash[:])
		hasher.Write(node.storageHash[:])

	case *BranchNode:
		// TODO: compute sub-tree hashes in parallel
		if manager != nil {
			modified := false
			for i, child := range node.children {
				if !child.IsEmpty() && node.isChildHashDirty(byte(i)) {
					hash, err := h.updateHashes(child, manager)
					if err != nil {
						return hash, err
					}
					node.hashes[byte(i)] = hash
					modified = true
				}
			}
			node.clearChildHashDirtyFlags()
			if modified {
				manager.update(id, handle)
			}
		}

		hasher.Write([]byte{'B'})
		for i, child := range node.children {
			if child.IsEmpty() {
				hasher.Write([]byte{'E'})
			} else {
				hasher.Write(node.hashes[byte(i)][:])
			}
		}

	case *ExtensionNode:

		if manager != nil && node.nextHashDirty {
			hash, err := h.updateHashes(node.next, manager)
			if err != nil {
				return hash, err
			}
			node.nextHash = hash
			node.nextHashDirty = false
			manager.update(id, handle)
		}

		hasher.Write([]byte{'E'})
		hasher.Write(node.path.path[:])
		hasher.Write(node.nextHash[:])

	case *ValueNode:
		hasher.Write([]byte{'V'})
		hasher.Write(node.key[:])
		hasher.Write(node.value[:])

	case EmptyNode:
		return common.Hash{}, nil

	default:
		return hash, fmt.Errorf("unsupported node type: %v", reflect.TypeOf(node))
	}
	hasher.Sum(hash[0:0])
	return hash, nil
}

// ----------------------------------------------------------------------------
//                          Ethereum Like Hasher
// ----------------------------------------------------------------------------

// makeEthereumLikeHasher creates a hasher producing hashes according to
// Ethereum's State and Storage Trie specification.
// See Appendix D of https://ethereum.github.io/yellowpaper/paper.pdf
func makeEthereumLikeHasher() hasher {
	return &ethHasher{}
}

type ethHasher struct{}

var emptyNodeEthereumHash = common.Keccak256(rlp.Encode(rlp.String{}))

func (h ethHasher) updateHashes(id NodeId, manager NodeManager) (common.Hash, error) {
	if id.IsEmpty() {
		return emptyNodeEthereumHash, nil
	}
	// Get write access to the node (hashes may be updated).
	handle, err := manager.getMutableNode(id)
	if err != nil {
		return common.Hash{}, err
	}
	node := handle.Get()

	// TODO: enable this when dirty flag is properly updated
	/*
		hash, dirty := node.GetHash()
		if !dirty {
			return hash, nil
		}
	*/

	// Encode the node in RLP and compute its hash.
	data, err := h.encode(id, node, handle, manager, manager)
	if err != nil {
		handle.Release()
		return common.Hash{}, err
	}
	hash := common.Keccak256(data)
	handle.Get().SetHash(hash)
	handle.Release()
	return hash, nil
}

func (h ethHasher) getHash(id NodeId, source NodeSource) (common.Hash, error) {
	if id.IsEmpty() {
		return emptyNodeEthereumHash, nil
	}
	// Get write access to the node (hashes may be updated).
	handle, err := source.getNode(id)
	if err != nil {
		return common.Hash{}, err
	}
	node := handle.Get()

	// Encode the node in RLP and compute its hash.
	data, err := h.encode(id, node, shared.WriteHandle[Node]{}, nil, source)
	handle.Release()
	if err != nil {
		return common.Hash{}, err
	}
	return common.Keccak256(data), nil
}

// encode computes the RLP encoding of the given node. If needed, additional nodes are
// fetched from the given manager/source for deriving the encoding. If the manager is
// provided, write access to required nodes is obtained and dirty node information like
// hashes and embedded flags are updated. If the manager is nil, this operation is a
// read-only operation accepting the current hashes and embedded flags as the true value
// even if dirty flags are set. The node and source parameter must not be nil.
func (h ethHasher) encode(id NodeId, node Node, handle shared.WriteHandle[Node], manager NodeManager, source NodeSource) ([]byte, error) {
	switch trg := node.(type) {
	case EmptyNode:
		return h.encodeEmpty(trg, manager, source)
	case *AccountNode:
		return h.encodeAccount(id, trg, handle, manager, source)
	case *BranchNode:
		return h.encodeBranch(id, trg, handle, manager, source)
	case *ExtensionNode:
		return h.encodeExtension(id, trg, handle, manager, source)
	case *ValueNode:
		return h.encodeValue(id, trg, handle, manager, source)
	default:
		return nil, fmt.Errorf("unsupported node type: %v", reflect.TypeOf(node))
	}
}

var emptyStringRlpEncoded = rlp.Encode(rlp.String{})

func (h ethHasher) encodeEmpty(EmptyNode, NodeManager, NodeSource) ([]byte, error) {
	return emptyStringRlpEncoded, nil
}

// This pools stores not only the slice, but also its pointer, to reduce calls to runtime.convTslice(),
// inspired by:
// https://blog.mike.norgate.xyz/unlocking-go-slice-performance-navigating-sync-pool-for-enhanced-efficiency-7cb63b0b453e
var branchRlpStreamPool = sync.Pool{New: func() any {
	s := make([]rlp.Item, 16+1)
	return &s
},
}

func (h ethHasher) encodeBranch(id NodeId, node *BranchNode, handle shared.WriteHandle[Node], manager NodeManager, source NodeSource) ([]byte, error) {
	children := node.children

	// Refresh all child hashes.
	// TODO: test whether doing this in parallel provides any benefits.
	if manager != nil {
		modified := false
		for i, child := range children {
			if child.IsEmpty() || !node.isChildHashDirty(byte(i)) {
				continue
			}

			// check whether the node is embedded and mark as such
			embedded, err := h.isEmbedded(child, manager)
			if err != nil {
				return nil, err
			}
			node.setEmbedded(byte(i), embedded)

			// For non-embedded nodes, the hash needs to be refreshed.
			if !embedded {
				hash, err := h.updateHashes(child, manager)
				if err != nil {
					return nil, err
				}
				node.hashes[i] = hash
			}

			modified = true
		}

		node.clearChildHashDirtyFlags()
		if modified {
			manager.update(id, handle)
		}
	}

	ptr := branchRlpStreamPool.Get().(*[]rlp.Item)
	defer branchRlpStreamPool.Put(ptr)
	items := *ptr

	for i, child := range children {
		if child.IsEmpty() {
			items[i] = rlp.String{}
			continue
		}

		if node.isEmbedded(byte(i)) {
			node, err := source.getNode(child)
			if err != nil {
				return nil, err
			}
			encoded, err := h.encode(child, node.Get(), shared.WriteHandle[Node]{}, nil, source)
			node.Release()
			if err != nil {
				return nil, err
			}
			items[i] = rlp.Encoded{Data: encoded}
		} else {
			// passing by pointer to hash limits convTslice() calls
			items[i] = rlp.Hash{Hash: &node.hashes[i]}
		}
	}

	// There is one 17th entry which would be filled if this node is a terminator. However,
	// branch nodes are never terminators in State or Storage Tries.
	items[len(children)] = rlp.String{}

	return rlp.Encode(rlp.List{Items: items}), nil
}

var extensionRlpStreamPool = sync.Pool{New: func() any {
	s := make([]rlp.Item, 2)
	return &s
},
}

func (h ethHasher) encodeExtension(id NodeId, node *ExtensionNode, handle shared.WriteHandle[Node], manager NodeManager, source NodeSource) ([]byte, error) {
	ptr := extensionRlpStreamPool.Get().(*[]rlp.Item)
	defer extensionRlpStreamPool.Put(ptr)
	items := *ptr

	numNibbles := node.path.Length()
	packedNibbles := node.path.GetPackedNibbles()
	items[0] = &rlp.String{Str: encodePartialPath(packedNibbles, numNibbles, false)}

	if manager != nil && node.nextHashDirty {

		// check whether the node is embedded and mark as such
		embedded, err := h.isEmbedded(node.next, manager)
		if err != nil {
			return nil, err
		}
		node.nextIsEmbedded = embedded

		if !embedded {
			hash, err := h.updateHashes(node.next, manager)
			if err != nil {
				return nil, err
			}
			node.nextHash = hash
		}
		node.nextHashDirty = false

		manager.update(id, handle)
	}

	// TODO: the use of the same encoding as for the branch nodes is
	// done for symmetry, but there is no unit test for this yet; it
	// would require to find two keys or address with a very long
	// common hash prefix.
	if node.nextIsEmbedded {
		next, err := source.getNode(node.next)
		if err != nil {
			return nil, err
		}
		defer next.Release()
		encoded, err := h.encode(id, next.Get(), shared.WriteHandle[Node]{}, nil, source)
		if err != nil {
			return nil, err
		}
		items[1] = rlp.Encoded{Data: encoded}
	} else {
		items[1] = rlp.String{Str: node.nextHash[:]}
	}

	return rlp.Encode(rlp.List{Items: items}), nil
}

var accountRlpStreamPool = sync.Pool{New: func() any {
	s := make([]rlp.Item, 4)
	return &s
},
}

func (h *ethHasher) encodeAccount(id NodeId, node *AccountNode, handle shared.WriteHandle[Node], manager NodeManager, source NodeSource) ([]byte, error) {
	storageRoot := node.storage
	if manager != nil && node.storageHashDirty {
		storageHash, err := h.updateHashes(storageRoot, manager)
		if err != nil {
			return nil, err
		}
		node.storageHash = storageHash
		node.storageHashDirty = false
		manager.update(id, handle)
	}

	// Encode the account information to get the value.
	ptr := accountRlpStreamPool.Get().(*[]rlp.Item)
	defer accountRlpStreamPool.Put(ptr)
	items := *ptr

	items[0] = rlp.Uint64{Value: node.info.Nonce.ToUint64()}
	items[1] = rlp.BigInt{Value: node.info.Balance.ToBigInt()}
	if storageRoot.IsEmpty() {
		items[2] = rlp.Hash{Hash: &emptyNodeEthereumHash}
	} else {
		items[2] = rlp.Hash{Hash: &node.storageHash}
	}
	items[3] = rlp.Hash{Hash: &node.info.CodeHash}
	value := rlp.Encode(rlp.List{Items: items})

	// Encode the leaf node by combining the partial path with the value.
	items = items[0:2]
	items[0] = rlp.String{Str: encodeAddressPath(node.address, int(node.pathLength), source)}
	items[1] = rlp.String{Str: value}
	return rlp.Encode(rlp.List{Items: items}), nil
}

var valueRlpStreamPool = sync.Pool{New: func() any {
	s := make([]rlp.Item, 2)
	return &s
},
}

func (h *ethHasher) encodeValue(_ NodeId, node *ValueNode, _ shared.WriteHandle[Node], _ NodeManager, source NodeSource) ([]byte, error) {
	ptr := valueRlpStreamPool.Get().(*[]rlp.Item)
	defer valueRlpStreamPool.Put(ptr)
	items := *ptr

	// The first item is an encoded path fragment.
	items[0] = &rlp.String{Str: encodeKeyPath(node.key, int(node.pathLength), source)}

	// The second item is the value without leading zeros.
	value := node.value[:]
	for len(value) > 0 && value[0] == 0 {
		value = value[1:]
	}
	items[1] = &rlp.String{Str: rlp.Encode(&rlp.String{Str: value[:]})}

	return rlp.Encode(rlp.List{Items: items}), nil
}

func encodeKeyPath(key common.Key, numNibbles int, nodes NodeSource) []byte {
	path := nodes.hashKey(key)
	return encodePartialPath(path[32-(numNibbles/2+numNibbles%2):], numNibbles, true)
}

func encodeAddressPath(address common.Address, numNibbles int, nodes NodeSource) []byte {
	path := nodes.hashAddress(address)
	return encodePartialPath(path[32-(numNibbles/2+numNibbles%2):], numNibbles, true)
}

// Requires packedNibbles to include nibbles as [0a bc de] or [ab cd ef]
func encodePartialPath(packedNibbles []byte, numNibbles int, targetsValue bool) []byte {
	// Path encoding derived from Ethereum.
	// see https://github.com/ethereum/go-ethereum/blob/v1.12.0/trie/encoding.go#L37
	oddLength := numNibbles%2 == 1
	compact := make([]byte, getEncodedPartialPathSize(numNibbles))

	// The high nibble of the first byte encodes the 'is-value' mark
	// and whether the length is even or odd.
	if targetsValue {
		compact[0] |= 1 << 5
	}
	compact[0] |= (byte(numNibbles) % 2) << 4 // odd flag

	// If there is an odd number of nibbles, the first is included in the
	// low-part of the compact path encoding.
	if oddLength {
		compact[0] |= packedNibbles[0] & 0xf
		packedNibbles = packedNibbles[1:]
	}
	// The rest of the nibbles can be copied.
	copy(compact[1:], packedNibbles)
	return compact
}

func getEncodedPartialPathSize(numNibbles int) int {
	return numNibbles/2 + 1
}

// isEmbedded determines whether the given node is an embedded node or not.
// If information required for determining the embedded-state of the node is
// marked dirty, this information is updated. Thus, calls to this function may
// cause updates to the state of some nodes.
func (h ethHasher) isEmbedded(id NodeId, manager NodeManager) (bool, error) {
	// TODO: test this function

	// Start by estimating a lower bound for the node size.
	node, err := manager.getMutableNode(id) // mutable since encoding may update hashes.
	if err != nil {
		return false, err
	}
	defer node.Release()

	minSize, err := getLowerBoundForEncodedSize(node.Get(), 32, manager)
	if err != nil {
		return false, err
	}

	// If the lower boundary exceeds the limit we can be sure it is not an embedded node.
	if minSize >= 32 {
		return false, nil
	}

	// We need to encode it to be certain.
	encoded, err := h.encode(id, node.Get(), node, manager, manager)
	if err != nil {
		return false, err
	}

	return len(encoded) < 32, nil
}

func getLowerBoundForEncodedSize(node Node, limit int, nodes NodeSource) (int, error) {
	switch trg := node.(type) {
	case EmptyNode:
		return getLowerBoundForEncodedSizeEmpty(trg, limit, nodes)
	case *AccountNode:
		return getLowerBoundForEncodedSizeAccount(trg, limit, nodes)
	case *BranchNode:
		return getLowerBoundForEncodedSizeBranch(trg, limit, nodes)
	case *ExtensionNode:
		return getLowerBoundForEncodedSizeExtension(trg, limit, nodes)
	case *ValueNode:
		return getLowerBoundForEncodedSizeValue(trg, limit, nodes)
	default:
		return 0, fmt.Errorf("unsupported node type: %v", reflect.TypeOf(node))
	}
}

func getLowerBoundForEncodedSizeEmpty(node EmptyNode, limit int, nodes NodeSource) (int, error) {
	return len(emptyStringRlpEncoded), nil
}

func getLowerBoundForEncodedSizeAccount(node *AccountNode, limit int, nodes NodeSource) (int, error) {
	size := 32 + 32 // storage and code hash
	// There is no need for anything more accurate so far, since
	// all queries will use a limit <= 32.
	return size, nil
}

func getLowerBoundForEncodedSizeBranch(node *BranchNode, limit int, nodes NodeSource) (int, error) {
	var emptySize = len(emptyStringRlpEncoded)
	sum := 1        // children are encoded as elements of a list and the list adds at least 1 byte for the size
	sum = emptySize // the 17th element.

	// Sum up non-embedded hashes first (because they are cheap to compute).
	for i := 0; i < len(node.children); i++ {
		child := node.children[i]
		if child.IsEmpty() {
			sum += emptySize
			continue
		}
		if !node.isChildHashDirty(byte(i)) && !node.isEmbedded(byte(i)) {
			sum += common.HashSize
		}
	}

	if sum >= limit {
		return sum, nil
	}

	for i := 0; i < len(node.children); i++ {
		child := node.children[i]
		if sum >= limit {
			return limit, nil
		}
		if child.IsEmpty() || !(node.isChildHashDirty(byte(i)) || node.isEmbedded(byte(i))) {
			continue
		}

		node, err := nodes.getNode(child)
		if err != nil {
			return 0, err
		}
		size, err := getLowerBoundForEncodedSize(node.Get(), limit-sum, nodes)
		node.Release()
		if err != nil {
			return 0, err
		}
		if size >= 32 {
			size = 32
		}
		sum += size
	}
	return sum, nil
}

func getLowerBoundForEncodedSizeExtension(node *ExtensionNode, limit int, nodes NodeSource) (int, error) {
	sum := 1 // list header

	sum += getEncodedPartialPathSize(node.path.Length())
	if sum >= limit {
		return sum, nil
	}

	next, err := nodes.getNode(node.next)
	if err != nil {
		return 0, err
	}
	defer next.Release()

	size, err := getLowerBoundForEncodedSize(next.Get(), limit-sum, nodes)
	if err != nil {
		return 0, err
	}
	if size > 32 {
		size = 32
	}
	sum += size

	return sum, nil
}

func getLowerBoundForEncodedSizeValue(node *ValueNode, limit int, nodes NodeSource) (int, error) {
	size := getEncodedPartialPathSize(int(node.pathLength))
	if size > 1 {
		size++ // one extra byte for the length
	}
	if size >= limit {
		return size, nil
	}

	value := node.value[:]
	for len(value) > 0 && value[0] == 0 {
		value = value[1:]
	}
	return size + len(value) + 1, nil
}

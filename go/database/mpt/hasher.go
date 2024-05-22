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

//go:generate mockgen -source hasher.go -destination hasher_mocks.go -package mpt

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"sync"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/rlp"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
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
	updateHashes(root *NodeReference, nodes NodeManager) (common.Hash, *NodeHashes, error)

	// getHash computes the hash of the node without modifying it. It is used
	// for debugging, when checking a trie without the intend of modifying it.
	getHash(*NodeReference, NodeSource) (common.Hash, error)

	// isEmbedded determines whether the given node is embedded in the parent
	// or represented by its hash.
	isEmbedded(Node, NodeSource) (bool, error)
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
func (h directHasher) updateHashes(ref *NodeReference, source NodeManager) (common.Hash, *NodeHashes, error) {
	hashCollector := &nodeHashCollector{hashes: NewNodeHashes()}
	hash, err := h.updateHashesInternal(ref, source, EmptyPath(), hashCollector)
	return hash, hashCollector.GetHashes(), err
}

func (h directHasher) updateHashesInternal(
	ref *NodeReference,
	manager NodeManager,
	path NodePath,
	hashCollector *nodeHashCollector,
) (common.Hash, error) {
	hash := common.Hash{}
	if ref.Id().IsEmpty() {
		return hash, nil
	}

	// Get write access to the node (hashes may be updated).
	handle, err := manager.getHashAccess(ref)
	if err != nil {
		return hash, err
	}
	defer handle.Release()

	// If the hash in the node is up-to-date we can skip re-hashing.
	hash, dirty := handle.Get().GetHash()
	if !dirty {
		return hash, nil
	}

	hash, err = h.hash(ref, handle.Get(), handle, manager, path, hashCollector)
	if err != nil {
		return hash, err
	}
	handle.Get().SetHash(hash)
	return hash, nil
}

// getHash implements the DirectHasher's hashing algorithm.
func (h directHasher) getHash(ref *NodeReference, source NodeSource) (common.Hash, error) {
	hash := common.Hash{}
	if ref.Id().IsEmpty() {
		return hash, nil
	}

	// Get read access to the node (no update is conducted).
	handle, err := source.getViewAccess(ref)
	if err != nil {
		return hash, err
	}
	defer handle.Release()
	return h.hash(ref, handle.Get(), shared.HashHandle[Node]{}, nil, EmptyPath(), nil)
}

// hash is the internal implementation of the direct hasher to compute the hash
// of a given node or to recursively refresh the hashes. If manager is nil, only
// the hash for the given node is computed, without modifying it, otherwise the
// hash of all recursively reachable nodes is refreshed.
func (h directHasher) hash(
	ref *NodeReference,
	node Node,
	handle shared.HashHandle[Node],
	manager NodeManager,
	path NodePath,
	hashCollector *nodeHashCollector,
) (common.Hash, error) {
	hash := common.Hash{}

	// Compute a simple hash for the node.
	hasher := sha256.New()
	switch node := node.(type) {
	case *AccountNode:

		// Refresh storage hash if needed.
		if manager != nil && node.storageHashDirty {
			hash, err := h.updateHashesInternal(&node.storage, manager, path.Next(), hashCollector)
			if err != nil {
				return hash, err
			}
			node.storageHash = hash
			node.storageHashDirty = false
		}

		hasher.Write([]byte{'A'})
		hasher.Write(node.address[:])
		hasher.Write(node.info.Balance[:])
		hasher.Write(node.info.Nonce[:])
		hasher.Write(node.info.CodeHash[:])
		hasher.Write(node.storageHash[:])

	case *BranchNode:
		// TODO [perf]: compute sub-tree hashes in parallel
		if manager != nil {
			for i, child := range node.children {
				if !child.Id().IsEmpty() && node.isChildHashDirty(byte(i)) {
					hash, err := h.updateHashesInternal(&child, manager, path.Child(Nibble(i)), hashCollector)
					if err != nil {
						return hash, err
					}
					node.hashes[byte(i)] = hash
				}
			}
			node.clearChildHashDirtyFlags()
		}

		hasher.Write([]byte{'B'})
		for i, child := range node.children {
			if child.Id().IsEmpty() {
				hasher.Write([]byte{'E'})
			} else {
				hasher.Write(node.hashes[byte(i)][:])
			}
		}

	case *ExtensionNode:

		if manager != nil && node.nextHashDirty {
			hash, err := h.updateHashesInternal(&node.next, manager, path.Next(), hashCollector)
			if err != nil {
				return hash, err
			}
			node.nextHash = hash
			node.nextHashDirty = false
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
	if hashCollector != nil {
		hashCollector.Add(path, hash)
	}
	return hash, nil
}

func (h directHasher) isEmbedded(Node, NodeSource) (bool, error) {
	return false, nil // nothing is embedded
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

var EmptyNodeEthereumHash = common.Keccak256(rlp.Encode(rlp.String{}))

func (h ethHasher) updateHashes(
	ref *NodeReference,
	manager NodeManager,
) (common.Hash, *NodeHashes, error) {
	hashCollector := &nodeHashCollector{hashes: NewNodeHashes()}
	hash, err := h.updateHashesInternal(ref, manager, hashCollector)
	return hash, hashCollector.GetHashes(), err
}

func (h ethHasher) updateHashesInternal(
	ref *NodeReference,
	manager NodeManager,
	hashCollector *nodeHashCollector,
) (common.Hash, error) {
	if ref.Id().IsEmpty() {
		return EmptyNodeEthereumHash, nil
	}

	type task struct {
		node   *NodeReference
		handle shared.HashHandle[Node]
		step   int
		path   NodePath
	}

	storesHashesInNodes := manager.getConfig().HashStorageLocation == HashStoredWithNode

	embedded := map[NodeId]bool{}

	tasks := make([]task, 0, 128)

	var err error
	var hash common.Hash
	data := make([]byte, 0, 1024)
	tasks = append(tasks, task{node: ref, path: EmptyPath()})
	for len(tasks) > 0 {
		cur := tasks[len(tasks)-1]
		tasks = tasks[0 : len(tasks)-1]

		if cur.step == 0 {
			// Get write access to the node (hashes may be updated).
			handle, e := manager.getHashAccess(cur.node)
			if e != nil {
				err = e
				break
			}
			node := handle.Get()

			// If the hash in the node is up-to-date we can skip re-hashing.
			dirty := false
			hash, dirty = node.GetHash()
			if !dirty {

				if storesHashesInNodes {
					// If the hashes are stored in nodes, not with the parents, embedded
					// flags in parent nodes may not be valid even for child nodes with
					// up-to-date hashes. Thus, whether the nodes is embedded or not
					// needs to be computed for all child nodes.
					if res, e := h.isEmbedded(handle.Get(), manager); err != nil {
						cur.handle.Release()
						err = e
						break
					} else if res {
						embedded[cur.node.Id()] = true
					}
				}

				handle.Release()
				continue
			}

			// The node's hash needs to be refreshed. To do so, schedule
			// the re-hashing of all children with dirty hashes followed
			// by a second pass of this node. Note: the task list is a
			// last-in-first-out stack.
			tasks = append(tasks, task{cur.node, handle, 1, cur.path})

			switch node := node.(type) {
			case *BranchNode:
				for i := 0; i < len(node.children); i++ {
					if !node.children[i].Id().IsEmpty() && node.isChildHashDirty(byte(i)) {
						tasks = append(tasks, task{node: &node.children[i], path: cur.path.Child(Nibble(i))})
					}
				}
			case *ExtensionNode:
				if node.nextHashDirty {
					tasks = append(tasks, task{node: &node.next, path: cur.path.Next()})
				}
			case *AccountNode:
				if node.storageHashDirty {
					if node.storage.Id().IsEmpty() {
						node.storageHash = EmptyNodeEthereumHash
						node.storageHashDirty = false
					} else {
						tasks = append(tasks, task{node: &node.storage, path: cur.path.Next()})
					}
				}
			}
		} else {
			// At this point the hashes of all children are up-to-date.
			// They can now be transferred to the parents.
			node := cur.handle.Get()
			switch cur := node.(type) {
			case *BranchNode:
				for i := 0; i < len(cur.children); i++ {
					if !cur.children[i].Id().IsEmpty() && cur.isChildHashDirty(byte(i)) {
						handle, e := manager.getViewAccess(&cur.children[i])
						if e != nil {
							err = e
							break
						}
						hash, dirty := handle.Get().GetHash()
						if dirty {
							panic("FATAL: detected dirty child of branch node\n")
						}
						cur.hashes[i] = hash
						cur.setEmbedded(byte(i), embedded[cur.children[i].Id()])
						handle.Release()
					}
				}
				cur.clearChildHashDirtyFlags()
			case *ExtensionNode:
				if cur.nextHashDirty {
					handle, e := manager.getViewAccess(&cur.next)
					if e != nil {
						err = e
						break
					}
					hash, dirty := handle.Get().GetHash()
					if dirty {
						panic("FATAL: detected dirty child of extension node\n")
					}
					cur.nextIsEmbedded = embedded[cur.next.Id()]
					cur.nextHash = hash
					handle.Release()
					cur.nextHashDirty = false
				}
			case *AccountNode:
				if cur.storageHashDirty && !cur.storage.Id().IsEmpty() {
					handle, e := manager.getViewAccess(&cur.storage)
					if e != nil {
						err = e
						break
					}
					hash, dirty := handle.Get().GetHash()
					if dirty {
						panic("FATAL: detected dirty child of account node\n")
					}
					cur.storageHash = hash
					handle.Release()
					cur.storageHashDirty = false
				}
			}

			// Test whether this node is to be embedded.
			if res, e := h.isEmbedded(cur.handle.Get(), manager); err != nil {
				cur.handle.Release()
				err = e
				break
			} else if res {
				// Fix hash of embedded nodes to be 0.
				hash = common.Hash{}
				embedded[cur.node.Id()] = true
			} else {
				// Encode the node using RLP and compute its hash.
				data, e := encode(node, manager, data)
				if e != nil {
					cur.handle.Release()
					err = e
					break
				}
				hash = common.Keccak256(data)
			}

			node.SetHash(hash)

			if hashCollector != nil {
				hashCollector.Add(cur.path, hash)
			}

			cur.handle.Release()
		}
	}

	for i := 0; i < len(tasks); i++ {
		if tasks[i].handle.Valid() {
			tasks[i].handle.Release()
		}
	}

	return hash, err
}

func (h ethHasher) getHash(ref *NodeReference, source NodeSource) (common.Hash, error) {
	if ref.Id().IsEmpty() {
		return EmptyNodeEthereumHash, nil
	}
	// Get read access to the node (hashes may not be updated).
	handle, err := source.getViewAccess(ref)
	if err != nil {
		return common.Hash{}, err
	}
	node := handle.Get()

	// Encode the node in RLP and compute its hash.
	data := make([]byte, 0, 1024)
	data, err = encode(node, source, data)
	handle.Release()
	if err != nil {
		return common.Hash{}, err
	}

	// The hash for embedded nodes is 0.
	if len(data) < 32 {
		return common.Hash{}, nil
	}

	return common.Keccak256(data), nil
}

// encode computes the RLP encoding of the given node. If needed, additional nodes are
// fetched from the given source for deriving the encoding.
// The node and source parameter must not be nil.
// The result is stored in the input slice, and the slice is returned as well.
// The reason for having the slice on the input is that the encoding of nodes
// is a frequent operation and the slice can be reused to reduce memory allocations.
func encode(
	node Node,
	source NodeSource,
	target []byte,
) ([]byte, error) {
	switch trg := node.(type) {
	case EmptyNode:
		return encodeEmpty()
	case *AccountNode:
		return encodeAccount(trg, source, target)
	case *BranchNode:
		return encodeBranch(trg, source, target)
	case *ExtensionNode:
		return encodeExtension(trg, source, target)
	case *ValueNode:
		return encodeValue(trg, source, target)
	default:
		return nil, fmt.Errorf("unsupported node type: %v", reflect.TypeOf(node))
	}
}

var emptyStringRlpEncoded = rlp.Encode(rlp.String{})

// rlpEncodingBufferPool is a pool for temporary buffers required to encode RLP fragments.
var rlpEncodingBufferPool = sync.Pool{New: func() any {
	s := make([]byte, 0, 1024)
	return &s
},
}

func encodeEmpty() ([]byte, error) {
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

func encodeBranch(
	node *BranchNode,
	source NodeSource,
	target []byte,
) ([]byte, error) {
	children := &node.children

	ptr := branchRlpStreamPool.Get().(*[]rlp.Item)
	items := *ptr

	for i := 0; i < len(children); i++ {
		child := &children[i]
		if child.Id().IsEmpty() {
			items[i] = rlp.String{}
			continue
		}

		if node.isEmbedded(byte(i)) {
			node, err := source.getViewAccess(child)
			if err != nil {
				return nil, err
			}
			var encoded = make([]byte, 0, 1024)
			encoded, err = encode(node.Get(), source, encoded)
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

	res := rlp.EncodeInto(target[0:0], rlp.List{Items: items})
	branchRlpStreamPool.Put(ptr)
	return res, nil
}

var extensionRlpStreamPool = sync.Pool{New: func() any {
	s := make([]rlp.Item, 2)
	return &s
},
}

func encodeExtension(
	node *ExtensionNode,
	source NodeSource,
	target []byte,
) ([]byte, error) {
	ptr := extensionRlpStreamPool.Get().(*[]rlp.Item)
	defer extensionRlpStreamPool.Put(ptr)
	items := *ptr

	numNibbles := node.path.Length()
	packedNibbles := node.path.GetPackedNibbles()

	pathBufferPtr := rlpEncodingBufferPool.Get().(*[]byte)
	pathBuffer := *pathBufferPtr

	items[0] = &rlp.String{Str: encodePartialPath(packedNibbles, numNibbles, false, pathBuffer)}

	// TODO [cleanup]: the use of the same encoding as for the branch nodes is
	// done for symmetry, but there is no unit test for this yet; it
	// would require to find two keys or address with a very long
	// common hash prefix.
	if node.nextIsEmbedded {
		next, err := source.getViewAccess(&node.next)
		if err != nil {
			return nil, err
		}
		defer next.Release()
		encoded := make([]byte, 0, 1024)
		encoded, err = encode(next.Get(), source, encoded)
		if err != nil {
			return nil, err
		}
		items[1] = rlp.Encoded{Data: encoded}
	} else {
		items[1] = rlp.String{Str: node.nextHash[:]}
	}

	res := rlp.EncodeInto(target[0:0], rlp.List{Items: items})

	*pathBufferPtr = pathBuffer
	rlpEncodingBufferPool.Put(pathBufferPtr)

	return res, nil
}

var accountRlpStreamPool = sync.Pool{New: func() any {
	s := make([]rlp.Item, 4)
	return &s
},
}

func encodeAccount(
	node *AccountNode,
	source NodeSource,
	target []byte,
) ([]byte, error) {
	storageRoot := &node.storage

	// Encode the account information to get the value.
	ptr := accountRlpStreamPool.Get().(*[]rlp.Item)
	items := *ptr

	items[0] = rlp.Uint64{Value: node.info.Nonce.ToUint64()}
	items[1] = rlp.BigInt{Value: node.info.Balance.ToBigInt()}
	if storageRoot.Id().IsEmpty() {
		items[2] = rlp.Hash{Hash: &EmptyNodeEthereumHash}
	} else {
		items[2] = rlp.Hash{Hash: &node.storageHash}
	}
	items[3] = rlp.Hash{Hash: &node.info.CodeHash}

	addressBufferPtr := rlpEncodingBufferPool.Get().(*[]byte)
	addressBuffer := *addressBufferPtr

	contentBufferPtr := rlpEncodingBufferPool.Get().(*[]byte)
	contentBuffer := *contentBufferPtr

	contentBuffer = rlp.EncodeInto(contentBuffer[0:0], rlp.List{Items: items})

	// Encode the leaf node by combining the partial path with the value.
	items = items[0:2]
	items[0] = rlp.String{Str: encodeAddressPath(node.address, int(node.pathLength), source, addressBuffer)}
	items[1] = rlp.String{Str: contentBuffer}
	res := rlp.EncodeInto(target[0:0], rlp.List{Items: items})

	*addressBufferPtr = addressBuffer
	rlpEncodingBufferPool.Put(addressBufferPtr)

	*contentBufferPtr = contentBuffer
	rlpEncodingBufferPool.Put(contentBufferPtr)

	accountRlpStreamPool.Put(ptr)
	return res, nil
}

var valueRlpStreamPool = sync.Pool{New: func() any {
	s := make([]rlp.Item, 2)
	return &s
},
}

func encodeValue(
	node *ValueNode,
	source NodeSource,
	target []byte,
) ([]byte, error) {
	ptr := valueRlpStreamPool.Get().(*[]rlp.Item)
	defer valueRlpStreamPool.Put(ptr)
	items := *ptr

	// The first item is an encoded path fragment.
	encodedPathPtr := rlpEncodingBufferPool.Get().(*[]byte)
	encodedPath := *encodedPathPtr
	items[0] = &rlp.String{Str: encodeKeyPath(node.key, int(node.pathLength), source, encodedPath)}

	// The second item is the value without leading zeros.
	value := node.value[:]
	for len(value) > 0 && value[0] == 0 {
		value = value[1:]
	}

	encodedValuePtr := rlpEncodingBufferPool.Get().(*[]byte)
	encodedValue := *encodedValuePtr
	encodedValue = rlp.EncodeInto(encodedValue[0:0], &rlp.String{Str: value[:]})
	items[1] = &rlp.String{Str: encodedValue}

	res := rlp.EncodeInto(target[0:0], rlp.List{Items: items})

	*encodedPathPtr = encodedPath
	rlpEncodingBufferPool.Put(encodedPathPtr)
	*encodedValuePtr = encodedValue
	rlpEncodingBufferPool.Put(encodedValuePtr)
	return res, nil
}

func encodeKeyPath(key common.Key, numNibbles int, nodes NodeSource, target []byte) []byte {
	path := nodes.hashKey(key)
	return encodePartialPath(path[32-(numNibbles/2+numNibbles%2):], numNibbles, true, target)
}

func encodeAddressPath(address common.Address, numNibbles int, nodes NodeSource, target []byte) []byte {
	path := nodes.hashAddress(address)
	return encodePartialPath(path[32-(numNibbles/2+numNibbles%2):], numNibbles, true, target)
}

// Requires packedNibbles to include nibbles as [0a bc de] or [ab cd ef]
func encodePartialPath(packedNibbles []byte, numNibbles int, targetsValue bool, target []byte) []byte {
	// Path encoding derived from Ethereum.
	// see https://github.com/ethereum/go-ethereum/blob/v1.12.0/trie/encoding.go#L37
	oddLength := numNibbles%2 == 1
	compact := target[0:getEncodedPartialPathSize(numNibbles)]

	// The high nibble of the first byte encodes the 'is-value' mark
	// and whether the length is even or odd.
	compact[0] = 0
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
func (h ethHasher) isEmbedded(
	node Node,
	source NodeSource,
) (bool, error) {
	// Start by estimating a lower bound for the node size.
	minSize, err := getLowerBoundForEncodedSize(node, 32, source)
	if err != nil {
		return false, err
	}

	// If the lower boundary exceeds the limit we can be sure it is not an embedded node.
	if minSize >= 32 {
		return false, nil
	}

	// We need to encode it to be certain.
	var encoded = make([]byte, 0, 1024)
	encoded, err = encode(node, source, encoded)
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
		if child.Id().IsEmpty() {
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
		if child.Id().IsEmpty() || !(node.isChildHashDirty(byte(i)) || node.isEmbedded(byte(i))) {
			continue
		}

		node, err := nodes.getViewAccess(&child)
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

	next, err := nodes.getViewAccess(&node.next)
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

type nodeHashCollector struct {
	hashes *NodeHashes
}

func (n *nodeHashCollector) Add(path NodePath, hash common.Hash) {
	n.hashes.Add(path, hash)
}

func (n *nodeHashCollector) GetHashes() *NodeHashes {
	return n.hashes
}

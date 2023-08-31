package mpt

//go:generate mockgen -source hasher.go -destination hasher_mocks.go -package mpt

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"unsafe"

	"golang.org/x/crypto/sha3"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt/rlp"
)

// ----------------------------------------------------------------------------
//                             Public Interfaces
// ----------------------------------------------------------------------------

// hashAlgorithm is the type of a configuration toke selecting the algorithm to
// be used for hashing nodes in an MPT. Its main application is to serve as a
// configuration parameter in the MPT Config.
type hashAlgorithm struct {
	Name         string
	createHasher func(HashStore) hasher
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
	// getHash obtains the hash of a given node.
	getHash(NodeId, NodeSource) (common.Hash, error)
	// update signals to the hasher that the given node has been updated and
	// that its hash is to be considered outdated.
	update(NodeId) // when a node is changed
	// forget signals that the given node has been removed and its hash no
	// longer needs to be tracked.
	forget(NodeId) // when the node is released
	// flush writes all temporary state information to secondary storage.
	flush(NodeSource) error
	// close flushes the hasher's content and closes underlying resources.
	// After closing the hasher no more operations are supported.
	close(NodeSource) error

	common.MemoryFootprintProvider
}

// ----------------------------------------------------------------------------
//                         Abstract Hasher Base
// ----------------------------------------------------------------------------

type hashFunction = func(NodeId, NodeSource, *genericHasher) (common.Hash, error)

// genericHasher is a common base implementation covering common features to be
// offered by the direct and ethereum like hasher.
type genericHasher struct {
	store            HashStore
	dirtyHashes      map[NodeId]struct{}
	dirtyHashesMutex sync.Mutex
	hash             hashFunction // call-back to the actual hasher function
}

func makeGenericHasher(store HashStore, hash hashFunction) *genericHasher {
	return &genericHasher{
		store:       store,
		dirtyHashes: map[NodeId]struct{}{},
		hash:        hash,
	}
}

func (h *genericHasher) getHash(id NodeId, source NodeSource) (common.Hash, error) {
	// The empty node is never dirty and needs to be handled explicitly.
	if id.IsEmpty() {
		return h.hash(id, source, h)
	}
	// Non-dirty hashes can be taken from the store.
	h.dirtyHashesMutex.Lock()
	if _, dirty := h.dirtyHashes[id]; !dirty {
		h.dirtyHashesMutex.Unlock()
		return h.store.Get(id)
	}
	h.dirtyHashesMutex.Unlock()

	// Dirty hashes need to be re-freshed.
	hash, err := h.hash(id, source, h)
	if err != nil {
		return common.Hash{}, err
	}
	if err := h.store.Set(id, hash); err != nil {
		return hash, err
	}
	h.dirtyHashesMutex.Lock()
	delete(h.dirtyHashes, id)
	h.dirtyHashesMutex.Unlock()
	return hash, nil
}

func (h *genericHasher) update(id NodeId) {
	h.dirtyHashesMutex.Lock()
	h.dirtyHashes[id] = struct{}{}
	h.dirtyHashesMutex.Unlock()
}

func (h *genericHasher) forget(id NodeId) {
	h.dirtyHashesMutex.Lock()
	delete(h.dirtyHashes, id)
	h.dirtyHashesMutex.Unlock()
}

func (h *genericHasher) flush(source NodeSource) error {
	// Get list of dirty nodes to be re-hashed.
	h.dirtyHashesMutex.Lock()
	dirty := make([]NodeId, len(h.dirtyHashes))
	for id := range h.dirtyHashes {
		dirty = append(dirty, id)
	}
	h.dirtyHashes = map[NodeId]struct{}{}
	h.dirtyHashesMutex.Unlock()

	// Sort the list to improve store write performance.
	sort.Slice(dirty, func(i, j int) bool { return dirty[i] < dirty[j] })
	errs := []error{}
	for _, id := range dirty {
		if _, err := h.getHash(id, source); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return h.store.Flush()
}

func (h *genericHasher) close(source NodeSource) error {
	return errors.Join(
		h.flush(source),
		h.store.Close(),
	)
}

func (h *genericHasher) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*h))
	mf.AddChild("store", h.store.GetMemoryFootprint())
	mf.AddChild("dirtyHashes", common.NewMemoryFootprint(uintptr(len(h.dirtyHashes))*unsafe.Sizeof(NodeId(0))))
	return mf
}

// ----------------------------------------------------------------------------
//                             Direct Hasher
// ----------------------------------------------------------------------------

// makeDirectHasher creates a hasher using a simple, direct node-value hashing
// algorithm that combines the content of individual nodes with the hashes of
// referenced child nodes into a hash for individual nodes.
func makeDirectHasher(store HashStore) hasher {
	return makeGenericHasher(store, getDirectHash)
}

// getDirectHash implements the DirectHasher's hashing algorithm.
func getDirectHash(id NodeId, source NodeSource, _ *genericHasher) (common.Hash, error) {
	hash := common.Hash{}
	if id.IsEmpty() {
		return hash, nil
	}

	// Get read access to the node.
	handle, err := source.getNode(id)
	if err != nil {
		return hash, err
	}
	defer handle.Release()
	node := handle.Get()

	// Compute a simple hash for the node.
	hasher := sha256.New()
	switch node := node.(type) {
	case *AccountNode:
		hasher.Write([]byte{'A'})
		hasher.Write(node.address[:])
		hasher.Write(node.info.Balance[:])
		hasher.Write(node.info.Nonce[:])
		hasher.Write(node.info.CodeHash[:])
		if hash, err := source.getHashFor(node.storage); err == nil {
			hasher.Write(hash[:])
		} else {
			return hash, err
		}

	case *BranchNode:
		hasher.Write([]byte{'B'})
		// TODO: compute sub-tree hashes in parallel
		for _, child := range node.children {
			if hash, err := source.getHashFor(child); err == nil {
				hasher.Write(hash[:])
			} else {
				return hash, err
			}
		}

	case *ExtensionNode:
		hasher.Write([]byte{'E'})
		hasher.Write(node.path.path[:])
		if hash, err := source.getHashFor(node.next); err == nil {
			hasher.Write(hash[:])
		} else {
			return hash, err
		}

	case *ValueNode:
		hasher.Write([]byte{'V'})
		hasher.Write(node.key[:])
		hasher.Write(node.value[:])

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
func makeEthereumLikeHasher(store HashStore) hasher {
	res := &ethHasher{
		base:          makeGenericHasher(store, nil),
		encodingCache: common.NewNWaysCache[NodeId, rlp.Item](1<<20, 16),
	}
	res.base.hash = func(id NodeId, source NodeSource, _ *genericHasher) (common.Hash, error) {
		return res.getHashInternal(id, source)
	}
	return res
}

type ethHasher struct {
	base          *genericHasher
	encodingCache *common.NWaysCache[NodeId, rlp.Item]
}

func (h *ethHasher) getHash(id NodeId, source NodeSource) (common.Hash, error) {
	return h.base.getHash(id, source)
}

var emptyNodeEthereumHash = keccak256(rlp.Encode(rlp.String{}))

func (h *ethHasher) getHashInternal(id NodeId, nodes NodeSource) (common.Hash, error) {
	if id.IsEmpty() {
		return emptyNodeEthereumHash, nil
	}
	// Get read access to the node.
	handle, err := nodes.getNode(id)
	if err != nil {
		return common.Hash{}, err
	}
	defer handle.Release()
	node := handle.Get()

	// Encode the node in RLP and compute its hash.
	data, err := h.encode(node, nodes)
	if err != nil {
		return common.Hash{}, err
	}
	return keccak256(data), nil
}

func (h *ethHasher) update(id NodeId) {
	h.encodingCache.Remove(id)
	h.base.update(id)
}

func (h *ethHasher) forget(id NodeId) {
	h.encodingCache.Remove(id)
	h.base.forget(id)
}

func (h *ethHasher) flush(source NodeSource) error {
	return h.base.flush(source)
}

func (h *ethHasher) close(source NodeSource) error {
	return h.base.close(source)
}

func (h *ethHasher) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*h))
	mf.AddChild("base", h.base.GetMemoryFootprint())
	// TODO: compute actual size of items ...
	mf.AddChild("encodingCache", h.encodingCache.GetMemoryFootprint(0))
	return mf
}

func keccak256(data []byte) common.Hash {
	return common.GetHash(sha3.NewLegacyKeccak256(), data)
}

func (h *ethHasher) encode(node Node, nodes NodeSource) ([]byte, error) {
	switch trg := node.(type) {
	case EmptyNode:
		return h.encodeEmpty(trg, nodes)
	case *AccountNode:
		return h.encodeAccount(trg, nodes)
	case *BranchNode:
		return h.encodeBranch(trg, nodes)
	case *ExtensionNode:
		return h.encodeExtension(trg, nodes)
	case *ValueNode:
		return h.encodeValue(trg, nodes)
	default:
		return nil, fmt.Errorf("unsupported node type: %v", reflect.TypeOf(node))
	}
}

// getLowerBoundForEncodedSize computes a lower bound for the length of the RLP encoding of
// the given node. The provided limit indicates an upper bound beyond which any higher
// result is no longer relevant.
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

var emptyStringRlpEncoded = rlp.Encode(rlp.String{})

func (h *ethHasher) encodeEmpty(EmptyNode, NodeSource) ([]byte, error) {
	return emptyStringRlpEncoded, nil
}

func getLowerBoundForEncodedSizeEmpty(node EmptyNode, limit int, nodes NodeSource) (int, error) {
	return len(emptyStringRlpEncoded), nil
}

func (h *ethHasher) encodeBranch(node *BranchNode, nodes NodeSource) ([]byte, error) {
	children := node.children
	items := make([]rlp.Item, len(children)+1)

	for i, child := range children {
		if child.IsEmpty() {
			items[i] = rlp.String{}
			continue
		}

		item, err := h.toItem(child, nodes)
		if err != nil {
			return nil, err
		}
		items[i] = item
	}

	// There is one 17th entry which would be filled if this node is a terminator. However,
	// branch nodes are never terminators in State or Storage Tries.
	items[len(children)] = &rlp.String{}

	return rlp.Encode(rlp.List{Items: items}), nil
}

func getLowerBoundForEncodedSizeBranch(node *BranchNode, limit int, nodes NodeSource) (int, error) {
	var emptySize = len(emptyStringRlpEncoded)
	sum := emptySize // the 17th element.
	for _, child := range node.children {
		if sum >= limit {
			return limit, nil
		}
		if child.IsEmpty() {
			sum += emptySize
			continue
		}

		node, err := nodes.getNode(child)
		if err != nil {
			return 0, err
		}
		defer node.Release()

		size, err := getLowerBoundForEncodedSize(node.Get(), limit-sum, nodes)
		if err != nil {
			return 0, err
		}
		if size >= 32 {
			size = 32
		}
		sum += size
	}
	return sum + 1, nil // the list length adds another byte
}

func (h *ethHasher) encodeExtension(node *ExtensionNode, nodes NodeSource) ([]byte, error) {
	items := make([]rlp.Item, 2)

	numNibbles := node.path.Length()
	packedNibbles := node.path.GetPackedNibbles()
	items[0] = &rlp.String{Str: encodePartialPath(packedNibbles, numNibbles, false)}

	// TODO: the use of the same encoding as for the branch nodes is
	// done for symetry, but there is no unit test for this yet; it
	// would require to find two keys or address with a very long
	// common hash prefix.
	item, err := h.toItem(node.next, nodes)
	if err != nil {
		return nil, err
	}
	items[1] = item

	return rlp.Encode(rlp.List{Items: items}), nil
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

func (h *ethHasher) encodeAccount(node *AccountNode, nodes NodeSource) ([]byte, error) {
	storageRoot := node.storage
	storageHash, err := h.getHash(storageRoot, nodes)
	if err != nil {
		return nil, err
	}

	// Encode the account information to get the value.
	info := node.info
	items := make([]rlp.Item, 4)
	items[0] = &rlp.Uint64{Value: info.Nonce.ToUint64()}
	items[1] = &rlp.BigInt{Value: info.Balance.ToBigInt()}
	items[2] = &rlp.String{Str: storageHash[:]}
	items[3] = &rlp.String{Str: info.CodeHash[:]}
	value := rlp.Encode(rlp.List{Items: items})

	// Encode the leaf node by combining the partial path with the value.
	items = items[0:2]
	items[0] = &rlp.String{Str: encodeAddressPath(node.address, int(node.pathLength), nodes)}
	items[1] = &rlp.String{Str: value}
	return rlp.Encode(rlp.List{Items: items}), nil
}

func getLowerBoundForEncodedSizeAccount(node *AccountNode, limit int, nodes NodeSource) (int, error) {
	size := 32 + 32 // storage and code hash
	// There is no need for anything more accurate so far, since
	// all queries will use a limit <= 32.
	return size, nil
}

func (h *ethHasher) encodeValue(node *ValueNode, nodes NodeSource) ([]byte, error) {
	items := make([]rlp.Item, 2)

	// The first item is an encoded path fragment.
	items[0] = &rlp.String{Str: encodeKeyPath(node.key, int(node.pathLength), nodes)}

	// The second item is the value without leading zeros.
	value := node.value[:]
	for len(value) > 0 && value[0] == 0 {
		value = value[1:]
	}
	items[1] = &rlp.String{Str: rlp.Encode(&rlp.String{Str: value[:]})}

	return rlp.Encode(rlp.List{Items: items}), nil
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
	// Path encosing derived from Ethereum.
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

func (h *ethHasher) toItem(id NodeId, source NodeSource) (rlp.Item, error) {
	// Check cache first.
	if item, found := h.encodingCache.Get(id); found {
		return item, nil
	}

	// The encoding is not cached, so we need to reproduce it.
	node, err := source.getNode(id)
	if err != nil {
		return nil, err
	}
	defer node.Release()

	minSize, err := getLowerBoundForEncodedSize(node.Get(), 32, source)
	if err != nil {
		return nil, err
	}

	var encoded []byte
	if minSize < 32 {
		encoded, err = h.encode(node.Get(), source)
		if err != nil {
			return nil, err
		}
		if len(encoded) >= 32 {
			encoded = nil
		}
	}

	var res rlp.Item
	if encoded == nil {
		hash, err := h.getHash(id, source)
		if err != nil {
			return nil, err
		}
		res = rlp.String{Str: hash[:]}
	} else {
		res = rlp.Encoded{Data: encoded}
	}

	// cache result for future reuse.
	h.encodingCache.Set(id, res)
	return res, nil
}

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
	"bytes"
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common/immutable"
	"github.com/Fantom-foundation/Carmen/go/common/witness"
	"slices"
	"sort"
	"strings"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/common/tribool"
	"golang.org/x/exp/maps"
)

//go:generate mockgen -source proof.go -destination proof_mocks.go -package mpt

// rlpEncodedNode is an RLP encoded MPT node.
type rlpEncodedNode []byte

// rlpEncodedNodeEquals returns true if the two RLP encoded nodes are equal.
func rlpEncodedNodeEquals(a, b rlpEncodedNode) bool {
	return bytes.Equal(a, b)
}

// proofDb is a database of RLP encoded MPT nodes and their hashes that represent witness proofs.
type proofDb map[common.Hash]rlpEncodedNode

// WitnessProof represents a witness proof.
// It contains a database of MPT nodes and their hashes.
type WitnessProof struct {
	proofDb
}

// CreateWitnessProofFromNodes creates a witness proof from a list of strings.
// Each string is an RLP node of the witness proof.
func CreateWitnessProofFromNodes(nodes []immutable.Bytes) WitnessProof {
	db := make(proofDb, len(nodes))
	for _, n := range nodes {
		b := n.ToBytes()
		db[common.Keccak256(b)] = b
	}

	return WitnessProof{db}
}

// CreateWitnessProof creates a witness proof for the input account address
// and possibly storage slots of the same account under the input storage keys.
// This method may return an error when it occurs in the underlying database.
func CreateWitnessProof(nodeSource NodeSource, root *NodeReference, address common.Address, keys ...common.Key) (WitnessProof, error) {
	proof := proofDb{}
	visitor := &proofExtractionVisitor{
		nodeSource: nodeSource,
		proof:      proof,
	}

	var innerError error

	_, err := VisitPathToAccount(nodeSource, root, address, MakeVisitor(func(node Node, info NodeInfo) VisitResponse {
		if res := visitor.Visit(node, info); res == VisitResponseAbort {
			return VisitResponseAbort
		}
		// if account reached, prove storage keys and terminate.
		if account, ok := node.(*AccountNode); ok {
			for _, key := range keys {
				_, err := VisitPathToStorage(nodeSource, &account.storage, key, visitor)
				if err != nil || visitor.err != nil {
					innerError = errors.Join(innerError, visitor.err, err)
					return VisitResponseAbort
				}
			}
			return VisitResponseAbort
		}

		return VisitResponseContinue
	}))

	return WitnessProof{proof}, errors.Join(innerError, visitor.err, err)
}

// MergeProofs merges the input witness proofs and returns the resulting witness proof.
func MergeProofs(others ...WitnessProof) WitnessProof {
	res := WitnessProof{make(proofDb)}
	for _, other := range others {
		res.Add(other)
	}

	return res
}

// Add merges the input witness proof into the current witness proof.
func (p WitnessProof) Add(other WitnessProof) {
	for k, v := range other.proofDb {
		p.proofDb[k] = v
	}
}

// Extract extracts a sub-proof for a given account and selected storage locations from this proof.
// It returns a copy that contains only the data necessary for proving the given address and storage keys.
// The resulting proof covers proofs for the intersection of the requested properties (account information and slots)
// and the properties covered by this proof. The second return parameter indicates whether everything that
// was requested could be covered. If so it is set to true, otherwise it is set to false.
func (p WitnessProof) Extract(root common.Hash, address common.Address, keys ...common.Key) (witness.Proof, bool) {
	result := proofDb{}
	visitor := &proofCollectingVisitor{visited: result}
	found, complete, err := visitWitnessPathTo(p.proofDb, root, addressToHashedNibbles(address), visitor)
	if err != nil || !found {
		return WitnessProof{result}, complete
	}

	storageRoot := visitor.visitedAccount.storageHash
	for _, key := range keys {
		_, completeKey, err := visitWitnessPathTo(p.proofDb, storageRoot, keyToHashedPathNibbles(key), visitor)
		if err != nil || !completeKey {
			complete = false
		}
	}

	return WitnessProof{result}, complete
}

// IsValid checks that this proof is self-consistent. If the result is true, the proof can be used
// for extracting verified information. If false, the proof is corrupted and should be discarded.
func (p WitnessProof) IsValid() bool {
	for k, v := range p.proofDb {
		if k != common.Keccak256(v) {
			return false
		}
		_, err := DecodeFromRlp(v)
		if err != nil {
			return false
		}
	}
	return true
}

// GetAccountInfo extracts an account info from the witness proof for the input root hash and the address.
// This method returns true, if the inputs could be proven. In this case, the first return parameter gives
// the actual value. If the methods return false, the input could not be proved, and the returned value
// is undefined.
// The method may return an error if the proof is invalid.
func (p WitnessProof) GetAccountInfo(root common.Hash, address common.Address) (AccountInfo, bool, error) {
	return witnessAccountFieldGetter(p.proofDb, root, address, func(n AccountNode) AccountInfo {
		return n.Info()
	})
}

// GetBalance extracts a balance from the witness proof for the input root hash and the address.
// This method returns true, if the inputs could be proven. In this case, the first return parameter gives
// the actual value. If the methods return false, the input could not be proved, and the returned value
// is undefined.
// The method may return an error if the proof is invalid.
func (p WitnessProof) GetBalance(root common.Hash, address common.Address) (amount.Amount, bool, error) {
	return witnessAccountFieldGetter(p.proofDb, root, address, func(n AccountNode) amount.Amount {
		return n.Info().Balance
	})
}

// GetNonce extracts a nonce from the witness proof for the input root hash and the address.
// This method returns true, if the inputs could be proven. In this case, the first return parameter gives
// the actual value. If the methods return false, the input could not be proved, and the returned value
// is undefined.
// The method may return an error if the proof is invalid.
func (p WitnessProof) GetNonce(root common.Hash, address common.Address) (common.Nonce, bool, error) {
	return witnessAccountFieldGetter(p.proofDb, root, address, func(n AccountNode) common.Nonce {
		return n.Info().Nonce
	})
}

// GetCodeHash extracts a code hash from the witness proof for the input root hash and the address.
// This method returns true, if the inputs could be proven. In this case, the first return parameter gives
// the actual value. If the methods return false, the input could not be proved, and the returned value
// is undefined.
// The method may return an error if the proof is invalid.
func (p WitnessProof) GetCodeHash(root common.Hash, address common.Address) (common.Hash, bool, error) {
	return witnessAccountFieldGetter(p.proofDb, root, address, func(n AccountNode) common.Hash {
		return n.Info().CodeHash
	})
}

// GetState extracts a storage slot from the witness proof for the input root hash, account address and the storage key.
// If the proof was complete, this method returns true, otherwise it returns false.
// The proof was complete if it could fully determine either existence or non-existence of the slot.
// In other words, it was possible to reach either a value node or an empty node.
// The method may return an error if the proof is invalid.
func (p WitnessProof) GetState(root common.Hash, address common.Address, key common.Key) (common.Value, bool, error) {
	visitor := &proofCollectingVisitor{}
	found, complete, err := visitWitnessPathTo(p.proofDb, root, addressToHashedNibbles(address), visitor)
	if err != nil || !found {
		return common.Value{}, complete, err
	}

	storageRoot := visitor.visitedAccount.storageHash
	found, complete, err = visitWitnessPathTo(p.proofDb, storageRoot, keyToHashedPathNibbles(key), visitor)
	if err != nil || !found {
		return common.Value{}, complete, err
	}

	return visitor.visitedValue.value, true, nil
}

// AllStatesZero checks that all storage slots are empty for the input root hash,
// account address and the storage key range. If the witness proof contains all empty slots
// for the input key range, it returns true. If there is at least one non-empty slot,
// it returns false. If the proof is not complete, it returns unknown. An incomplete proof
// is a proof where the input address or key terminates in a node that is not a correct
// value node, or an empty node.
func (p WitnessProof) AllStatesZero(root common.Hash, address common.Address, from, to common.Key) (tribool.Tribool, error) {
	panic("not implemented")
}

// AllAddressesEmpty checks that all accounts are empty for the input root hash and the address range.
// If the witness proof contains all empty accounts for the input address range, it returns true.
// An empty account is an account that contains a zero balance, nonce, and code hash.
// If there is at least one non-empty account, it returns false. If the proof is not complete,
// it returns unknown. An incomplete proof is a proof where the input address terminates in a node
// that is not a correct account node.
func (p WitnessProof) AllAddressesEmpty(root common.Hash, from, to common.Address) (tribool.Tribool, error) {
	panic("not implemented")
}

// Equals returns true if the two witness proofs are equal.
func (p WitnessProof) Equals(other witness.Proof) bool {
	otherWitness, ok := other.(WitnessProof)
	if !ok {
		return false
	}
	return maps.EqualFunc(p.proofDb, otherWitness.proofDb, rlpEncodedNodeEquals)
}

// String returns a string representation of the witness proof.
// The representation contains all nodes sorted by their hash.
func (p WitnessProof) String() string {
	// Extract keys and sort them
	keys := maps.Keys(p.proofDb)
	cmp := common.HashComparator{}
	sort.Slice(keys, func(i, j int) bool {
		return cmp.Compare(&keys[i], &keys[j]) <= 0
	})

	// Build the string representation
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(fmt.Sprintf("0x%x->0x%x\n", k, p.proofDb[k]))
	}
	return b.String()
}

// GetElements returns serialised elements of the witness proof.
func (p WitnessProof) GetElements() []immutable.Bytes {
	res := make([]immutable.Bytes, 0, len(p.proofDb))
	for _, v := range p.proofDb {
		res = append(res, immutable.NewBytes(v))
	}
	return res
}

func (p WitnessProof) GetStorageElements(root common.Hash, address common.Address, keys ...common.Key) ([]immutable.Bytes, common.Hash, bool) {
	visitor := &proofCollectingVisitor{}
	found, complete, err := visitWitnessPathTo(p.proofDb, root, addressToHashedNibbles(address), visitor)
	if err != nil || !found {
		return []immutable.Bytes{}, common.Hash{}, complete
	}

	storageRoot := visitor.visitedAccount.storageHash
	visitor.visited = make(proofDb)
	for _, key := range keys {
		_, keyComplete, err := visitWitnessPathTo(p.proofDb, storageRoot, keyToHashedPathNibbles(key), visitor)
		if err != nil || !keyComplete {
			complete = false
		}
	}

	return WitnessProof{visitor.visited}.GetElements(), storageRoot, complete
}

// proofExtractionVisitor is a visitor that visits MPT nodes and creates a witness proof.
// It hashes and encodes the nodes and stores them into the proof database.
type proofExtractionVisitor struct {
	proof      proofDb
	nodeSource NodeSource
	err        error
}

// Visit computes RLP and hash of the visited node and puts it to the proof.
func (p *proofExtractionVisitor) Visit(node Node, info NodeInfo) VisitResponse {
	if info.Embedded.True() {
		return VisitResponseAbort
	}

	// node child hashes will be dirty for the archive when hashes are stored with nodes
	// and must be loaded here for witness proof.
	switch n := node.(type) {
	case *ExtensionNode:
		if n.nextHashDirty {
			nextHandle, err := p.nodeSource.getViewAccess(&n.next)
			if err != nil {
				p.err = err
				return VisitResponseAbort
			}
			embedded, err := isNodeEmbedded(nextHandle.Get(), p.nodeSource)
			nextHandle.Release()
			if err != nil {
				p.err = err
				return VisitResponseAbort
			}
			if err := updateChildrenHashes(p.nodeSource, n, map[NodeId]bool{n.next.Id(): embedded}); err != nil {
				p.err = err
				return VisitResponseAbort
			}
		}
	case *BranchNode:
		if n.dirtyHashes != 0 {
			embeddedChildren := make(map[NodeId]bool, 16)
			for i := 0; i < 16; i++ {
				if n.isChildHashDirty(byte(i)) {
					childHandle, err := p.nodeSource.getViewAccess(&n.children[i])
					if err != nil {
						p.err = err
						return VisitResponseAbort
					}
					embedded, err := isNodeEmbedded(childHandle.Get(), p.nodeSource)
					childHandle.Release()
					if err != nil {
						p.err = err
						return VisitResponseAbort
					}
					embeddedChildren[n.children[i].Id()] = embedded
				}
			}

			if err := updateChildrenHashes(p.nodeSource, n, embeddedChildren); err != nil {
				p.err = err
				return VisitResponseAbort
			}
		}
	case *AccountNode:
		if n.storageHashDirty {
			if err := updateChildrenHashes(p.nodeSource, n, map[NodeId]bool{n.storage.Id(): false}); err != nil {
				p.err = err
				return VisitResponseAbort
			}
		}
	}

	data := make([]byte, 0, 1024)
	rlp, err := encodeToRlp(node, p.nodeSource, data)
	if err != nil {
		p.err = err
		return VisitResponseAbort
	}
	hash := common.Keccak256(rlp)

	p.proof[hash] = rlp

	return VisitResponseContinue
}

// witnessAccountFieldGetter extracts an account field from the witness proof for the input root hash and the address.
// Which particular field to extract is given by the callback function.
// This method returns true, if the inputs could be proven. In this case, the first return parameter gives
// the actual value. If the methods return false, the input could not be proved, and the returned value
// is undefined.
// The method may return an error if the proof is invalid.
func witnessAccountFieldGetter[T any](source proofDb, root common.Hash, address common.Address, getter func(AccountNode) T) (T, bool, error) {
	visitor := &proofCollectingVisitor{}
	found, complete, err := visitWitnessPathTo(source, root, addressToHashedNibbles(address), visitor)
	if err != nil || !found {
		var empty T
		return empty, complete, err
	}
	return getter(visitor.visitedAccount.AccountNode), true, nil
}

// visitWitnessPathTo visits all nodes from the input root following the input path.
// Each encountered node is passed to the visitor.
// If no more nodes are available on the path, the execution ends.
// When the function reaches either an account node or a value node it is compared to the remaining input path
// that was not iterated yet.
// If the path matches, the function terminates and returns found equals to true.
// The function determines if the proof was complete.
// The proof is complete if it reaches a terminal node, where it could either fully consume the path,
// or determine that the path cannot recurse to further nodes.
// The proof is incomplete when the path could not be fully iterated and reached a node that is not in the proof.
// The function returns an error if the path cannot be iterated due to error propagated from the input proof.
func visitWitnessPathTo(source proofDb, root common.Hash, path []Nibble, visitor witnessProofVisitor) (found, complete bool, err error) {
	if root == EmptyNodeEthereumHash {
		visitor.Visit(root, emptyStringRlpEncoded, EmptyNode{}, false)
		return false, true, nil
	}

	nodeHash := root
	var nextEmbedded, currentEmbedded bool
	var done bool
	for !done && nodeHash != EmptyNodeEthereumHash {
		var rlpNode rlpEncodedNode
		if nextEmbedded {
			rlpNode = nodeHash[:]
		} else {
			var exists bool
			rlpNode, exists = source[nodeHash]
			if !exists {
				// missing node, proof is not complete
				return false, false, nil
			}
		}
		node, err := DecodeFromRlp(rlpNode)
		if err != nil {
			return false, false, err
		}

		var nextHash common.Hash
		switch n := node.(type) {
		case *ExtensionNode:
			if n.path.IsPrefixOf(path) {
				nextHash = n.nextHash
				path = path[n.path.Length():]
				nextEmbedded = n.nextIsEmbedded
				done = len(path) == 0
			} else {
				done = true
			}
		case *BranchNode:
			if len(path) == 0 {
				done = true
			} else {
				nextHash = n.hashes[path[0]]
				nextEmbedded = n.isEmbedded(byte(path[0]))
				path = path[1:]
			}
		case *decodedAccountNode:
			if n.suffix.IsEqualTo(path) {
				found = true
			}
			done = true
		case *ValueNode:
			keyNibbles := createNibblesFromKeyPrefix(n.key, n.pathLength)
			if slices.Equal(keyNibbles, path) {
				found = true
			}
			done = true
		default:
			return false, true, nil // EmptyNode -> do not visit, and terminate, proof is complete
		}

		visitor.Visit(nodeHash, rlpNode, node, currentEmbedded)
		nodeHash = nextHash
		currentEmbedded = nextEmbedded
	}

	return found, true, nil
}

// witnessProofVisitor is a visitor that visits witness proof nodes.
// It visits the proof element and provides hash of the element,
// the RLP encoded node and the encoded node itself.
type witnessProofVisitor interface {

	// Visit visits the witness proof node.
	// It provides the hash of the node, the RLP encoded node and the node itself.
	Visit(hash common.Hash, rlpNode rlpEncodedNode, node Node, isEmbedded bool)
}

type proofCollectingVisitor struct {
	visited        proofDb            // all visited nodes
	visitedAccount decodedAccountNode // the last visited account node
	visitedValue   ValueNode          // the last visited value node
}

func (v *proofCollectingVisitor) Visit(hash common.Hash, rlpNode rlpEncodedNode, node Node, isEmbedded bool) {
	switch n := node.(type) {
	case *ValueNode:
		v.visitedValue = *n
	case *decodedAccountNode:
		v.visitedAccount = *n
	}

	if !isEmbedded && v.visited != nil {
		v.visited[hash] = rlpNode
	}
	if account, ok := node.(*decodedAccountNode); ok {
		v.visitedAccount = *account
	}
}

// createNibblesFromKeyPrefix creates a nibble path from the input key and the number of nibbles.
func createNibblesFromKeyPrefix(key common.Key, nibbles uint8) []Nibble {
	return createNibblesFromCompact(key[:], int(nibbles))
}

// createNibblesFromCompact creates a nibble path from the input compact byte slice.
// The input slice is trimmed of trailing data if the size exceeds the number of requested nibbles.
// If the number of nibbles is odd, the first nibble from the input slice is ignored.
func createNibblesFromCompact(compact []byte, nibbles int) []Nibble {
	odd := nibbles % 2
	res := make([]Nibble, nibbles+odd)
	numBytes := nibbles/2 + odd
	parseNibbles(res, compact[:numBytes])
	if odd == 1 {
		res = res[1:]
	}
	return res
}

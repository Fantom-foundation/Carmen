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
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/tribool"
	"golang.org/x/exp/maps"
	"sort"
	"strings"
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

// CreateWitnessProof creates a witness proof for the input account address
// and possibly storage slots of the same account under the input storage keys.
// This method may return an error when it occurs in the underlying database.
func CreateWitnessProof(nodeSource NodeSource, root *NodeReference, address common.Address, keys ...common.Key) (WitnessProof, error) {
	panic("not implemented")
}

// Add merges the input witness proof into the current witness proof.
func (p WitnessProof) Add(other WitnessProof) {
	for k, v := range other.proofDb {
		p.proofDb[k] = v
	}
}

// Extract extracts a sub-proof for a given account and selected storage locations from this proof.
// It returns a copy that contains only the data necessary for proofing the given address and storage keys.
// The resulting proof covers proofs for the intersection of the requested properties (account information and slots)
// and the properties covered by this proof. The second return parameter indicates whether everything that was requested could be covered. If so
// it is set to true, otherwise it is set to false.
func (p WitnessProof) Extract(root common.Hash, address common.Address, keys ...common.Key) (WitnessProof, bool) {
	result := proofDb{}
	visitor := &proofCollectingVisitor{visited: result}
	found, err := visitWitnessPathTo(p.proofDb, root, addressToHashedNibbles(address), visitor)
	if err != nil || !found {
		return WitnessProof{result}, found
	}

	storageRoot := visitor.visitedAccount.storageHash
	for _, key := range keys {
		foundKey, err := visitWitnessPathTo(p.proofDb, storageRoot, keyToHashedPathNibbles(key), visitor)
		if err != nil || !foundKey {
			found = false
		}
	}

	return WitnessProof{result}, found
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
// If the witness proof contains an account for the input address, it returns its information.
// If the proof does not contain an account, it returns false.
// The method may return an error if the proof is invalid.
func (p WitnessProof) GetAccountInfo(root common.Hash, address common.Address) (AccountInfo, bool, error) {
	panic("not implemented")
}

// GetBalance extracts a balance from the witness proof for the input root hash and the address.
// If the witness proof contains the requested account for the input address for the given root hash, it returns its balance.
// If the proof does not cover the requested account, it returns false.
// The method may return an error if the proof is invalid.
func (p WitnessProof) GetBalance(root common.Hash, address common.Address) (common.Balance, bool, error) {
	panic("not implemented")
}

// GetNonce extracts a nonce from the witness proof for the input root hash and the address.
// If the witness proof contains the account for the input address, it returns its nonce.
// If the proof does not contain the account, it returns false.
// The method may return an error if the proof is invalid.
func (p WitnessProof) GetNonce(root common.Hash, address common.Address) (common.Nonce, bool, error) {
	panic("not implemented")
}

// GetCodeHash extracts a code hash from the witness proof for the input root hash and the address.
// If the witness proof contains the account for the input address, it returns its code hash.
// If the proof does not contain the account, it returns false.
// The method may return an error if the proof is invalid.
func (p WitnessProof) GetCodeHash(root common.Hash, address common.Address) (common.Hash, bool, error) {
	panic("not implemented")
}

// GetState extracts a storage slot from the witness proof for the input root hash, account address and the storage key.
// If the witness proof contains the input storage slot for the input key, it returns its value.
// If the proof does not contain the slot, it returns false.
// The method may return an error if the proof is invalid.
func (p WitnessProof) GetState(root common.Hash, address common.Address, key common.Key) (common.Value, bool, error) {
	panic("not implemented")
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
func (p WitnessProof) Equals(other WitnessProof) bool {
	return maps.EqualFunc(p.proofDb, other.proofDb, rlpEncodedNodeEquals)
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

// MergeProofs merges the input witness proofs and returns the resulting witness proof.
func MergeProofs(others ...WitnessProof) WitnessProof {
	res := WitnessProof{make(proofDb)}
	for _, other := range others {
		res.Add(other)
	}

	return res
}

// visitWitnessPathTo visits all nodes from the input root following the input path.
// Each encountered node is passed to the visitor.
// If no more nodes are available on the path, the execution ends.
// If the path does not exist, the function returns false.
// The function returns an error if the path cannot be iterated due to error propagated from the input proof.
// When the function reaches either an account node or a value node it is compared to the remaining input path
// that was not iterated yet.
// If the path matches, the function terminates and returns true.
// It means this function can be used to find either an account node or a value node,
// but it cannot find both at the same time.
func visitWitnessPathTo(source proofDb, root common.Hash, path []Nibble, visitor witnessProofVisitor) (bool, error) {
	nodeHash := root

	var nextEmbedded bool
	var found, done bool
	for !done {
		var rlpNode rlpEncodedNode
		if nextEmbedded {
			rlpNode = nodeHash[:]
		} else {
			var exists bool
			rlpNode, exists = source[nodeHash]
			if !exists {
				return false, nil
			}
		}
		node, err := DecodeFromRlp(rlpNode)
		if err != nil {
			return false, err
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
		case *AccountNode:
			addressPath := createPathFromAddressPrefix(n.address, n.pathLength)
			if addressPath.IsEqualTo(path) {
				found = true
			}
			done = true
		case *ValueNode:
			keyPath := createPathFromKeyPrefix(n.key, n.pathLength)
			if keyPath.IsEqualTo(path) {
				found = true
			}
			done = true
		default:
			return false, nil // EmptyNode -> do not visit, and terminate
		}

		visitor.Visit(nodeHash, rlpNode, node, found && nextEmbedded)
		nodeHash = nextHash
	}

	return found, nil
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
	visited        proofDb     // all visited nodes
	visitedAccount AccountNode // the last visited account node
}

func (v *proofCollectingVisitor) Visit(hash common.Hash, rlpNode rlpEncodedNode, node Node, isEmbedded bool) {
	if !isEmbedded {
		v.visited[hash] = rlpNode
	}
	if account, ok := node.(*AccountNode); ok {
		v.visitedAccount = *account
	}
}

// createPathFromAddressPrefix creates a path from an address with the given number
// of nibbles to use from the beginning of the address.
func createPathFromAddressPrefix(address common.Address, nibbles uint8) Path {
	res := Path{length: nibbles}
	copy(res.path[:], address[:])
	return res
}

// createPathFromKeyPrefix creates a path from a key with the given number of nibbles
// to use from the beginning of the key.
func createPathFromKeyPrefix(key common.Key, nibbles uint8) Path {
	res := Path{length: nibbles}
	copy(res.path[:], key[:])
	return res
}

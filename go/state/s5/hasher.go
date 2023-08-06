package s5

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/s4"
	"github.com/Fantom-foundation/Carmen/go/state/s5/rlp"
	"golang.org/x/crypto/sha3"
)

// Based on Appendix D of https://ethereum.github.io/yellowpaper/paper.pdf
type MptHasher struct{}

// GetHash implements the MPT hashing algorithm.
func (h *MptHasher) GetHash(node s4.Node, nodes s4.NodeSource, hashes s4.HashSource) (common.Hash, error) {
	data, err := encode(node, nodes, hashes)
	if err != nil {
		return common.Hash{}, err
	}
	return keccak256(data), nil
}

func keccak256(data []byte) common.Hash {
	return common.GetHash(sha3.NewLegacyKeccak256(), data)
}

func encode(node s4.Node, nodes s4.NodeSource, hashes s4.HashSource) ([]byte, error) {
	switch trg := node.(type) {
	case s4.EmptyNode:
		return encodeEmpty(trg, nodes, hashes)
	case *s4.AccountNode:
		return encodeAccount(trg, nodes, hashes)
	case *s4.BranchNode:
		return encodeBranch(trg, nodes, hashes)
	case *s4.ExtensionNode:
		return encodeExtension(trg, nodes, hashes)
	case *s4.ValueNode:
		return encodeValue(trg, nodes, hashes)
	default:
		return nil, fmt.Errorf("unsupported node type: %v", reflect.TypeOf(node))
	}
}

var emptyStringRlpEncoded = rlp.Encode(rlp.String{})

func encodeEmpty(s4.EmptyNode, s4.NodeSource, s4.HashSource) ([]byte, error) {
	return emptyStringRlpEncoded, nil
}

func encodeBranch(node *s4.BranchNode, nodes s4.NodeSource, hashes s4.HashSource) ([]byte, error) {
	children := node.Children()
	items := make([]rlp.Item, len(children)+1)

	for i, child := range children {
		node, err := nodes.GetNode(child)
		if err != nil {
			return nil, err
		}

		encoded, err := encode(node, nodes, hashes)
		if err != nil {
			return nil, err
		}

		if len(encoded) >= 32 {
			hash, err := hashes.GetHashFor(child)
			if err != nil {
				return nil, err
			}
			encoded = hash[:]
		}
		items[i] = rlp.String{Str: encoded}
	}

	// There is one 17th entry which would be filled if this node is a terminator. However,
	// branch nodes are never terminators in State or Storage Tries.
	items[len(children)] = &rlp.String{}

	var buffer bytes.Buffer
	rlp.List{Items: items}.Write(&buffer)
	return buffer.Bytes(), nil
}

func encodeExtension(node *s4.ExtensionNode, nodes s4.NodeSource, hashes s4.HashSource) ([]byte, error) {
	items := make([]rlp.Item, 2)

	items[0] = &rlp.String{} // = should be the extension path

	next, err := nodes.GetNode(node.Next())
	if err != nil {
		return nil, err
	}
	encoded, err := encode(next, nodes, hashes)
	if err != nil {
		return nil, err
	}
	if len(encoded) >= 32 {
		hash, err := hashes.GetHashFor(node.Next())
		if err != nil {
			return nil, err
		}
		encoded = hash[:]
	}
	items[1] = &rlp.String{Str: encoded}

	var buffer bytes.Buffer
	rlp.List{Items: items}.Write(&buffer)
	return buffer.Bytes(), nil
}

func encodeAccount(node *s4.AccountNode, nodes s4.NodeSource, hashes s4.HashSource) ([]byte, error) {
	storageRoot := node.StorageRoot()
	storageHash, err := hashes.GetHashFor(storageRoot)
	if err != nil {
		return nil, err
	}

	// Encode the account information to get the value.
	info := node.Info()
	items := make([]rlp.Item, 4)
	items[0] = &rlp.String{Str: info.Nonce[:]}
	items[1] = &rlp.String{Str: info.Balance[:]}
	items[2] = &rlp.String{Str: storageHash[:]}
	items[3] = &rlp.String{Str: info.CodeHash[:]}
	value := rlp.Encode(rlp.List{Items: items})

	// Encode the leaf node by combining the partial path with the value.
	items = items[0:2]
	items[0] = &rlp.String{} // Need partial path derived from the address
	items[1] = &rlp.String{Str: value}
	return rlp.Encode(rlp.List{Items: items}), nil
}

func encodeValue(node *s4.ValueNode, nodes s4.NodeSource, hashSource s4.HashSource) ([]byte, error) {
	// TODO: need to know the suffix of the key to be encoded
	// NOTE: the address of the account is not relevant

	items := make([]rlp.Item, 2)
	items[0] = &rlp.String{} // = should be the non-consumed key

	value := node.Value()
	items[1] = &rlp.String{Str: value[:]}

	var buffer bytes.Buffer
	rlp.List{Items: items}.Write(&buffer)
	return buffer.Bytes(), nil
}

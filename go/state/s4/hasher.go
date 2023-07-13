package s4

import (
	"crypto/sha256"
	"fmt"
	"reflect"

	"github.com/Fantom-foundation/Carmen/go/common"
)

type Hasher interface {
	// GetHash requests a hash value for the given node. To compute the node's hash,
	// implementations may recursively resolve hashes for other nodes using the given
	// HashSource implementation. Due to its recursive nature, multiple calls to the
	// function may be nested and/or processed concurrently. Thus, implementations are
	// required to be reentrant and thread-safe.
	GetHash(Node, HashSource) (common.Hash, error)
}

type HashSource interface {
	GetHashFor(NodeId) (common.Hash, error)
}

// DirectHasher implements a simple, direct node-value hashing algorithm that combines
// the content of individual nodes with the hashes of referenced child nodes into a
// hash for individual nodes.
type DirectHasher struct{}

// GetHash implements the DirectHasher's hashing algorithm.
func (h *DirectHasher) GetHash(node Node, source HashSource) (common.Hash, error) {
	hash := common.Hash{}
	if _, ok := node.(EmptyNode); ok {
		return hash, nil
	}
	hasher := sha256.New()
	switch node := node.(type) {
	case *AccountNode:
		hasher.Write([]byte{'A'})
		hasher.Write(node.address[:])
		hasher.Write(node.info.Balance[:])
		hasher.Write(node.info.Nonce[:])
		hasher.Write(node.info.CodeHash[:])
		if hash, err := source.GetHashFor(node.state); err == nil {
			hasher.Write(hash[:])
		} else {
			return hash, err
		}

	case *BranchNode:
		hasher.Write([]byte{'B'})
		// TODO: compute sub-tree hashes in parallel
		for _, child := range node.children {
			if hash, err := source.GetHashFor(child); err == nil {
				hasher.Write(hash[:])
			} else {
				return hash, err
			}
		}

	case *ExtensionNode:
		hasher.Write([]byte{'E'})
		hasher.Write(node.path.path[:])
		if hash, err := source.GetHashFor(node.next); err == nil {
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

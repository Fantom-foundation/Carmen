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
	"encoding/binary"
	"fmt"
)

// NodeId is used to address nodes within tries. Each node ID encodes the type
// of node encoded (either empty, branch, extension, account, or value), and an
// index allowing different instances to be differentiated.
// NodeIds serve the same role as pointers in in-memory implementations of
// tries. They allow to reference one node from another.
//
// For efficiency, each NodeId is represented as a single, 64-bit value. The
// type of addressed node is encoded in the value using the following rules:
//
//   - the value is 0 ... empty node
//   - the value has a binary suffix of 0 ... branch node
//   - the value has a binary suffix of 01 ... value node
//   - the value has a binary suffix of 011 ... account node
//   - the value has a binary suffix of 111 ... extension node
//
// This allows to address 2^63 different branch nodes, 2^62 different values
// and 2^61 account and extension nodes, sufficient for any forseeable future.
type NodeId uint64

// EmptyId returns the node ID representing the empty node.
func EmptyId() NodeId {
	return NodeId(0)
}

// BranchId returns the NodeID of the branch node with the given index.
func BranchId(index uint64) NodeId {
	return NodeId((index + 1) << 1)
}

// ExtensionId returns the NodeID of the extension node with the given index.
func ExtensionId(index uint64) NodeId {
	return NodeId((index << 3) | 0b111)
}

// AccountId returns the NodeID of the account node with the given index.
func AccountId(index uint64) NodeId {
	return NodeId((index << 3) | 0b011)
}

// ValueId returns the NodeID of the value node with the given index.
func ValueId(index uint64) NodeId {
	return NodeId((index << 2) | 0b01)
}

// IsEmpty is true if node n is addressing the empty node.
func (n NodeId) IsEmpty() bool {
	return n == 0
}

// IsBranch is true if node n is addressing a branch node.
func (n NodeId) IsBranch() bool {
	return !n.IsEmpty() && (n&0b1 == 0b0)
}

// IsExtension is true if node n is addressing an extension node.
func (n NodeId) IsExtension() bool {
	return n&0b111 == 0b111
}

// IsAccount is true if node n is addressing an account node.
func (n NodeId) IsAccount() bool {
	return n&0b111 == 0b011
}

// IsValue is true if node n is addressing a value node.
func (n NodeId) IsValue() bool {
	return n&0b11 == 0b01
}

// Index returns the index of the addressed node type.
func (n NodeId) Index() uint64 {
	if n.IsBranch() {
		return uint64(n>>1) - 1
	}
	if n.IsValue() {
		return uint64(n >> 2)
	}
	return uint64(n >> 3)
}

func (n NodeId) String() string {
	if n.IsEmpty() {
		return "E"
	}
	if n.IsAccount() {
		return fmt.Sprintf("A-%d", n.Index())
	}
	if n.IsBranch() {
		return fmt.Sprintf("B-%d", n.Index())
	}
	if n.IsExtension() {
		return fmt.Sprintf("E-%d", n.Index())
	}
	return fmt.Sprintf("V-%d", n.Index())
}

// ----------------------------------------------------------------------------
//                               NodeId Encoder
// ----------------------------------------------------------------------------

// NodeIdEncoder encodes the internal 8-byte node IDs using a fixed-length
// 6-byte disk format ignoring the two most significant bytes which are always
// zero for any state DB bellow ~1PB.
type NodeIdEncoder struct{}

func (NodeIdEncoder) GetEncodedSize() int {
	return 6
}

func (NodeIdEncoder) Store(dst []byte, id *NodeId) {
	var buffer [8]byte
	binary.BigEndian.PutUint64(buffer[:], uint64(*id))
	copy(dst, buffer[2:])
}

func (NodeIdEncoder) Load(src []byte, id *NodeId) {
	var buffer [8]byte
	copy(buffer[2:], src)
	*id = NodeId(binary.BigEndian.Uint64(buffer[:]))
}

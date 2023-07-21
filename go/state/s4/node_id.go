package s4

import (
	"encoding/binary"
	"fmt"
)

// Encoding:
//
//	0 ... empty node
//	binary suffix 0 ... branch node    // Branch nodes are the most frequently used ones
//	binary suffix 01 ... value node
//	binary suffix 011 ... account node
//	binary suffix 111 ... extension node
type NodeId uint64

func EmptyId() NodeId {
	return NodeId(0)
}

func BranchId(index uint64) NodeId {
	return NodeId((index + 1) << 1)
}

func ExtensionId(index uint64) NodeId {
	return NodeId((index << 3) | 0b111)
}

func AccountId(index uint64) NodeId {
	return NodeId((index << 3) | 0b011)
}

func ValueId(index uint64) NodeId {
	return NodeId((index << 2) | 0b01)
}

func (n NodeId) IsEmpty() bool {
	return n == 0
}

func (n NodeId) IsBranch() bool {
	return !n.IsEmpty() && (n&0b1 == 0b0)
}

func (n NodeId) IsExtension() bool {
	return n&0b111 == 0b111
}

func (n NodeId) IsAccount() bool {
	return n&0b111 == 0b011
}

func (n NodeId) IsValue() bool {
	return n&0b11 == 0b01
}

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
	if n.IsValue() {
		return fmt.Sprintf("V-%d", n.Index())
	}
	return "?"
}

// ----------------------------------------------------------------------------
//                               NodeId Encoder
// ----------------------------------------------------------------------------

// NodeIdEncoder encode interal 8-byte node IDs using a fixed-length 6-byte disk
// format ignoring the two most significant bytes which are always zero for any
// state DB bellow ~1PB.
type NodeIdEncoder struct{}

func (NodeIdEncoder) GetEncodedSize() int {
	return 6
}

func (NodeIdEncoder) Store(dst []byte, id *NodeId) error {
	var buffer [8]byte
	binary.BigEndian.PutUint64(buffer[:], uint64(*id))
	copy(dst, buffer[2:])
	return nil
}

func (NodeIdEncoder) Load(src []byte, id *NodeId) error {
	var buffer [8]byte
	buffer[0] = src[0]
	copy(buffer[2:], src)
	*id = NodeId(binary.BigEndian.Uint64(buffer[:]))
	return nil
}

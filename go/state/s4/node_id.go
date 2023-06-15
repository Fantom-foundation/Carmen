package s4

import (
	"encoding/binary"
	"fmt"
)

// Encoding:
//
//	0 ... empty node
//	prefix 0 ... value node
//	prefix 10 ... account node    // Account and value could use the same prefix since they can be distinguished by scope
//	prefix 110 ... branch node
//	prefix 111 ... extension node
type NodeId uint32

func EmptyId() NodeId {
	return NodeId(0)
}

func BranchId(index uint32) NodeId {
	return NodeId(0xC0000000 | index)
}

func ExtensionId(index uint32) NodeId {
	return NodeId(0xE0000000 | index)
}

func AccountId(index uint32) NodeId {
	return NodeId(0x80000000 | index)
}

func ValueId(index uint32) NodeId {
	return NodeId(index + 1)
}

func (n NodeId) IsEmpty() bool {
	return n == 0
}

func (n NodeId) IsBranch() bool {
	return n>>29 == 0x6
}

func (n NodeId) IsExtension() bool {
	return n>>29 == 0x7
}

func (n NodeId) IsAccount() bool {
	return n>>30 == 0x2
}

func (n NodeId) IsValue() bool {
	return !n.IsEmpty() && (n>>31) == 0
}

func (n NodeId) Index() uint32 {
	if n.IsValue() {
		return uint32(n - 1)
	}
	if n.IsAccount() {
		return uint32(n) & ^uint32(0xC0000000)
	}
	return uint32(n) & ^uint32(0xE0000000)
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

type NodeIdEncoder struct{}

func (NodeIdEncoder) GetEncodedSize() int {
	return 4
}

func (NodeIdEncoder) Store(dst []byte, id *NodeId) error {
	binary.LittleEndian.PutUint32(dst, uint32(*id))
	return nil
}

func (NodeIdEncoder) Load(src []byte, id *NodeId) error {
	*id = NodeId(binary.LittleEndian.Uint32(src))
	return nil
}

package mpt

import (
	"strings"
)

// NodePath is a limited length, compact navigation path into a MPT structure
// that can be used to effectively address nodes by their position. Unlike
// Path instances, describing the path encoding the key value of some object
// stored in a MPT, this path merely addresses the navigation paths on a node
// bases. In particular, it ignores path sections introduced by extension nodes
// and does not terminate at the account node level.
type NodePath struct {
	// the path is encoded as follows:
	//  bit 0: valid (1) or not (0)
	//  bit 1-7: length of the path
	//  bits 8-63: 14x 4-bit encoded elements defining the path
	encoded uint64
}

// EmptyPath creates an empty path addressing the root node of an MPT.
func EmptyPath() NodePath {
	return NodePath{1}
}

// CreateNodePath creates a path into a tree following the given nibbles. The
// Nibble given for navigation through an extension node or account node must
// be zero.
func CreateNodePath(steps ...Nibble) NodePath {
	res := EmptyPath()
	for _, step := range steps {
		res = res.Child(step)
	}
	return res
}

// IsValid tests whether the given path is a valid path. Paths may be invalid
// if they are the default path (for safety to avoid accidental usage), or if
// they would be long to be represented (currently, the limit is 14 steps).
func (p NodePath) IsValid() bool {
	return p.encoded&0x1 == 1
}

// Length returns the length of this path if valid. The result is undefined for
// invalid paths.
func (p NodePath) Length() int {
	return int((p.encoded >> 1) & 0x7F)
}

// Get returns the entry at the given position along the path. The result is
// undefined for invalid paths or if the position is out of range.
func (p NodePath) Get(pos byte) Nibble {
	offset := 4 * (pos + 2)
	return Nibble((p.encoded >> offset) & 0xF)
}

// Next produces a path extending this path by one step, addressing a unique
// child of a node. This may be the next-node referenced by an extension or the
// storage referenced by an account.
func (p NodePath) Next() NodePath {
	return p.Child(0)
}

// Child produces a path referencing a child node of the node addressed by the
// path p. To be meaningful, the node referenced by p must be a branch node.
func (p NodePath) Child(step Nibble) NodePath {
	if !p.IsValid() || p.Length() >= 14 {
		return NodePath{}
	}
	currentLength := p.Length()

	res := p.encoded
	res = (res & ^uint64(0x7F<<1)) | uint64((currentLength+1)<<1)
	offset := 4 * (currentLength + 2)
	res = (res & ^uint64(0xF<<offset)) | uint64(step&0xF)<<offset
	return NodePath{res}
}

func (p NodePath) String() string {
	if !p.IsValid() {
		return "-invalid-"
	}
	var builder strings.Builder
	builder.WriteRune('[')
	for i := 0; i < p.Length(); i++ {
		if i != 0 {
			builder.WriteRune(',')
		}
		builder.WriteString(p.Get(byte(i)).String())
	}
	builder.WriteRune(']')
	return builder.String()
}

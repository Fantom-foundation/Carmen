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
	"slices"
	"strings"
)

// NodePath is a simple navigation path into an MPT structure that can be used
// to effectively address nodes by their positions. Unlike Path instances,
// describing the path encoding the key value of some object stored in an MPT,
// this path merely addresses the navigation paths on a node bases. In
// particular, it ignores path sections introduced by extension nodes and does
// not terminate at the account node level.
type NodePath struct {
	path []Nibble
}

// EmptyPath creates an empty path addressing the root node of an MPT.
func EmptyPath() NodePath {
	return NodePath{}
}

// CreateNodePath creates a path into a tree following the given nibbles. The
// Nibble given for navigation through an extension node or account node must
// be zero.
func CreateNodePath(steps ...Nibble) NodePath {
	return NodePath{steps}
}

// Equal returns true if the given path is equal to this path.
func (p NodePath) Equal(o NodePath) bool {
	return slices.Equal(p.path, o.path)
}

// Length returns the length of this path if valid. The result is undefined for
// invalid paths.
func (p NodePath) Length() int {
	return len(p.path)
}

// Get returns the entry at the given position along the path. The result is
// undefined for invalid paths or if the position is out of range.
func (p NodePath) Get(pos byte) Nibble {
	return p.path[pos]
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
	res := make([]Nibble, len(p.path)+1)
	copy(res, p.path)
	res[len(p.path)] = step
	return NodePath{res}
}

func (p NodePath) String() string {
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

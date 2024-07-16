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
	"fmt"
	"strings"
)

// Path is a sequence of nibble's describing a navigation path in a Trie.
// Paths are used in expansion nodes to short-cut multiple steps in the
// trie which would otherwise require numerous branch nodes.
// Unlike []Nibble slices, Paths are encoding pairs of 4-bit Nibbles into
// 8-bit values for a dense data representation. Also, paths are limited
// to a maximum length of 64 Nibbles.
type Path struct {
	// The zero-padded navigation path to be covered. The maximum length
	// is 256 bits, which are 32 bytes and 64 nibbles. Nibbles are encoded
	// in bytes in little-endian order.
	path [32]byte
	// The length of the relevant prefix of the path to be represented in
	// number of nibbles (= 4bit values). Limited to <= 64.
	length uint8
}

// SingleStepPath creates a path consisting of a single step.
func SingleStepPath(n Nibble) Path {
	return Path{path: [32]byte{byte(n) << 4}, length: uint8(1)}
}

// CreatePathFromNibbles converts a Nibble-slice into a path.
func CreatePathFromNibbles(path []Nibble) Path {
	res := Path{}
	for _, cur := range path {
		res.Append(cur)
	}
	return res
}

// Length returns the length of the path.
func (p *Path) Length() int {
	return int(p.length)
}

// GetPackedNibbles returns a slice of nibbles encoded in consecutive high/low
// bits of bytes. If the path length is odd, a leading 0 is added.
func (p *Path) GetPackedNibbles() []byte {
	// If the length is even, we can return a prefix of the path.
	if p.length%2 == 0 {
		return p.path[:p.length/2]
	}
	// Otherwise we need to shift the path by 4 bit.
	length := p.length/2 + 1
	res := make([]byte, length)
	res[0] = p.path[0] >> 4
	for i := 1; i < len(res); i++ {
		res[i] = (p.path[i-1]&0xf)<<4 | (p.path[i] >> 4)
	}
	return res
}

// Get returns the Nibble value at the given path position, where pos == 0
// is the first position and Length()-1 the last. For positions outside this
// range the value 0 is returned.
func (p *Path) Get(pos int) Nibble {
	if pos < 0 || pos >= int(p.length) {
		return 0
	}
	twin := p.path[pos/2]
	if pos%2 == 0 {
		return Nibble(twin >> 4)
	}
	return Nibble(twin & 0xF)
}

// Set updates the value of a Nibble on this path or ignores the call if
// the position is not on the path, thus not in the range [0,Lenght()-1].
func (p *Path) Set(pos int, val Nibble) {
	if pos < 0 || pos >= int(p.length) {
		panic(fmt.Sprintf("out-of-range path update at %d in range [%d,%d)", pos, 0, p.length))
	}
	if pos%2 == 0 {
		p.path[pos/2] = (p.path[pos/2] & 0xF) | byte(val<<4)
	} else {
		p.path[pos/2] = (p.path[pos/2] & 0xF0) | byte(val&0xF)
	}
}

// IsPrefixOf determines whether the given nibble sequence is a prefix of
// this path.
func (p *Path) IsPrefixOf(list []Nibble) bool {
	return p.GetCommonPrefixLength(list) == int(p.length)
}

// IsEqualTo determines whether the given nibble sequence is equal to this path.
func (p *Path) IsEqualTo(list []Nibble) bool {
	return p.Length() == len(list) && p.GetCommonPrefixLength(list) == int(p.length)
}

// GetCommonPrefixLength determines the common prefix of the given Nibble
// slice and this path.
func (p *Path) GetCommonPrefixLength(list []Nibble) int {
	max := int(p.length)
	if max > len(list) {
		max = len(list)
	}
	for i := 0; i < max; i++ {
		if p.Get(i) != list[i] {
			return i
		}
	}
	return max
}

// Append appends a nibble to the end of this path extending it by one element.
func (p *Path) Append(n Nibble) *Path {
	trg := &p.path[p.length/2]
	if p.length%2 == 0 {
		*trg |= byte(n&0xF) << 4
	} else {
		*trg |= byte(n & 0xF)
	}
	p.length++
	return p
}

// AppendAll appends the given path to the end of this path.
func (p *Path) AppendAll(other *Path) *Path {
	for i := 0; i < other.Length(); i++ {
		p.Append(other.Get(i))
	}
	return p
}

// Prepend adds a nibble to be begin of this path, growing it by one element.
func (p *Path) Prepend(n Nibble) *Path {
	p.length++
	for i := int(p.length) - 2; i >= 0; i-- {
		p.Set(i+1, p.Get(i))
	}
	p.Set(0, n)
	return p
}

// RemoveLast removes the last n elements from this path. If n > length, the
// resulting list is empty.
func (p *Path) RemoveLast(n int) *Path {
	if n > int(p.length) {
		p.length = 0
	} else {
		p.length -= uint8(n)
	}
	return p
}

// ShiftLeft shifts alle elements in the path by the given number of steps,
// dropping leading elements and reducing the path length by steps elements.
func (p *Path) ShiftLeft(steps int) *Path {
	if steps >= p.Length() {
		*p = Path{}
		return p
	}
	if steps < 0 {
		return p
	}
	if steps%2 == 0 {
		// Which way: we can shift full bytes.
		copy(p.path[:], p.path[steps/2:])
	} else {
		// Slower: we need to shift half-bytes.
		j := 0
		for i := 0; i < int(p.length)-steps; i++ {
			p.Set(j, p.Get(i+steps))
			j++
		}
		for i := 0; i < steps; i++ {
			p.Set(j, 0)
			j++
		}

	}
	p.length -= uint8(steps)
	return p
}

func (p *Path) String() string {
	if p.length == 0 {
		return "-empty-"
	}
	builder := strings.Builder{}
	for i := 0; i < p.Length(); i++ {
		builder.WriteRune(p.Get(i).Rune())
	}
	builder.WriteString(fmt.Sprintf(" : %d", p.length))
	return builder.String()
}

// ----------------------------------------------------------------------------
//                               Path Encoder
// ----------------------------------------------------------------------------

type PathEncoder struct{}

func (PathEncoder) GetEncodedSize() int {
	return 33
}

func (PathEncoder) Store(trg []byte, path *Path) {
	copy(trg, path.path[:])
	trg[32] = path.length
}

func (PathEncoder) Load(src []byte, path *Path) {
	copy(path.path[:], src)
	path.length = uint8(src[32])
}

package s4

import (
	"fmt"
	"strings"
)

// Path is a sequence of nibble's describing a navigation path in a Trie.
type Path struct {
	// The zero-padded navigation path to be covered. The maximum length
	// is 256 bits, which are 32 bytes and 64 nibbles. Nibbles are encoded
	// in bytes in little-endian order.
	path [32]byte
	// The length of the relevant prefix of the path to be represented in
	// number of nibbles (= 4bit values). Limited to <= 64.
	length uint8
}

func SingleStepPath(n Nibble) Path {
	return Path{path: [32]byte{byte(n) << 4}, length: uint8(1)}
}

func CreatePathFromNibbles(path []Nibble) Path {
	res := Path{}
	for _, cur := range path {
		res.Append(cur)
	}
	return res
}

func (p *Path) Length() int {
	return int(p.length)
}

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

func (p *Path) Set(pos int, val Nibble) {
	if pos < 0 || pos >= int(p.length) {
		return
	}
	if pos%2 == 0 {
		p.path[pos/2] = (p.path[pos/2] & 0xF) | byte(val<<4)
	} else {
		p.path[pos/2] = (p.path[pos/2] & 0xF0) | byte(val&0xF)
	}
}

func (p *Path) IsPrefixOf(list []Nibble) bool {
	return p.GetCommonPrefixLength(list) == int(p.length)
}

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

func (p *Path) AppendAll(other *Path) *Path {
	for i := 0; i < other.Length(); i++ {
		p.Append(other.Get(i))
	}
	return p
}

func (p *Path) Prepend(n Nibble) *Path {
	p.length++
	for i := int(p.length - 2); i >= 0; i-- {
		p.Set(i+1, p.Get(i))
	}
	p.Set(0, n)
	return p
}

// TODO: test
func (p *Path) ShiftLeft(steps int) *Path {
	if steps >= p.Length() {
		*p = Path{}
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

func (PathEncoder) Store(trg []byte, path *Path) error {
	copy(trg, path.path[:])
	trg[32] = path.length
	return nil
}

func (PathEncoder) Load(src []byte, path *Path) error {
	copy(path.path[:], src)
	path.length = uint8(src[32])
	return nil
}

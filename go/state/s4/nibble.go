package s4

import "github.com/Fantom-foundation/Carmen/go/common"

// Nibble is a 4-bit signed integer in the range 0-F. It is a single letter
// used to navigate in the MPT structure.
type Nibble byte

func (n Nibble) Rune() rune {
	if n < 10 {
		return rune('0' + n)
	} else if n < 16 {
		return rune('a' + n - 10)
	} else {
		return '?'
	}
}

func (n Nibble) String() string {
	return string(n.Rune())
}

func addressToNibbles(addr common.Address) [40]Nibble {
	var res [40]Nibble
	parseNibbles(res[:], addr[:])
	return res
}

func keyToNibbles(key common.Key) [64]Nibble {
	var res [64]Nibble
	parseNibbles(res[:], key[:])
	return res
}

func parseNibbles(dst []Nibble, src []byte) {
	for i := 0; i < len(src); i++ {
		dst[2*i] = Nibble(src[i] >> 4)
		dst[2*i+1] = Nibble(src[i] & 0xF)
	}
}

// TODO: test
func getCommonPrefixLength(a, b []Nibble) int {
	lengthA := len(a)
	if lengthA > len(b) {
		return getCommonPrefixLength(b, a)
	}
	for i := 0; i < lengthA; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return lengthA
}

// TODO: test
func isPrefixOf(a, b []Nibble) bool {
	return len(a) <= len(b) && getCommonPrefixLength(a, b) == len(a)
}

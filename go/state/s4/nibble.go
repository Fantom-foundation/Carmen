package s4

// Nibble is a 4-bit signed integer in the range 0-F. It is a single letter
// used to navigate in the MPT structure.
type Nibble byte

// Rune converts a Nibble in a hexa-decimal rune (0-9a-f).
func (n Nibble) Rune() rune {
	if n < 10 {
		return rune('0' + n)
	} else if n < 16 {
		return rune('a' + n - 10)
	} else {
		return '?'
	}
}

// String converts a Nibble in a hexa-decimal stirng (0-9a-f).
func (n Nibble) String() string {
	return string(n.Rune())
}

// ToNibblePath converts the given path into a slice of Nibbles. Optionally, the
// path is hashed before being converted.
func ToNibblePath(path []byte, hashPath bool) []Nibble {
	if hashPath {
		hash := keccak256(path)
		return ToNibblePath(hash[:], false)
	}
	res := make([]Nibble, len(path)*2)
	parseNibbles(res, path)
	return res
}

func parseNibbles(dst []Nibble, src []byte) {
	for i := 0; i < len(src); i++ {
		dst[2*i] = Nibble(src[i] >> 4)
		dst[2*i+1] = Nibble(src[i] & 0xF)
	}
}

// GetCommonPrefixLength computes the length of the common prefix of the given
// Nibble-slices.
func GetCommonPrefixLength(a, b []Nibble) int {
	lengthA := len(a)
	if lengthA > len(b) {
		return GetCommonPrefixLength(b, a)
	}
	for i := 0; i < lengthA; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return lengthA
}

// IsPrefixOf tests whether one Nibble slice is the prefix of another.
func IsPrefixOf(a, b []Nibble) bool {
	return len(a) <= len(b) && GetCommonPrefixLength(a, b) == len(a)
}

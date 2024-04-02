package mpt

import "github.com/Fantom-foundation/Carmen/go/common"

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

// String converts a Nibble in a hexa-decimal string (0-9a-f).
func (n Nibble) String() string {
	return string(n.Rune())
}

// AddressToNibblePath converts the given path into a slice of Nibbles. Optionally, the
// path is hashed before being converted. The path is hashed when hashing is enabled in configuration.
func AddressToNibblePath(address common.Address, source NodeSource) []Nibble {
	var path []byte
	if source != nil && source.getConfig().UseHashedPaths {
		hash := source.hashAddress(address)
		path = hash[:]
	} else {
		path = address[:]
	}

	res := make([]Nibble, len(path)*2)
	parseNibbles(res, path)
	return res
}

// KeyToNibblePath converts the given path into a slice of Nibbles. Optionally, the
// path is hashed before being converted. The path is hashed when hashing is enabled in configuration.
func KeyToNibblePath(key common.Key, source NodeSource) []Nibble {
	var path []byte
	if source != nil && source.getConfig().UseHashedPaths {
		hash := source.hashKey(key)
		path = hash[:]
	} else {
		path = key[:]
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
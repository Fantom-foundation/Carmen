package rlp

import (
	"encoding/binary"
	"math/big"
)

// The definition of the RLP encoding can be found here:
// https://ethereum.org/en/developers/docs/data-structures-and-encoding/rlp
//
// Based on Appendix B of https://ethereum.github.io/yellowpaper/paper.pdf
//
// Recursive-Length Prefix (RLP) serialization is based on a recursive
// structure definition of an `item`. An item is defined as
//   - a string of bytes
//   - a list of items
// Note the recursive definition in the second line. This recursive step
// allows arbitraryily nested structures to be encoded. This package provides
// RLP encoding support for Items and a few convenience utilities for encoding
// frequently utilized types.

// Item is an interface for everything that can be RLP encoded by this package.
type Item interface {
	// write writes the RLP encoding of this item to the given writer.
	write(*writer)

	// getEncodedLength computes the encoded length of this item in bytes.
	getEncodedLength() int
}

// Encode is a convenience function for serializing an item structure.
func Encode(item Item) []byte {
	length := item.getEncodedLength()
	writer := writer(make([]byte, 0, length))
	item.write(&writer)
	return writer
}

// writer is a specialized writer for this package writing encoded RLP
// content in a pre-allocated buffer.
type writer []byte

func (w *writer) Write(data []byte) {
	*w = append(*w, data...)
}

func (w *writer) WriteByte(c byte) error {
	*w = append(*w, c)
	return nil
}

// ----------------------------------------------------------------------------
//                           Core Item Types
// ----------------------------------------------------------------------------

// String is the atomic ground type of an RLP input structure representing a
// (potentially empty) string of bytes.
type String struct {
	Str []byte
}

func (s String) write(writer *writer) {
	l := len(s.Str)
	// Single-element strings are encoded as a single byte if the
	// value is small enough.
	if l == 1 && s.Str[0] < 0x80 {
		writer.Write(s.Str)
		return
	}
	// For the rest, the length is encoded, followed by the string itself.
	encodeLength(l, 0x80, writer)
	writer.Write(s.Str)
}

func (s String) getEncodedLength() int {
	l := len(s.Str)
	if l == 1 && s.Str[0] < 0x80 {
		return 1
	}
	return l + getEncodedLengthLength(l)
}

// List composes a list of items into a new item to be serialized.
type List struct {
	Items []Item
}

func (l List) write(writer *writer) {
	length := 0
	for _, item := range l.Items {
		length += item.getEncodedLength()
	}
	encodeLength(length, 0xc0, writer)
	for _, item := range l.Items {
		item.write(writer)
	}
}

func (s List) getEncodedLength() int {
	sum := 0
	for _, item := range s.Items {
		sum += item.getEncodedLength()
	}
	return sum + getEncodedLengthLength(sum)
}

// encodeLength is utility function used by String and List structures to
// encode the length of the string or list in the output stream.
func encodeLength(length int, offset byte, writer *writer) {
	if length < 56 {
		writer.WriteByte(offset + byte(length))
		return
	}
	numBytesForLength := getNumBytes(uint64(length))
	writer.WriteByte(offset + 55 + numBytesForLength)
	for i := byte(0); i < numBytesForLength; i++ {
		writer.WriteByte(byte(length >> (8 * (numBytesForLength - i - 1))))
	}
}

// getNumBytes computes the minimum number of bytes required to represent
// the given value in big-endian encoding.
func getNumBytes(value uint64) byte {
	if value == 0 {
		return 0
	}
	for res := byte(1); ; res++ {
		if value >>= 8; value == 0 {
			return res
		}
	}
}

func getEncodedLengthLength(length int) int {
	if length < 56 {
		return 1
	}
	return int(getNumBytes(uint64(length))) + 1
}

// ----------------------------------------------------------------------------
//                           Utility Item Types
// ----------------------------------------------------------------------------

// Uint64 is an Item encoding unsigned integers into RLP by interpreting them
// as a string of bytes. The bytes are derived from the integer value by
// encoding it in big-endian byte order and removing leading zero-bytes.
type Uint64 struct {
	Value uint64
}

func (u Uint64) write(writer *writer) {
	// Uint64 values are encoded using their non-zero big-endian encoding suffix.
	if u.Value == 0 {
		writer.WriteByte(0x80)
		return
	}
	var buffer = make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, u.Value)
	for buffer[0] == 0 {
		buffer = buffer[1:]
	}
	String{Str: buffer}.write(writer)
}

func (u Uint64) getEncodedLength() int {
	if u.Value < 0x80 {
		return 1
	}
	return 1 + int(getNumBytes(u.Value))
}

// BigInt is an Item encoding big.Int values into RLP by interpreting them
// as a string of bytes. The encoding schema is implemented anologous to the
// Uint64 encoder above.
type BigInt struct {
	Value *big.Int
}

func (i BigInt) write(writer *writer) {
	// Based on: https://github.com/ethereum/go-ethereum/blob/v1.12.0/rlp/encbuffer.go#L152
	// Values that fit in 64 bit are encoded using the uint64 encoder.
	bitlen := i.Value.BitLen()
	if bitlen <= 64 {
		Uint64{Value: i.Value.Uint64()}.write(writer)
		return
	}
	// Integer is larger than 64 bits, encode from BigInt's Bits()
	// using big-endian order.
	const wordBytes = (32 << (uint64(^big.Word(0)) >> 63)) / 8
	length := ((bitlen + 7) & -8) >> 3
	index := length
	var buffer = make([]byte, length)
	for _, d := range i.Value.Bits() {
		for j := 0; j < wordBytes && index > 0; j++ {
			index--
			buffer[index] = byte(d)
			d >>= 8
		}
	}
	encodeLength(length, 0x80, writer)
	writer.Write(buffer)
}

func (i BigInt) getEncodedLength() int {
	bitlen := i.Value.BitLen()
	if bitlen <= 64 {
		return Uint64{Value: i.Value.Uint64()}.getEncodedLength()
	}
	length := ((bitlen + 7) & -8) >> 3
	return getEncodedLengthLength(length) + length
}

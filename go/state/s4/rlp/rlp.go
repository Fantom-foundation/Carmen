package rlp

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/big"
)

// The definition of the RLP encoding can be found here:
// https://ethereum.org/en/developers/docs/data-structures-and-encoding/rlp

// Based on Appendix B of https://ethereum.github.io/yellowpaper/paper.pdf

type Item interface {
	Write(io.Writer)
}

func Encode(item Item) []byte {
	var buffer bytes.Buffer
	item.Write(&buffer)
	return buffer.Bytes()
}

// ----------------------------------------------------------------------------
//                           Core Item Types
// ----------------------------------------------------------------------------

type String struct {
	Str []byte
}

func (s String) Write(writer io.Writer) {
	l := len(s.Str)
	// Single-element strings are encoded as a single byte if the
	// value is small enough.
	if l == 1 && s.Str[0] < 0x80 {
		writer.Write(s.Str)
		return
	}
	// For the rest, the lenght is encoded, followed by the string itself.
	encodeLength(l, 0x80, writer)
	writer.Write(s.Str)
}

type List struct {
	Items []Item
}

func (l List) Write(writer io.Writer) {
	// TODO: eliminate this temporary buffer by obtaining the
	// encoded length first, write the length, and then write
	// the result directly in the output writer.
	var buffer bytes.Buffer
	for _, item := range l.Items {
		item.Write(&buffer)
	}
	data := buffer.Bytes()
	encodeLength(len(data), 0xc0, writer)
	writer.Write(data)
}

func encodeLength(length int, offset byte, writer io.Writer) {
	if length < 56 {
		writer.Write([]byte{offset + byte(length)})
		return
	}
	numBytesForLength := getNumBytes(uint64(length))
	writer.Write([]byte{offset + 55 + numBytesForLength})
	for i := byte(0); i < numBytesForLength; i++ {
		writer.Write([]byte{byte(length >> (8 * (numBytesForLength - i - 1)))})
	}
}

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

// ----------------------------------------------------------------------------
//                           Utility Item Types
// ----------------------------------------------------------------------------

type Uint64 struct {
	Value uint64
}

func (u Uint64) Write(writer io.Writer) {
	// Uint64 values are encoded using their non-zero big-endian encoding suffix.
	if u.Value == 0 {
		writer.Write([]byte{0x80})
		return
	}
	var buffer = make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, u.Value)
	for buffer[0] == 0 {
		buffer = buffer[1:]
	}
	String{Str: buffer}.Write(writer)
}

type BigInt struct {
	Value *big.Int
}

func (i BigInt) Write(writer io.Writer) {
	// Based on: https://github.com/ethereum/go-ethereum/blob/v1.12.0/rlp/encbuffer.go#L152
	// Values that fit in 64 bit are encoded using the uint64 encoder.
	bitlen := i.Value.BitLen()
	if bitlen <= 64 {
		Uint64{Value: i.Value.Uint64()}.Write(writer)
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
	String{Str: buffer}.Write(writer)
}

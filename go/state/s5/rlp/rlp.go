package rlp

import (
	"bytes"
	"io"
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
	numBytesForLength := getNumBytes(length)
	writer.Write([]byte{offset + 55 + numBytesForLength})
	for i := byte(0); i < numBytesForLength; i++ {
		writer.Write([]byte{byte(length >> (8 * (numBytesForLength - i - 1)))})
	}
}

func getNumBytes(value int) byte {
	if value == 0 {
		return 0
	}
	if value < 256 {
		return 1
	}
	return 1 + getNumBytes(value/256)
}

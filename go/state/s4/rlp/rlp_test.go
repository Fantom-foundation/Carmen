package rlp

import (
	"bytes"
	"testing"
)

func TestEncoding_EncodeStrings(t *testing.T) {
	tests := []struct {
		input  []byte
		result []byte
	}{
		// empty string
		{[]byte{}, []byte{0x80}},

		// single values < 0x80
		{[]byte{0}, []byte{0}},
		{[]byte{1}, []byte{1}},
		{[]byte{2}, []byte{2}},
		{[]byte{0x7f}, []byte{0x7f}},

		// single values >= 0x80
		{[]byte{0x80}, []byte{0x81, 0x80}},
		{[]byte{0x81}, []byte{0x81, 0x81}},
		{[]byte{0xff}, []byte{0x81, 0xff}},

		// more than one element for short strings (< 56 bytes)
		{[]byte{0, 0}, []byte{0x82, 0, 0}},
		{[]byte{1, 2, 3}, []byte{0x83, 1, 2, 3}},

		{make([]byte, 55), func() []byte {
			res := make([]byte, 56)
			res[0] = 0x80 + 55
			return res
		}()},

		// 56 or more bytes
		{make([]byte, 56), func() []byte {
			res := make([]byte, 58)
			res[0] = 0xb7 + 1
			res[1] = 56
			return res
		}()},

		{make([]byte, 1024), func() []byte {
			res := make([]byte, 1027)
			res[0] = 0xb7 + 2
			res[1] = 1024 >> 8
			res[2] = 1024 & 0xff
			return res
		}()},

		{make([]byte, 1<<20), func() []byte {
			l := 1 << 20
			res := make([]byte, l+4)
			res[0] = 0xb7 + 3
			res[1] = byte(l >> 16)
			res[2] = byte(l >> 8)
			res[3] = byte(l)
			return res
		}()},
	}

	for _, test := range tests {
		if got, want := Encode(String{test.input}), test.result; !bytes.Equal(got, want) {
			t.Errorf("invalid encoding, wanted %v, got %v", want, got)
		}
	}
}

func TestEncoding_EncodeList(t *testing.T) {
	tests := []struct {
		input  []Item
		result []byte
	}{
		// empty list
		{[]Item{}, []byte{0xc0}},

		// single element list with short content
		{[]Item{&String{[]byte{1}}}, []byte{0xc1, 1}},
		{[]Item{&String{[]byte{1, 2}}}, []byte{0xc3, 0x82, 1, 2}},

		// multi-element list with short content
		{[]Item{&String{[]byte{1}}, &String{[]byte{2}}}, []byte{0xc2, 1, 2}},

		// list with long content
		{[]Item{&String{make([]byte, 100)}}, expand([]byte{0xf7 + 1, 102, 184, 100}, 4+100)},
	}

	for _, test := range tests {
		if got, want := Encode(List{test.input}), test.result; !bytes.Equal(got, want) {
			t.Errorf("invalid encoding, wanted %v, got %v", want, got)
		}
	}
}

func expand(prefix []byte, size int) []byte {
	res := make([]byte, size)
	copy(res[:], prefix[:])
	return res
}

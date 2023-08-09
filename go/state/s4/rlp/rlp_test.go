package rlp

import (
	"bytes"
	"math/big"
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

func TestEncoding_Uint64(t *testing.T) {
	tests := []struct {
		input  uint64
		result []byte
	}{
		{0, Encode(&String{[]byte{}})},
		{1, Encode(&String{[]byte{1}})},
		{2, Encode(&String{[]byte{2}})},

		{255, Encode(&String{[]byte{255}})},
		{256, Encode(&String{[]byte{1, 0}})},
		{257, Encode(&String{[]byte{1, 1}})},

		{1<<16 - 1, Encode(&String{[]byte{255, 255}})},
		{1 << 16, Encode(&String{[]byte{1, 0, 0}})},
		{1<<16 + 1, Encode(&String{[]byte{1, 0, 1}})},

		{1<<24 - 1, Encode(&String{[]byte{255, 255, 255}})},
		{1 << 24, Encode(&String{[]byte{1, 0, 0, 0}})},
		{1<<24 + 1, Encode(&String{[]byte{1, 0, 0, 1}})},

		{1<<32 - 1, Encode(&String{[]byte{255, 255, 255, 255}})},
		{1 << 32, Encode(&String{[]byte{1, 0, 0, 0, 0}})},
		{1<<32 + 1, Encode(&String{[]byte{1, 0, 0, 0, 1}})},

		{1<<56 - 1, Encode(&String{[]byte{255, 255, 255, 255, 255, 255, 255}})},
		{1 << 56, Encode(&String{[]byte{1, 0, 0, 0, 0, 0, 0, 0}})},
		{1<<56 + 1, Encode(&String{[]byte{1, 0, 0, 0, 0, 0, 0, 1}})},
	}
	for _, test := range tests {
		if got, want := Encode(Uint64{test.input}), test.result; !bytes.Equal(got, want) {
			t.Errorf("invalid encoding, wanted %v, got %v", want, got)
		}
	}
}

func TestEncoding_BigInt(t *testing.T) {
	tests := []struct {
		input  *big.Int
		result []byte
	}{
		{big.NewInt(0), Encode(&String{[]byte{}})},
		{big.NewInt(1), Encode(&String{[]byte{1}})},
		{big.NewInt(2), Encode(&String{[]byte{2}})},

		{big.NewInt(255), Encode(&String{[]byte{255}})},
		{big.NewInt(256), Encode(&String{[]byte{1, 0}})},
		{big.NewInt(257), Encode(&String{[]byte{1, 1}})},

		{big.NewInt(1<<16 - 1), Encode(&String{[]byte{255, 255}})},
		{big.NewInt(1 << 16), Encode(&String{[]byte{1, 0, 0}})},
		{big.NewInt(1<<16 + 1), Encode(&String{[]byte{1, 0, 1}})},

		{big.NewInt(1<<24 - 1), Encode(&String{[]byte{255, 255, 255}})},
		{big.NewInt(1 << 24), Encode(&String{[]byte{1, 0, 0, 0}})},
		{big.NewInt(1<<24 + 1), Encode(&String{[]byte{1, 0, 0, 1}})},

		{big.NewInt(1<<32 - 1), Encode(&String{[]byte{255, 255, 255, 255}})},
		{big.NewInt(1 << 32), Encode(&String{[]byte{1, 0, 0, 0, 0}})},
		{big.NewInt(1<<32 + 1), Encode(&String{[]byte{1, 0, 0, 0, 1}})},

		{big.NewInt(1<<56 - 1), Encode(&String{[]byte{255, 255, 255, 255, 255, 255, 255}})},
		{big.NewInt(1 << 56), Encode(&String{[]byte{1, 0, 0, 0, 0, 0, 0, 0}})},
		{big.NewInt(1<<56 + 1), Encode(&String{[]byte{1, 0, 0, 0, 0, 0, 0, 1}})},

		{new(big.Int).Lsh(big.NewInt(1), 64), Encode(&String{[]byte{1, 0, 0, 0, 0, 0, 0, 0, 0}})},
		{new(big.Int).Lsh(big.NewInt(1), 65), Encode(&String{[]byte{2, 0, 0, 0, 0, 0, 0, 0, 0}})},
		{new(big.Int).Lsh(big.NewInt(1), 66), Encode(&String{[]byte{4, 0, 0, 0, 0, 0, 0, 0, 0}})},
		{new(big.Int).Lsh(big.NewInt(1), 72), Encode(&String{[]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0}})},
	}
	for _, test := range tests {
		if got, want := Encode(BigInt{test.input}), test.result; !bytes.Equal(got, want) {
			t.Errorf("invalid encoding, wanted %v, got %v", want, got)
		}
	}
}

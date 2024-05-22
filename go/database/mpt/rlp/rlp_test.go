// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package rlp

import (
	"bytes"
	"fmt"
	"math/big"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestEncoding_EncodeStrings(t *testing.T) {
	testWithRlpStrings(t, func(t *testing.T, rlp []byte, item String) {
		testEncoder(t, rlp, item)
	})
}

func TestEncoding_EncodeList(t *testing.T) {
	testWithRlpLists(t, func(t *testing.T, rlp []byte, item List) {
		testEncoder(t, rlp, item)
	})
}

func TestEncoding_Uint64(t *testing.T) {
	testWithRlpUint64(t, func(t *testing.T, rlp []byte, item Uint64) {
		testEncoder(t, rlp, item)
	})
}

func TestEncoding_BigInt(t *testing.T) {
	testWithRlpBigInt(t, func(t *testing.T, rlp []byte, item BigInt) {
		testEncoder(t, rlp, item)
	})
}

func TestEncoding_EncodeHash(t *testing.T) {
	testWithRlpHash(t, func(t *testing.T, rlp []byte, item Hash) {
		testEncoder(t, rlp, item)
	})
}

func TestDecode_List(t *testing.T) {
	testWithRlpLists(t, func(t *testing.T, rlp []byte, item List) {
		testDecoder(t, rlp, item)
	})
}

func TestDecode_Strings(t *testing.T) {
	testWithRlpStrings(t, func(t *testing.T, rlp []byte, item String) {
		testDecoder(t, rlp, item)
	})
}

func TestDecode_Uint64(t *testing.T) {
	testWithRlpUint64(t, func(t *testing.T, rlp []byte, item Uint64) {
		testDecoder(t, rlp, item)
	})
}

func TestDecode_BigInt(t *testing.T) {
	testWithRlpBigInt(t, func(t *testing.T, rlp []byte, item BigInt) {
		testDecoder(t, rlp, item)
	})
}

func TestDecode_Hash(t *testing.T) {
	testWithRlpHash(t, func(t *testing.T, rlp []byte, item Hash) {
		testDecoder(t, rlp, item)
	})
}

func TestEncoding_EncodeEncoded(t *testing.T) {
	tests := [][]byte{
		{},
		{1},
		{1, 2},
		{1, 2, 3},
	}

	for _, test := range tests {
		if got, want := Encode(Encoded{test}), test; !bytes.Equal(got, want) {
			t.Errorf("invalid encoding, wanted %v, got %v", want, got)
		}
		if got, want := (Encoded{test}).getEncodedLength(), len(test); got != want {
			t.Errorf("invalid result for encoded length, wanted %d, got %d", want, got)
		}
	}
}

func TestEncoding_getNumBytes_Zero(t *testing.T) {
	if got, want := getNumBytes(0), byte(0); got != want {
		t.Errorf("invalid result for encoded length, wanted %d, got %d", want, got)
	}
}

func TestReadSize_All_Correct_Sizes(t *testing.T) {
	want := uint64(0)
	for i := 1; i <= 8; i++ {
		b := make([]byte, i)
		for j := 0; j < i; j++ {
			b[j] = byte(0xFF)
		}
		got, err := readSize(b, byte(i))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		want = want<<8 | 0xFF
		if got, want := got, want; got != want {
			t.Errorf("invalid result for readSize, wanted %d, got %d", want, got)
		}
	}
}

func TestReadSize_All_InCorrect_Size(t *testing.T) {
	b := make([]byte, 1)
	if _, err := readSize(b, 4); err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestDecoder_Corrupted_RLPs(t *testing.T) {
	tests := [][]byte{
		{},                        // empty
		{0x80 + 1},                // short string byte with missing payload
		{0xb7 + 1},                // long string with missing payload
		{0xc0 + 1},                // short list with missing payload
		{0xf7 + 1},                // long list with missing payload
		{0x80, 0x80},              // two short strings
		{0xc0 + 2, 0xc0 + 2, 0x1}, // short list inner list missing payload
	}

	for _, rlp := range tests {
		t.Run(fmt.Sprintf("%x", rlp), func(t *testing.T) {
			if _, err := Decode(rlp); err == nil {
				t.Errorf("expected error, got nil")
			}
		})
	}
}

// testEncoder runs a test for encoding an item.
func testEncoder(t *testing.T, rlp []byte, item Item) {
	t.Run(fmt.Sprintf("%x->%x", item, rlp), func(t *testing.T) {
		if got, want := Encode(item), rlp; !bytes.Equal(got, want) {
			t.Errorf("invalid encoding, wanted %v, got %v, input %v", want, got, rlp)
		}
		if got, want := item.getEncodedLength(), len(rlp); got != want {
			t.Errorf("invalid result for encoded length, wanted %d, got %d, input %v", want, got, rlp)
		}
	})
}

// testDecoder runs a test for decoding an item.
func testDecoder(t *testing.T, rlp []byte, item Item) {
	t.Run(fmt.Sprintf("%x->%x", rlp, item), func(t *testing.T) {
		got, err := Decode(rlp)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got, want := got, item; !equal(got, want) {
			t.Errorf("invalid encoding, wanted %v, got %v, input %v", want, got, rlp)
		}
	})
}

// testWithRlpStrings runs a test function with a set of RLP strings.
func testWithRlpStrings(t *testing.T, action func(t *testing.T, rlp []byte, item String)) {
	tests := []struct {
		rlp  []byte
		item String
	}{

		// empty string
		{[]byte{0x80}, String{}},

		// single values < 0x80
		{[]byte{0}, String{[]byte{0}}},
		{[]byte{1}, String{[]byte{1}}},
		{[]byte{2}, String{[]byte{2}}},
		{[]byte{0x7f}, String{[]byte{0x7f}}},

		// single values >= 0x80
		{[]byte{0x81, 0x80}, String{[]byte{0x80}}},
		{[]byte{0x81, 0x81}, String{[]byte{0x81}}},
		{[]byte{0x81, 0xff}, String{[]byte{0xff}}},

		// more than one element for short strings (< 56 bytes)
		{[]byte{0x82, 0, 0}, String{[]byte{0, 0}}},
		{[]byte{0x83, 1, 2, 3}, String{[]byte{1, 2, 3}}},

		{func() []byte {
			res := make([]byte, 56)
			res[0] = 0x80 + 55
			return res
		}(), String{make([]byte, 55)}},

		// 56 or more bytes
		{func() []byte {
			res := make([]byte, 58)
			res[0] = 0xb7 + 1
			res[1] = 56
			return res
		}(), String{make([]byte, 56)}},

		{func() []byte {
			res := make([]byte, 1027)
			res[0] = 0xb7 + 2
			res[1] = 1024 >> 8
			res[2] = 1024 & 0xff
			return res
		}(), String{make([]byte, 1024)}},

		{func() []byte {
			l := 1 << 20
			res := make([]byte, l+4)
			res[0] = 0xb7 + 3
			res[1] = byte(l >> 16)
			res[2] = byte(l >> 8)
			res[3] = byte(l)
			return res
		}(), String{make([]byte, 1<<20)}},
	}

	for _, test := range tests {
		action(t, test.rlp, test.item)
	}
}

func testWithRlpLists(t *testing.T, action func(t *testing.T, rlp []byte, item List)) {
	tests := []struct {
		item []Item
		rlp  []byte
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
		action(t, test.rlp, List{test.item})
	}
}

// testWithRlpUint64 runs a test function with a set of Uint64 values.
func testWithRlpUint64(t *testing.T, action func(t *testing.T, rlp []byte, item Uint64)) {
	tests := []struct {
		item Uint64
		rlp  []byte
	}{
		{Uint64{0}, Encode(&String{[]byte{}})},
		{Uint64{1}, Encode(&String{[]byte{1}})},
		{Uint64{2}, Encode(&String{[]byte{2}})},

		{Uint64{255}, Encode(&String{[]byte{255}})},
		{Uint64{256}, Encode(&String{[]byte{1, 0}})},
		{Uint64{257}, Encode(&String{[]byte{1, 1}})},

		{Uint64{1<<16 - 1}, Encode(&String{[]byte{255, 255}})},
		{Uint64{1 << 16}, Encode(&String{[]byte{1, 0, 0}})},
		{Uint64{1<<16 + 1}, Encode(&String{[]byte{1, 0, 1}})},

		{Uint64{1<<24 - 1}, Encode(&String{[]byte{255, 255, 255}})},
		{Uint64{1 << 24}, Encode(&String{[]byte{1, 0, 0, 0}})},
		{Uint64{1<<24 + 1}, Encode(&String{[]byte{1, 0, 0, 1}})},

		{Uint64{1<<32 - 1}, Encode(&String{[]byte{255, 255, 255, 255}})},
		{Uint64{1 << 32}, Encode(&String{[]byte{1, 0, 0, 0, 0}})},
		{Uint64{1<<32 + 1}, Encode(&String{[]byte{1, 0, 0, 0, 1}})},

		{Uint64{1<<56 - 1}, Encode(&String{[]byte{255, 255, 255, 255, 255, 255, 255}})},
		{Uint64{1 << 56}, Encode(&String{[]byte{1, 0, 0, 0, 0, 0, 0, 0}})},
		{Uint64{1<<56 + 1}, Encode(&String{[]byte{1, 0, 0, 0, 0, 0, 0, 1}})},
	}

	for _, test := range tests {
		action(t, test.rlp, test.item)
	}
}

func testWithRlpBigInt(t *testing.T, action func(t *testing.T, rlp []byte, item BigInt)) {
	tests := []struct {
		item BigInt
		rlp  []byte
	}{
		{BigInt{big.NewInt(0)}, Encode(&String{[]byte{}})},
		{BigInt{big.NewInt(1)}, Encode(&String{[]byte{1}})},
		{BigInt{big.NewInt(2)}, Encode(&String{[]byte{2}})},

		{BigInt{big.NewInt(255)}, Encode(&String{[]byte{255}})},
		{BigInt{big.NewInt(256)}, Encode(&String{[]byte{1, 0}})},
		{BigInt{big.NewInt(257)}, Encode(&String{[]byte{1, 1}})},

		{BigInt{big.NewInt(1<<16 - 1)}, Encode(&String{[]byte{255, 255}})},
		{BigInt{big.NewInt(1 << 16)}, Encode(&String{[]byte{1, 0, 0}})},
		{BigInt{big.NewInt(1<<16 + 1)}, Encode(&String{[]byte{1, 0, 1}})},

		{BigInt{big.NewInt(1<<24 - 1)}, Encode(&String{[]byte{255, 255, 255}})},
		{BigInt{big.NewInt(1 << 24)}, Encode(&String{[]byte{1, 0, 0, 0}})},
		{BigInt{big.NewInt(1<<24 + 1)}, Encode(&String{[]byte{1, 0, 0, 1}})},

		{BigInt{big.NewInt(1<<32 - 1)}, Encode(&String{[]byte{255, 255, 255, 255}})},
		{BigInt{big.NewInt(1 << 32)}, Encode(&String{[]byte{1, 0, 0, 0, 0}})},
		{BigInt{big.NewInt(1<<32 + 1)}, Encode(&String{[]byte{1, 0, 0, 0, 1}})},

		{BigInt{big.NewInt(1<<56 - 1)}, Encode(&String{[]byte{255, 255, 255, 255, 255, 255, 255}})},
		{BigInt{big.NewInt(1 << 56)}, Encode(&String{[]byte{1, 0, 0, 0, 0, 0, 0, 0}})},
		{BigInt{big.NewInt(1<<56 + 1)}, Encode(&String{[]byte{1, 0, 0, 0, 0, 0, 0, 1}})},

		{BigInt{new(big.Int).Lsh(big.NewInt(1), 64)}, Encode(&String{[]byte{1, 0, 0, 0, 0, 0, 0, 0, 0}})},
		{BigInt{new(big.Int).Lsh(big.NewInt(1), 65)}, Encode(&String{[]byte{2, 0, 0, 0, 0, 0, 0, 0, 0}})},
		{BigInt{new(big.Int).Lsh(big.NewInt(1), 66)}, Encode(&String{[]byte{4, 0, 0, 0, 0, 0, 0, 0, 0}})},
		{BigInt{new(big.Int).Lsh(big.NewInt(1), 72)}, Encode(&String{[]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0}})},
	}

	for _, test := range tests {
		action(t, test.rlp, test.item)
	}
}

func testWithRlpHash(t *testing.T, action func(t *testing.T, rlp []byte, item Hash)) {
	type test struct {
		item common.Hash
		rlp  []byte
	}
	const size = 32
	tests := make([]test, 0, size)
	var hash common.Hash
	for i := 0; i < size; i++ {
		hash[i] = byte(i)
		tests = append(tests, test{hash, append([]byte{0xA0}, hash[:]...)})
	}

	for _, test := range tests {
		action(t, test.rlp, Hash{&test.item})
	}
}

func expand(prefix []byte, size int) []byte {
	res := make([]byte, size)
	copy(res[:], prefix[:])
	return res
}

func equal(a, b Item) bool {
	if a == nil || b == nil {
		return a == b
	}

	return bytes.Equal(Encode(a), Encode(b))
}

func BenchmarkListEncoding(b *testing.B) {
	example := &List{
		[]Item{
			&String{[]byte("hello")},
			&String{[]byte("world")},
			&List{
				[]Item{
					&String{[]byte("nested")},
					&String{[]byte("content")},
				},
			},
			// Some 'hashes'
			&String{make([]byte, 32)},
			&String{make([]byte, 32)},
			&List{
				[]Item{
					&String{[]byte("1")},
					&String{[]byte("2")},
					&String{[]byte("3")},
					&String{[]byte("4")},
					&String{[]byte("5")},
					&String{[]byte("6")},
					&String{[]byte("7")},
					&String{[]byte("8")},
					&String{[]byte("9")},
					&String{[]byte("10")},
					&String{[]byte("11")},
					&String{[]byte("12")},
					&String{[]byte("13")},
					&String{[]byte("14")},
					&String{[]byte("15")},
					&String{[]byte("16")},
				},
			},
		},
	}

	for i := 0; i < b.N; i++ {
		Encode(example)
	}
}

// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package common

import (
	"fmt"
	"testing"
)

var nonce_value_pairs = []struct {
	i uint64
	n Nonce
}{
	{0, Nonce{}},
	{1, Nonce{0, 0, 0, 0, 0, 0, 0, 1}},
	{2, Nonce{0, 0, 0, 0, 0, 0, 0, 2}},
	{256, Nonce{0, 0, 0, 0, 0, 0, 1, 0}},
	{1 << 32, Nonce{0, 0, 0, 1, 0, 0, 0, 0}},
	{^uint64(0), Nonce{255, 255, 255, 255, 255, 255, 255, 255}},
}

func TestUint64ToNonceConversion(t *testing.T) {
	for _, pair := range nonce_value_pairs {
		nonce := ToNonce(pair.i)
		if nonce != pair.n {
			t.Errorf("Incorrect conversion of numeric value %v into nonce - wanted %v, got %v", pair.i, pair.n, nonce)
		}
	}
}

func TestNonceToUint64Conversion(t *testing.T) {
	for _, pair := range nonce_value_pairs {
		val := pair.n.ToUint64()
		if val != pair.i {
			t.Errorf("Incorrect conversion of nonce %v into numeric value - wanted %v, got %v", pair.n, pair.i, val)
		}
	}
}

func TestKeccak256NilHashesLikeEmptyList(t *testing.T) {
	nil_hash := GetKeccak256Hash(nil)
	empty_hash := GetKeccak256Hash([]byte{})
	if nil_hash != empty_hash {
		t.Errorf("nil does not hash like empty slice, got %x, wanted %x", nil_hash, empty_hash)
	}
}

func TestKeccak256KnownHashes(t *testing.T) {
	inputs := []struct {
		plain, hash string
	}{
		{"", "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"},
		{"a", "3ac225168df54212a25c1c01fd35bebfea408fdac2e31ddd6f80a4bbf9a5f1cb"},
		{"abc", "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45"},
	}
	for _, input := range inputs {
		hash := GetKeccak256Hash([]byte(input.plain))
		if input.hash != fmt.Sprintf("%x", hash) {
			t.Errorf("invalid hash: %x (expected %s)", hash, input.hash)
		}
	}
}

func TestHashFromString(t *testing.T) {
	tests := []struct {
		input  string
		result Hash
	}{
		{"0000000000000000000000000000000000000000000000000000000000000000", Hash{}},
		{"1000000000000000000000000000000000000000000000000000000000000000", Hash{0x10}},
		{"1200000000000000000000000000000000000000000000000000000000000000", Hash{0x12}},
		{"123456789abcdefABCDEF0000000000000000000000000000000000000000000", Hash{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xfa, 0xbc, 0xde, 0xf0}},
	}

	for _, test := range tests {
		if got, want := HashFromString(test.input), test.result; got != want {
			t.Errorf("failed to parse %s: expected %v, got %v", test.input, want, got)
		}
	}
}

func TestHashFromString_Panic_ShortString(t *testing.T) {
	s := "123456789abcdefABCDEF000000000000 Good Morning 00000000000000000"
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("method call did not panic")
		}
	}()

	HashFromString(s)
}

func TestHashFromString_Panic_NonHexString(t *testing.T) {
	s := "abc"
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("method call did not panic")
		}
	}()

	HashFromString(s)
}

func TestTypes_AccountStateString(t *testing.T) {
	if got, want := Unknown.String(), "unknown"; got != want {
		t.Errorf("unexpectd string: %s != %s", got, want)
	}
	if got, want := Exists.String(), "exists"; got != want {
		t.Errorf("unexpectd string: %s != %s", got, want)
	}
	if got, want := AccountState(0xAA).String(), "invalid"; got != want {
		t.Errorf("unexpectd string: %s != %s", got, want)
	}
}

func TestTypes_Comparators(t *testing.T) {
	t.Run("TestTypesComparator_Address", func(t *testing.T) {
		var a, b Address
		b[0]++
		testTypesComparators[Address](t, &a, &b, AddressComparator{})
	})
	t.Run("TestTypesComparator_Key", func(t *testing.T) {
		var a, b Key
		b[0]++
		testTypesComparators[Key](t, &a, &b, KeyComparator{})
	})
	t.Run("TestTypesComparator_Hash", func(t *testing.T) {
		var a, b Hash
		b[0]++
		testTypesComparators[Hash](t, &a, &b, HashComparator{})
	})
	t.Run("TestTypesComparator_Uint32Comparator", func(t *testing.T) {
		var a, b uint32
		b = 1
		testTypesComparators[uint32](t, &a, &b, Uint32Comparator{})
	})
	t.Run("TestTypesComparator_Uint64Comparator", func(t *testing.T) {
		var a, b uint64
		b = 1
		testTypesComparators[uint64](t, &a, &b, Uint64Comparator{})
	})
	t.Run("TestTypesComparator_SlotIdx32Comparator", func(t *testing.T) {
		var a, b SlotIdx[uint32]
		b.KeyIdx = 1
		testTypesComparators[SlotIdx[uint32]](t, &a, &b, SlotIdx32Comparator{})
	})
	t.Run("TestTypesComparator_SlotIdx32KeyComparator", func(t *testing.T) {
		var key Key
		key[0]++
		var a, b SlotIdxKey[uint32]
		b.AddressIdx = 1
		b.Key = key
		testTypesComparators[SlotIdxKey[uint32]](t, &a, &b, SlotIdx32KeyComparator{})
	})
}

func testTypesComparators[T any](t *testing.T, a, b *T, cmp Comparator[T]) {
	if cmp.Compare(a, b) > 0 {
		t.Errorf("a < b does not hold: %v, %v", a, b)
	}

	if cmp.Compare(b, a) < 0 {
		t.Errorf("b > a does not hold: %v, %v", a, b)
	}

	if cmp.Compare(a, a) != 0 {
		t.Errorf("a == a does not hold: %v, %v", a, b)
	}
}

func TestTypes_Hash(t *testing.T) {
	t.Run("TestTypesHash_Address", func(t *testing.T) {
		var a Address
		var pos int
		f := func() *Address {
			a[pos%20]++
			pos++
			return &a
		}
		testTypesHash[Address](t, f, AddressHasher{})
	})

	t.Run("TestTypesHash_Key", func(t *testing.T) {
		var a Key
		var pos int
		f := func() *Key {
			a[pos%32]++
			pos++
			return &a
		}
		testTypesHash[Key](t, f, KeyHasher{})
	})

	t.Run("TestTypesHash_Hash", func(t *testing.T) {
		var a Hash
		var pos int
		f := func() *Hash {
			a[pos%32]++
			pos++
			return &a
		}
		testTypesHash[Hash](t, f, HashHasher{})
	})

	t.Run("TestTypesHash_UInt32", func(t *testing.T) {
		var a uint32
		f := func() *uint32 {
			a++
			return &a
		}
		testTypesHash[uint32](t, f, UInt32Hasher{})
	})
	t.Run("TestTypesHash_SlotIdx32Hasher", func(t *testing.T) {
		var a SlotIdx[uint32]
		f := func() *SlotIdx[uint32] {
			a.KeyIdx++
			a.AddressIdx++
			return &a
		}
		testTypesHash[SlotIdx[uint32]](t, f, SlotIdx32Hasher{})
	})
	t.Run("TestTypesHash_SlotIdxKey", func(t *testing.T) {
		var pos int
		var a SlotIdxKey[uint32]
		f := func() *SlotIdxKey[uint32] {
			a.Key[pos%32]++
			a.AddressIdx++
			pos++
			return &a
		}
		testTypesHash[SlotIdxKey[uint32]](t, f, SlotIdx32KeyHasher{})
	})
}

func testTypesHash[T any](t *testing.T, next func() *T, h Hasher[T]) {
	var prev uint64
	for i := 0; i < 1000; i++ {
		hash := h.Hash(next())
		if hash == 0 {
			t.Errorf("hash is zero")
		}
		if hash == prev {
			t.Errorf("hash colision: %v == %v", hash, prev)
		}
		prev = hash
	}
}

func TestTypes_HashToBytes(t *testing.T) {
	var v Hash
	for i := 0; i < 32; i++ {
		v[i]++
	}
	b := v.ToBytes()

	if got, want := len(b), len(v); got != want {
		t.Errorf("sizes do not match: %d != %d", got, want)
	}

	for i := 0; i < len(b); i++ {
		if got, want := b[i], v[i]; got != want {
			t.Errorf("bytes do not match: %d != %d (pos: %d)", b, v, i)
		}
	}
}

// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package mpt

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/rlp"
	"slices"
	"testing"
)

func TestDecoder_CanDecodeNodes(t *testing.T) {
	hash := common.Keccak256([]byte{0x01, 0x02, 0x03, 0x04})

	value := []byte{0x11, 0x22, 0x33, 0x44}
	var commonValue common.Value
	copy(commonValue[:], value[:])

	valueNode := rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x20, 0x12, 0x34}}, rlp.String{Str: value}}}
	valueNodeRlp := rlp.Encode(valueNode)
	var valueNodeRlpAsHash common.Hash
	copy(valueNodeRlpAsHash[:], valueNodeRlp)
	valueNodeRlpAsHash[len(valueNodeRlp)] = 0xF // add termination mark

	childrenHashes := [16]common.Hash{
		hash,
		hash,
		EmptyNodeEthereumHash,
		hash,
		hash,
		hash,
		hash,
		EmptyNodeEthereumHash,
		EmptyNodeEthereumHash,
		hash,
		hash,
		EmptyNodeEthereumHash,
		hash,
		valueNodeRlpAsHash,
		valueNodeRlpAsHash,
		hash,
	}

	embeddedChildrenSizes := [16]uint16{}

	childrenRlp := make([]rlp.Item, 17)
	for i := 0; i < 16; i++ {
		childrenRlp[i] = rlp.String{Str: childrenHashes[i][:]}
		if childrenHashes[i] == valueNodeRlpAsHash {
			childrenRlp[i] = rlp.String{Str: valueNodeRlp} // inject real size slice, not a 32bit hash
			embeddedChildrenSizes[i] = uint16(len(valueNodeRlp))
		}
	}
	childrenRlp[16] = rlp.String{}

	nonce := common.Nonce{0xAA}
	balance := common.Balance{0xBB}
	accountDetailEmptyStorage := rlp.List{Items: []rlp.Item{
		rlp.Uint64{Value: nonce.ToUint64()},
		rlp.BigInt{Value: balance.ToBigInt()},
		rlp.String{Str: EmptyNodeEthereumHash[:]},
		rlp.String{Str: hash[:]}},
	}

	accountDetailStorage := rlp.List{Items: []rlp.Item{
		rlp.Uint64{Value: nonce.ToUint64()},
		rlp.BigInt{Value: balance.ToBigInt()},
		rlp.String{Str: hash[:]},
		rlp.String{Str: hash[:]}},
	}

	key1 := common.Key{0x1, 0x2, 0x3, 0x4}
	key2 := common.Key{0x1, 0x2, 0x3, 0x4, 0x5}

	address1 := common.Address{0x1, 0x2, 0x3, 0x4}
	address2 := common.Address{0x1, 0x2, 0x3, 0x4, 0x5}

	tests := map[string]struct {
		item     rlp.Item
		expected Node
	}{
		"empty": {
			rlp.String{},
			EmptyNode{},
		},
		"even extension": {
			rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x00, 0x12, 0x34}}, rlp.String{Str: hash[:]}}},
			&ExtensionNode{path: CreatePathFromNibbles([]Nibble{0x1, 0x2, 0x3, 0x4}), nextHash: hash, nextIsEmbedded: false},
		},
		"odd extension": {
			rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x11, 0x23, 0x45}}, rlp.String{Str: hash[:]}}},
			&ExtensionNode{path: CreatePathFromNibbles([]Nibble{0x1, 0x2, 0x3, 0x4, 0x5}), nextHash: hash, nextIsEmbedded: false},
		},
		"even extension - embedded": {
			rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x00, 0x12, 0x34}}, rlp.String{Str: valueNodeRlp}}},
			&ExtensionNode{path: CreatePathFromNibbles([]Nibble{0x1, 0x2, 0x3, 0x4}), nextHash: valueNodeRlpAsHash, nextIsEmbedded: true},
		},
		"odd extension - embedded": {
			rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x11, 0x23, 0x45}}, rlp.String{Str: valueNodeRlp}}},
			&ExtensionNode{path: CreatePathFromNibbles([]Nibble{0x1, 0x2, 0x3, 0x4, 0x5}), nextHash: valueNodeRlpAsHash, nextIsEmbedded: true},
		},
		"even value": {
			rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x20, 0x12, 0x34}}, rlp.String{Str: value}}},
			&ValueNode{key: key1, value: commonValue, pathLength: 4},
		},
		"odd value": {
			rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x31, 0x23, 0x45}}, rlp.String{Str: value}}},
			&ValueNode{key: key2, value: commonValue, pathLength: 5},
		},
		"branch": {
			rlp.List{Items: childrenRlp},
			&BranchNode{hashes: childrenHashes, embeddedChildren: (1 << 14) | (1 << 13)},
		},
		"even account empty storage ": {
			rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x20, 0x12, 0x34}}, rlp.String{Str: rlp.Encode(accountDetailEmptyStorage)}}},
			&AccountNode{address: address1, info: AccountInfo{nonce, balance, hash}, storageHash: EmptyNodeEthereumHash, pathLength: 4},
		},
		"even account with storage ": {
			rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x20, 0x12, 0x34}}, rlp.String{Str: rlp.Encode(accountDetailStorage)}}},
			&AccountNode{address: address1, info: AccountInfo{nonce, balance, hash}, storageHash: hash, pathLength: 4},
		},
		"odd account empty storage ": {
			rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x31, 0x23, 0x45}}, rlp.String{Str: rlp.Encode(accountDetailEmptyStorage)}}},
			&AccountNode{address: address2, info: AccountInfo{nonce, balance, hash}, storageHash: EmptyNodeEthereumHash, pathLength: 5},
		},
		"odd account with storage ": {
			rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x31, 0x23, 0x45}}, rlp.String{Str: rlp.Encode(accountDetailStorage)}}},
			&AccountNode{address: address2, info: AccountInfo{nonce, balance, hash}, storageHash: hash, pathLength: 5},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			rlp := rlp.Encode(test.item)
			got, err := DecodeFromRlp(rlp)
			if err != nil {
				t.Fatalf("failed to decode node: %v", err)
			}

			matchNodesRlpDecoded(t, test.expected, got)
		})
	}
}

func TestDecoder_DecodeEmbeddedNode_CanDecode(t *testing.T) {
	value := []byte{0x11, 0x22, 0x33, 0x44}
	valueNode := rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x20, 0x12, 0x34}}, rlp.String{Str: value}}}
	valueNodeRlp := rlp.Encode(valueNode)

	var commonValue common.Value
	copy(commonValue[:], value[:])

	extNodeRlp := rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x11, 0x23, 0x45}}, rlp.String{Str: valueNodeRlp}}}

	extNode, err := DecodeFromRlp(rlp.Encode(extNodeRlp))
	if err != nil {
		t.Fatalf("failed to decode node: %v", err)
	}

	got, err := DecodeEmbeddedFromRlp(extNode.(*ExtensionNode).nextHash[:])
	if err != nil {
		t.Fatalf("failed to decode node: %v", err)
	}

	expectedValueNode := ValueNode{key: common.Key{0x1, 0x2, 0x3, 0x4}, value: commonValue, pathLength: 4}
	if matchNodesRlpDecoded(t, &expectedValueNode, got); err != nil {
		t.Fatalf("failed to match nodes: %v", err)
	}
}

func TestDecoder_DecodeEmbeddedNode_Malformed(t *testing.T) {
	value := []byte{0x11, 0x22, 0x33, 0x44}
	valueNode := rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x20, 0x12, 0x34}}, rlp.String{Str: value}}}
	valueNodeRlp := rlp.Encode(valueNode)

	var embeddedMissingTerminal common.Hash

	var embeddedAsHash common.Hash
	copy(embeddedAsHash[:], valueNodeRlp)
	embeddedAsHash[len(valueNodeRlp)] = 0xF     // add termination mark
	embeddedAsHash[len(embeddedAsHash)-1] = 0xC // non-zero padding

	tests := map[string][]byte{
		"too long":         make([]byte, 35),
		"missing terminal": embeddedMissingTerminal[:],
		"non-zero-padded":  embeddedAsHash[:],
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeEmbeddedFromRlp(test); err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func TestDecoder_CorruptedRlp(t *testing.T) {
	str := rlp.String{Str: []byte{0xFF}}
	hash := common.Keccak256([]byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF})
	longStr := rlp.String{Str: hash[:]}
	strLongerThan32 := rlp.String{Str: make([]byte, 33)}
	for i := 0; i < len(strLongerThan32.Str); i++ {
		strLongerThan32.Str[i] = 0xFF
	}
	threeItemsList := rlp.String{Str: rlp.Encode(rlp.List{Items: []rlp.Item{strLongerThan32, str, str}})}
	tooLongNumberItemsList := rlp.String{Str: rlp.Encode(rlp.List{Items: []rlp.Item{longStr, longStr, longStr, longStr}})}

	list := rlp.List{Items: []rlp.Item{strLongerThan32, str, str, str}}
	trailingBytes := rlp.Encode(threeItemsList)
	trailingBytes = append(trailingBytes, 0x01, 0x02, 0x03)

	nonListSubItem := rlp.String{Str: rlp.Encode(strLongerThan32)}

	nonceNotStr := rlp.String{Str: rlp.Encode(rlp.List{Items: []rlp.Item{rlp.List{}, longStr, longStr, longStr}})}
	nonceWrongNumber := rlp.String{Str: rlp.Encode(rlp.List{Items: []rlp.Item{longStr, longStr, longStr, longStr}})}

	balanceNotStr := rlp.String{Str: rlp.Encode(rlp.List{Items: []rlp.Item{str, rlp.List{}, longStr, longStr}})}
	balanceWrongNumber := rlp.String{Str: rlp.Encode(rlp.List{Items: []rlp.Item{str, strLongerThan32, longStr, longStr}})}

	storageNotStr := rlp.String{Str: rlp.Encode(rlp.List{Items: []rlp.Item{str, str, rlp.List{}, longStr}})}
	storageTooLong := rlp.String{Str: rlp.Encode(rlp.List{Items: []rlp.Item{str, str, strLongerThan32, longStr}})}

	codeHashNotStr := rlp.String{Str: rlp.Encode(rlp.List{Items: []rlp.Item{str, str, longStr, rlp.List{}}})}
	codeHashTooLong := rlp.String{Str: rlp.Encode(rlp.List{Items: []rlp.Item{str, str, longStr, strLongerThan32}})}

	childrenTooLongHashes := make([]rlp.Item, 17)
	for i := 0; i < len(childrenTooLongHashes); i++ {
		childrenTooLongHashes[i] = strLongerThan32
	}

	childrenNotStrings := make([]rlp.Item, 17)
	for i := 0; i < len(childrenNotStrings); i++ {
		childrenNotStrings[i] = list
	}

	tests := map[string][]byte{
		"":                                         {},
		"single string":                            rlp.EncodeInto([]byte{}, str),
		"3 items list":                             rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{str, str, str}}),
		"two items node, path is list":             rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{list, str}}),
		"possible value but nested list":           rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x31, 0x23, 0x45}}, rlp.List{}}}),
		"possible value but too long key":          rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{rlp.String{Str: append([]byte{0x31, 0x23, 0x45}, strLongerThan32.Str...)}, str}}),
		"possible ext but nested list":             rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x00, 0x12, 0x34}}, rlp.List{}}}),
		"possible ext but too long hash":           rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{rlp.String{Str: []byte{0x00, 0x12, 0x34}}, strLongerThan32}}),
		"possible account but 3 items nested list": rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{str, threeItemsList}}),
		"possible account but nested empty":        rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{str, rlp.String{Str: trailingBytes}}}),
		"possible account but nested not list":     rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{str, nonListSubItem}}),
		"possible account but nonce not string":    rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{str, nonceNotStr}}),
		"possible account but nonce wrong number":  rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{str, nonceWrongNumber}}),
		"possible account but balance not string":  rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{str, balanceNotStr}}),
		"possible account but balance too long":    rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{str, balanceWrongNumber}}),
		"possible account but storage not string":  rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{str, storageNotStr}}),
		"possible account but storage too long":    rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{str, storageTooLong}}),
		"possible account but codeHash not string": rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{str, codeHashNotStr}}),
		"possible account but codeHash too long":   rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{str, codeHashTooLong}}),
		"possible account but long address":        rlp.EncodeInto([]byte{}, rlp.List{Items: []rlp.Item{tooLongNumberItemsList, strLongerThan32}}),
		"possible branch too long child":           rlp.EncodeInto([]byte{}, rlp.List{Items: childrenTooLongHashes}),
		"possible branch child not strings":        rlp.EncodeInto([]byte{}, rlp.List{Items: childrenNotStrings}),
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeFromRlp(test); err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func Test_isCompactPathLeafNode(t *testing.T) {
	tests := make([]struct {
		path   []byte
		isLeaf bool
	}, 0, 0xFF)

	for i := 0; i < 0xFF; i++ {
		tests = append(tests, struct {
			path   []byte
			isLeaf bool
		}{path: []byte{byte(i)}, isLeaf: i&0b_0010_0000 != 0})

	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test), func(t *testing.T) {
			if got, want := isEncodedLeafNode(test.path), test.isLeaf; got != want {
				t.Errorf("unexpected result, got %v, want %v", got, want)
			}
		})
	}
}

func Test_compactPathToNibbles(t *testing.T) {
	tests := map[string]struct {
		path    []byte
		nibbles []Nibble
	}{
		"even extension": {[]byte{0x00, 0x12, 0x34}, []Nibble{0x1, 0x2, 0x3, 0x4}},
		"odd extension":  {[]byte{0x11, 0x23, 0x45}, []Nibble{0x1, 0x2, 0x3, 0x4, 0x5}},
		"even leaf":      {[]byte{0x20, 0x12, 0x34}, []Nibble{0x1, 0x2, 0x3, 0x4}},
		"odd leaf":       {[]byte{0x31, 0x23, 0x45}, []Nibble{0x1, 0x2, 0x3, 0x4, 0x5}},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if got, want := compactPathToNibbles(test.path), test.nibbles; !slices.Equal(got, want) {
				t.Errorf("unexpected result, got %v, want %v", got, want)
			}
		})
	}
}

func matchNodesRlpDecoded(t *testing.T, a, b Node) {
	t.Helper()

	switch aa := a.(type) {
	case EmptyNode:
		if _, ok := b.(EmptyNode); !ok {
			t.Fatalf("expected EmptyNode, got %T", b)
		}
	case *ExtensionNode:
		bb, ok := b.(*ExtensionNode)
		if !ok {
			t.Fatalf("expected *ExtensionNode, got %T", b)
		}
		if aa.path != bb.path {
			t.Errorf("expected path %v, got %v", aa.path, bb.path)
		}
		if aa.nextHash != bb.nextHash {
			t.Errorf("expected nextHash %v, got %v", aa.nextHash, bb.nextHash)
		}
		if aa.nextIsEmbedded != bb.nextIsEmbedded {
			t.Errorf("expected nextIsEmbedded %v, got %v", aa.nextIsEmbedded, bb.nextIsEmbedded)
		}
	case *ValueNode:
		bb, ok := b.(*ValueNode)
		if !ok {
			t.Fatalf("expected *ValueNode, got %T", b)
		}
		if aa.key != bb.key {
			t.Errorf("expected key %v, got %v", aa.key, bb.key)
		}
		if aa.value != bb.value {
			t.Errorf("expected value %v, got %v", aa.value, bb.value)
		}
		if aa.pathLength != bb.pathLength {
			t.Errorf("expected pathLength %v, got %v", aa.pathLength, bb.pathLength)
		}
	case *BranchNode:
		bb, ok := b.(*BranchNode)
		if !ok {
			t.Fatalf("expected *BranchNode, got %T", b)
		}
		if aa.embeddedChildren != bb.embeddedChildren {
			t.Errorf("expected embeddedChildren %v, got %v", aa.embeddedChildren, bb.embeddedChildren)
		}
		if aa.hashes != bb.hashes {
			t.Errorf("expected hashes %v, got %v", aa.hashes, bb.hashes)
		}
	case *AccountNode:
		bb, ok := b.(*AccountNode)
		if !ok {
			t.Errorf("expected *AccountNode, got %T", b)
		}
		if aa.address != bb.address {
			t.Errorf("expected address %v, got %v", aa.address, bb.address)
		}
		if aa.info != bb.info {
			t.Errorf("expected info %v, got %v", aa.info, bb.info)
		}
		if aa.storageHash != bb.storageHash {
			t.Errorf("expected storageHash %v, got %v", aa.storageHash, bb.storageHash)
		}
		if aa.pathLength != bb.pathLength {
			t.Errorf("expected pathLength %v, got %v", aa.pathLength, bb.pathLength)
		}
	}
}
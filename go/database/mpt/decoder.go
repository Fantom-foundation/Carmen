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
	"bytes"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/rlp"
)

// DecodeFromRlp decodes a node from RLP-encoded data.
// It checks for malformed data and returns an error if the data is not valid.
// Otherwise, it returns the decoded node.
func DecodeFromRlp(data []byte) (Node, error) {
	if bytes.Equal(data, emptyStringRlpEncoded) {
		return EmptyNode{}, nil
	}

	item, err := rlp.Decode(data)
	if err != nil {
		return nil, err
	}

	list, ok := item.(rlp.List)
	if !ok {
		return nil, fmt.Errorf("invalid node type: got: %T, wanted: List", item)
	}

	switch len(list.Items) {
	case 2:
		path, ok := list.Items[0].(rlp.String)
		if !ok {
			return nil, fmt.Errorf("invalid prefix type: got: %T, wanted: String", list.Items[0])
		}
		nibbles := compactPathToNibbles(path.Str)
		if isEncodedLeafNode(path.Str) {
			return decodeLeafNodeFromRlp(nibbles, list.Items[1])
		} else {
			return decodeExtensionNodeFromRlp(nibbles, list.Items[1])
		}
	case 17:
		return decodeBranchNodeFromRlp(list)
	}

	return nil, fmt.Errorf("invalid number of list elements: got: %v, wanted: either 2 or 17", len(list.Items))
}

// DecodeEmbeddedFromRlp decodes an embedded node from RLP-encoded data.
// It checks for malformed data and returns an error if the data is not valid.
// Otherwise, it returns the decoded embedded node.
func DecodeEmbeddedFromRlp(data []byte) (Node, error) {
	if len(data) > common.HashSize {
		return nil, fmt.Errorf("embedded node is too long: got: %v, wanted: < 32", len(data))
	}

	var n int
	var found bool
	for n = len(data) - 1; n >= 0; n-- {
		if data[n] == 0xF {
			found = true
			break
		}
		if data[n] != 0 {
			return nil, fmt.Errorf("embedded node is not padded with zeros: got: %v, wanted: 0", data[n])
		}
	}

	if !found {
		return nil, fmt.Errorf("embedded node does not have terminal marker")
	}

	return DecodeFromRlp(data[:n])
}

// decodeExtensionNodeFromRlp decodes an extension node from RLP-encoded data.
// It checks for malformed data and returns an error if the data is not valid.
// Otherwise, it returns the decoded extension node.
func decodeExtensionNodeFromRlp(path []Nibble, payload rlp.Item) (Node, error) {
	next, ok := payload.(rlp.String)
	if !ok {
		return nil, fmt.Errorf("invalid next type: got: %T, wanted: String", payload)
	}
	if len(next.Str) > common.HashSize {
		return nil, fmt.Errorf("next node hash is too long: got: %v, wanted: <= 32", len(next.Str))
	}
	var nextNode common.Hash
	var embedded bool
	if n := copy(nextNode[:], next.Str); n < common.HashSize {
		embedded = true
		nextNode[n] = 0xF // terminal marker
	}

	return &ExtensionNode{path: CreatePathFromNibbles(path), nextHash: nextNode, nextIsEmbedded: embedded}, nil
}

// decodeLeafNodeFromRlp decodes a leaf node from RLP-encoded data.
// A leaf node can be either a value node or an account node.
// The node type is distinguished by the length of the payload.
// The value node has a payload of size <= common.ValueSize,
// in other cases, it is an account node.
// Ths method checks for malformed data and returns an error if the data is not valid.
// Otherwise, it returns the decoded leaf node.
func decodeLeafNodeFromRlp(path []Nibble, payload rlp.Item) (Node, error) {
	str, ok := payload.(rlp.String)
	if !ok {
		return nil, fmt.Errorf("invalid node payload: got: %T, wanted: String", payload)
	}

	// payload matches the size of the storage slot
	if len(str.Str) <= common.ValueSize {
		return decodeValueNodeFromRlp(path, str)
	}

	return decodeAccountFromRlp(path, str)
}

// decodeValueNodeFromRlp decodes a value node from RLP-encoded data.
// The value node will be decoded with the Key equivalent the input path.
// It means that the key will not be the full storage key, as this
// information is not available in the RLP-encoded data.
// It checks for malformed data and returns an error if the data is not valid.
// Otherwise, it returns the decoded value node.
func decodeValueNodeFromRlp(path []Nibble, payload rlp.String) (Node, error) {
	if len(path) > common.KeySize {
		return nil, fmt.Errorf("invalid value path length: got: %v, wanted: <= %v", len(path), common.KeySize)
	}
	var key common.Key
	copy(key[:], string(path)) // it does not cover full key as it is not available in RLP.
	var value common.Value
	copy(value[:], payload.Str)
	return &ValueNode{key: key, value: value, pathLength: byte(len(path))}, nil
}

// decodeAccountFromRlp decodes an account node from RLP-encoded data.
// The account node will be decoded with the address equivalent the input path.
// It means that the address will not be the full address, as this
// information is not available in the RLP-encoded data.
// It checks for malformed data and returns an error if the data is not valid.
// Otherwise, it returns the decoded account node.
func decodeAccountFromRlp(path []Nibble, payload rlp.String) (Node, error) {
	if len(path) > common.AddressSize {
		return nil, fmt.Errorf("invalid account path length: got: %v, wanted: <= %v", len(path), common.AddressSize)
	}

	// may be account node
	accountPayload, err := rlp.Decode(payload.Str)
	if err != nil {
		return nil, err
	}
	items, ok := accountPayload.(rlp.List)
	if !ok {
		return nil, fmt.Errorf("invalid account payload type: got: %T, wanted: List", accountPayload)
	}

	if len(items.Items) != 4 {
		return nil, fmt.Errorf("invalid number of account items: got: %v, wanted: 4", len(items.Items))
	}

	nonceStr, ok := items.Items[0].(rlp.String)
	if !ok {
		return nil, fmt.Errorf("invalid nonce type: got: %T, wanted: String", items.Items[0])
	}
	nonce, err := nonceStr.Uint64()
	if err != nil {
		return nil, fmt.Errorf("invalid nonce: %v", err)
	}

	balanceStr, ok := items.Items[1].(rlp.String)
	if !ok {
		return nil, fmt.Errorf("invalid balance type: got: %T, wanted: String", items.Items[1])
	}
	balance := balanceStr.BigInt()
	balanceInt, err := common.ToBalance(balance)
	if err != nil {
		return nil, fmt.Errorf("invalid balance: %v", err)
	}

	var address common.Address
	copy(address[:], string(path)) // it does not cover full key as it is not available in RLP.

	storageHashStr, ok := items.Items[2].(rlp.String)
	if !ok {
		return nil, fmt.Errorf("invalid storage hash type: got: %T, wanted: String", items.Items[2])
	}
	if len(storageHashStr.Str) > common.HashSize {
		return nil, fmt.Errorf("storage hash is too long: got: %v, wanted: <= 32", len(storageHashStr.Str))
	}
	var storageHash common.Hash
	copy(storageHash[:], storageHashStr.Str)

	codeHashStr, ok := items.Items[3].(rlp.String)
	if !ok {
		return nil, fmt.Errorf("invalid code hash type: got: %T, wanted: String", items.Items[3])
	}
	if len(codeHashStr.Str) > common.HashSize {
		return nil, fmt.Errorf("code hash is too long: got: %v, wanted: <= 32", len(codeHashStr.Str))
	}

	var codeHash common.Hash
	copy(codeHash[:], codeHashStr.Str)

	return &AccountNode{
		address:     address,
		storageHash: storageHash,
		pathLength:  byte(len(path)),
		info: AccountInfo{
			Nonce:    common.ToNonce(nonce),
			Balance:  balanceInt,
			CodeHash: codeHash,
		}}, nil
}

// decodeBranchNodeFromRlp decodes a branch node from RLP-encoded data.
// It checks for malformed data and returns an error if the data is not valid.
// Otherwise, it returns the decoded branch node.
func decodeBranchNodeFromRlp(list rlp.List) (Node, error) {
	node := BranchNode{}
	for i, item := range list.Items[0:16] {
		child, ok := item.(rlp.String)
		if !ok {
			return nil, fmt.Errorf("invalid child type: got: %T, wanted: String", item)
		}
		if len(child.Str) > common.HashSize {
			return nil, fmt.Errorf("child node hash is too long: got: %v, wanted: <= 32", len(child.Str))
		}
		var hash common.Hash
		var embedded bool
		if n := copy(hash[:], child.Str); n < common.HashSize {
			embedded = true
			hash[n] = 0xF // terminal marker
		}

		node.hashes[i] = hash
		node.setEmbedded(byte(i), embedded)
	}

	return &node, nil
}

// isEncodedLeafNode checks if the path is a leaf node in the compact encoding.
// In the compact encoding, the first nibble of the path contains the oddness of the path,
// and if the node is leaf or not.
// The encoding is as follows:
// - 0b_0000_0000 (0x00): extension node, even path
// - 0b_0001_xxxx (0x1_): extension node, odd path
// - 0b_0010_0000 (0x20): leaf node, even path
// - 0b_0011_xxxx (0x3_): leaf node, odd path
// for more see:
// https://arxiv.org/pdf/2108.05513/1000 sec 4.1
func isEncodedLeafNode(path []byte) bool {
	return path[0]&0b_0010_0000>>5 == 1
}

// compactPathToNibbles converts a compact path to nibbles.
// The compact path packs two nibbles into a single byte.
// The higher nibble of first byte contains the oddness of the path and if the node is a leaf node.
// If the payload is odd, the lower nibble of the  first byte contains already payload.
// If the payload is even, the lower nibble of the first byte is padded with zero.
// The encoding is as follows:
// - 0b_0000_0000 (0x00): extension node, even path
// - 0b_0001_xxxx (0x1_): extension node, odd path
// - 0b_0010_0000 (0x20): leaf node, even path
// - 0b_0011_xxxx (0x3_): leaf node, odd path
// Examples:
//
//	[5,6,7,8,9] -> [15,67,89] extension node, or [35,67,89] leaf node
//	[4,5,6,7,8,9] -> [00,45,67,89] extension node, or [20,45,67,89] leaf node
//
// for more see:
// https://arxiv.org/pdf/2108.05513/1000 sec 4.1
func compactPathToNibbles(path []byte) []Nibble {
	odd := int(path[0] & 0b_0001_0000 >> 4) // will become either 1 or 0

	res := make([]Nibble, 0, len(path)*2)
	for _, b := range path {
		res = append(res, Nibble(b>>4), Nibble(b&0xF))
	}

	return res[2-odd:]
}

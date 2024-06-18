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
		if len(nibbles) > 64 {
			return nil, fmt.Errorf("invalid path length: got: %v, wanted: <= 64", len(nibbles))
		}
		compactPath := CreatePathFromNibbles(nibbles)
		if isEncodedLeafNode(path.Str) {
			return decodeLeafNodeFromRlp(compactPath, list.Items[1])
		} else {
			return decodeExtensionNodeFromRlp(compactPath, list.Items[1])
		}
	case 17:
		return decodeBranchNodeFromRlp(list)
	}

	return nil, fmt.Errorf("invalid number of list elements: got: %v, wanted: either 2 or 17", len(list.Items))
}

// decodeExtensionNodeFromRlp decodes an extension node from RLP-encoded data.
// It checks for malformed data and returns an error if the data is not valid.
// Otherwise, it returns the decoded extension node.
func decodeExtensionNodeFromRlp(path Path, payload rlp.Item) (Node, error) {
	hashed, embedded, err := decodeEmbeddedOrHashedNode(payload)
	if err != nil {
		return nil, err
	}
	return &ExtensionNode{path: path, nextHash: hashed, nextIsEmbedded: embedded}, nil
}

// decodeLeafNodeFromRlp decodes a leaf node from RLP-encoded data.
// A leaf node can be either a value node or an account node.
// The node type is distinguished by the length of the payload.
// The value node has a payload of size <= common.ValueSize,
// in other cases, it is an account node.
// Ths method checks for malformed data and returns an error if the data is not valid.
// Otherwise, it returns the decoded leaf node.
func decodeLeafNodeFromRlp(path Path, payload rlp.Item) (Node, error) {
	str, ok := payload.(rlp.String)
	if !ok {
		return nil, fmt.Errorf("invalid node payload: got: %T, wanted: String", payload)
	}

	innerPayload, err := rlp.Decode(str.Str)
	if err != nil {
		return nil, err
	}

	switch n := innerPayload.(type) {
	case rlp.String:
		return decodeValueNodeFromRlp(path, n)
	case rlp.List:
		return decodeAccountFromRlp(path, n)
	}

	return nil, fmt.Errorf("invalid leaf node type: got: %T, wanted: String or List", innerPayload)
}

// decodeValueNodeFromRlp decodes a value node from RLP-encoded data.
// The value node will be decoded with the Key equivalent the input path.
// It means that the key will not be the full storage key, as this
// information is not available in the RLP-encoded data.
// It checks for malformed data and returns an error if the data is not valid.
// Otherwise, it returns the decoded value node.
func decodeValueNodeFromRlp(path Path, payload rlp.String) (Node, error) {
	var key common.Key
	copy(key[:], path.GetPackedNibbles()) // it does not cover full key as it is not available in RLP.
	var value common.Value
	copy(value[32-len(payload.Str):], payload.Str) // align the value to the right
	return &ValueNode{key: key, value: value, pathLength: byte(path.Length())}, nil
}

// decodeAccountFromRlp decodes an account node from RLP-encoded data.
// The account node will be decoded with the address equivalent the input path.
// It means that the address will not be the full address, as this
// information is not available in the RLP-encoded data.
// It checks for malformed data and returns an error if the data is not valid.
// Otherwise, it returns the decoded account node.
func decodeAccountFromRlp(path Path, items rlp.List) (Node, error) {
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
	copy(address[:], path.GetPackedNibbles()) // it does not cover full key as it is not available in RLP.

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
		pathLength:  byte(path.Length()),
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
		hashed, embedded, err := decodeEmbeddedOrHashedNode(item)
		if err != nil {
			return nil, err
		}
		node.hashes[i] = hashed
		node.setEmbedded(byte(i), embedded)
	}

	return &node, nil
}

// decodeEmbeddedOrHashedNode decodes an embedded or hashed node from RLP-encoded data.
// It checks for malformed data and returns an error if the data is not valid.
// Otherwise, it returns the decoded node hash and a flag indicating if the node is embedded.
func decodeEmbeddedOrHashedNode(payload rlp.Item) (node common.Hash, embedded bool, err error) {
	var hash common.Hash
	switch item := payload.(type) {
	case rlp.String:
		if len(item.Str) > common.HashSize {
			return common.Hash{}, false, fmt.Errorf("node hash is too long: got: %v, wanted: <= 32", len(item.Str))
		}
		if len(item.Str) == 0 {
			hash = EmptyNodeEthereumHash
		} else {
			copy(hash[:], item.Str)
		}
	case rlp.List: // embedded node is a two item list of a value node.
		arr := make([]byte, 0, common.HashSize)
		if n := copy(hash[:], rlp.EncodeInto(arr, item)); n > 0 && n < common.HashSize {
			embedded = true
		} else {
			return common.Hash{}, false, fmt.Errorf("embedded node is too long: got: %v, wanted: < 32", n)
		}
	}

	return hash, embedded, nil
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

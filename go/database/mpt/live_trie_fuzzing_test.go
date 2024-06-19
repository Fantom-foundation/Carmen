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
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/fuzzing"
)

// FuzzLiveTrie_RandomAccountOps is a wrapper function for fuzzLiveTrieRandomAccountOps.
// It calls the fuzzLiveTrieRandomAccountOps function with the provided testing.F parameter.
//
// fuzzLiveTrieRandomAccountOps is a function that performs random operations on live trie accounts.
// It sets, gets, and deletes account information from the live trie and compares it with a shadow map.
// This function is intended for use in fuzz testing of the live trie.
//
// Parameters:
// - f: The testing.F parameter for the fuzzLiveTrieRandomAccountOps function.
func FuzzLiveTrie_RandomAccountOps(f *testing.F) {
	fuzzLiveTrieRandomAccountOps(f)
}

// FuzzLiveTrie_RandomAccountStorageOps is a wrapper function for fuzzLiveTrieRandomAccountStorageOps.
// It calls the fuzzLiveTrieRandomAccountStorageOps function with the provided testing.F parameter.
//
// fuzzLiveTrieRandomAccountStorageOps is a function that performs random storage operations on live trie accounts.
// It sets, gets, and deletes storage values from the live trie and compares them with a shadow map.
// This function is intended for use in fuzz testing of the live trie.
//
// Parameters:
// - f: The testing.F parameter for the fuzzLiveTrieRandomAccountStorageOps function.
func FuzzLiveTrie_RandomAccountStorageOps(f *testing.F) {
	fuzzLiveTrieRandomAccountStorageOps(f)
}

// fuzzLiveTrieRandomAccountOps is a function that performs random operations on live trie accounts.
// It sets, gets, and deletes account information from the live trie and compares it with a shadow map.
// This function is intended for use in fuzz testing of the live trie.
//
// Parameters:
// - f: The testing.F parameter for the fuzzLiveTrieRandomAccountOps function.
func fuzzLiveTrieRandomAccountOps(f *testing.F) {
	var opSet = func(_ accountOpType, value accountPayload, t fuzzing.TestingT, c *liveTrieAccountFuzzingContext) {
		if err := c.liveTrie.SetAccountInfo(value.address.GetAddress(), value.info); err != nil {
			t.Errorf("error to set account: %s", err)
		}
		c.shadow[value.address] = value.info
	}

	var opGet = func(_ accountOpType, value accountPayload, t fuzzing.TestingT, c *liveTrieAccountFuzzingContext) {
		info, _, err := c.liveTrie.GetAccountInfo(value.address.GetAddress())
		if err != nil {
			t.Errorf("cannot get account: %s", err)
		}
		shadow := c.shadow[value.address]
		if shadow != info {
			t.Errorf("accounts do not match: %v -> got: %v != want: %v", value.address, info, shadow)
		}
	}

	var opDelete = func(_ accountOpType, value accountPayload, t fuzzing.TestingT, c *liveTrieAccountFuzzingContext) {
		if err := c.liveTrie.SetAccountInfo(value.address.GetAddress(), AccountInfo{}); err != nil {
			t.Errorf("error to set account: %s", err)
		}
		c.shadow[value.address] = AccountInfo{}
	}

	serialise := func(payload accountPayload) []byte {
		return payload.Serialise()
	}
	serialiseAddrOnly := func(payload accountPayload) []byte {
		return payload.SerialiseAddress()
	}

	deserialiseAddrOnly := func(b *[]byte) accountPayload {
		var addr tinyAddress
		if len(*b) >= 1 {
			addr = tinyAddress((*b)[0])
			*b = (*b)[1:]
		}
		return accountPayload{address: addr}
	}

	deserialise := func(b *[]byte) accountPayload {
		addr := deserialiseAddrOnly(b).address
		var nonce common.Nonce
		if len(*b) >= len(nonce) {
			nonce = common.Nonce((*b)[0:len(nonce)])
			*b = (*b)[len(nonce):]
		}
		var balance [amount.BytesLength]byte
		if len(*b) >= len(balance) {
			balance = [amount.BytesLength]byte((*b)[0:len(balance)])
			*b = (*b)[len(balance):]
		}
		var codeHash common.Hash
		if len(*b) >= len(codeHash) {
			codeHash = common.Hash((*b)[0:len(codeHash)])
			*b = (*b)[len(codeHash):]
		}

		return accountPayload{addr, AccountInfo{nonce, amount.NewFromBytes(balance[:]...), codeHash}}
	}

	registry := fuzzing.NewRegistry[accountOpType, liveTrieAccountFuzzingContext]()
	fuzzing.RegisterDataOp(registry, setAccount, serialise, deserialise, opSet)
	fuzzing.RegisterDataOp(registry, getAccount, serialiseAddrOnly, deserialiseAddrOnly, opGet)
	fuzzing.RegisterDataOp(registry, deleteAccount, serialiseAddrOnly, deserialiseAddrOnly, opDelete)

	init := func(registry fuzzing.OpsFactoryRegistry[accountOpType, liveTrieAccountFuzzingContext]) []fuzzing.OperationSequence[liveTrieAccountFuzzingContext] {
		var nonce1 common.Nonce
		var nonce2 common.Nonce
		var nonce3 common.Nonce

		for i := 0; i < common.NonceSize; i++ {
			nonce2[i] = byte(i + 1)
			nonce3[i] = byte(0xFF)
		}

		var balance1 [amount.BytesLength]byte
		var balance2 [amount.BytesLength]byte
		var balance3 [amount.BytesLength]byte

		for i := 0; i < amount.BytesLength; i++ {
			balance2[i] = byte(i + 1)
			balance3[i] = byte(0xFF)
		}

		var codeHash1 common.Hash
		var codeHash2 common.Hash
		var codeHash3 common.Hash

		for i := 0; i < common.HashSize; i++ {
			codeHash2[i] = byte(i + 1)
			codeHash3[i] = byte(0xFF)
		}

		var seed []fuzzing.OperationSequence[liveTrieAccountFuzzingContext]
		{
			var sequence fuzzing.OperationSequence[liveTrieAccountFuzzingContext]
			for _, addr := range []tinyAddress{0, 1, 2, 5, 10, 255} {
				for _, nonce := range []common.Nonce{nonce1, nonce2, nonce3} {
					for _, balance := range [][amount.BytesLength]byte{balance1, balance2, balance3} {
						for _, codeHash := range []common.Hash{codeHash1, codeHash2, codeHash3} {
							info := AccountInfo{nonce, amount.NewFromBytes(balance[:]...), codeHash}
							sequence = append(sequence, registry.CreateDataOp(setAccount, accountPayload{addr, info}))
						}
					}
				}
			}
			seed = append(seed, sequence)
		}

		{
			var sequence fuzzing.OperationSequence[liveTrieAccountFuzzingContext]
			for _, addr := range []tinyAddress{0, 1, 2, 5, 10, 255} {
				info := AccountInfo{}
				sequence = append(sequence, registry.CreateDataOp(deleteAccount, accountPayload{addr, info}))
			}
			seed = append(seed, sequence)
		}

		{
			var sequence fuzzing.OperationSequence[liveTrieAccountFuzzingContext]
			for _, addr := range []tinyAddress{0, 1, 2, 5, 10, 255} {
				info := AccountInfo{}
				sequence = append(sequence, registry.CreateDataOp(getAccount, accountPayload{addr, info}))
			}
			seed = append(seed, sequence)
		}

		return seed
	}

	create := func(liveTrie *LiveTrie) *liveTrieAccountFuzzingContext {
		shadow := make([]AccountInfo, 256)
		return &liveTrieAccountFuzzingContext{liveTrie, shadow}
	}

	fuzzing.Fuzz[liveTrieAccountFuzzingContext](f, &liveTrieAccountFuzzingCampaign[accountOpType, liveTrieAccountFuzzingContext]{registry: registry, init: init, create: create})
}

// accountOpType is an operation type to be applied to an MPT.
type accountOpType byte

const (
	setAccount accountOpType = iota
	getAccount
	deleteAccount
)

// liveTrieAccountFuzzingCampaign defines each campaign.
// It defines callback methods to initialize the campaign, and to create context for each campaign loop.
type liveTrieAccountFuzzingCampaign[T ~byte, C any] struct {
	registry fuzzing.OpsFactoryRegistry[T, C]
	liveTrie *LiveTrie
	init     func(fuzzing.OpsFactoryRegistry[T, C]) []fuzzing.OperationSequence[C]
	create   func(*LiveTrie) *C
}

// liveTrieAccountFuzzingContext represents the context for fuzzing account operations on a LiveTrie.
type liveTrieAccountFuzzingContext struct {
	liveTrie *LiveTrie
	shadow   []AccountInfo // index is tinyAddress
}

// Init initializes the liveTrieAccountFuzzingCampaign by calling the init method with the registry as an argument and returning the result.
func (c *liveTrieAccountFuzzingCampaign[T, C]) Init() []fuzzing.OperationSequence[C] {
	return c.init(c.registry)
}

// CreateContext creates a new context for the liveTrieAccountFuzzingCampaign.
// It creates a temporary directory and opens a LiveTrie using that directory.
func (c *liveTrieAccountFuzzingCampaign[T, C]) CreateContext(t fuzzing.TestingT) *C {
	path := t.TempDir()
	liveTrie, err := OpenFileLiveTrie(path, S5LiveConfig, 10_000)
	if err != nil {
		t.Fatalf("failed to open live trie: %v", err)
	}
	c.liveTrie = liveTrie
	return c.create(liveTrie)
}

// Deserialize converts a byte slice of raw data into a slice of fuzzing.Operation.
// It uses the ReadAllOps method of the c.registry to deserialize the raw data.
//
// The output is a slice of fuzzing.Operation, where each operation in the slice represents
// a deserialized operation from the raw data.
func (c *liveTrieAccountFuzzingCampaign[T, C]) Deserialize(rawData []byte) []fuzzing.Operation[C] {
	return c.registry.ReadAllOps(rawData)
}

// Cleanup checks the integrity of the trie and closes the file.
func (c *liveTrieAccountFuzzingCampaign[T, C]) Cleanup(t fuzzing.TestingT, _ *C) {
	if err := c.liveTrie.Check(); err != nil {
		t.Errorf("trie verification fails: \n%s", err)
	}
	if err := c.liveTrie.Close(); err != nil {
		t.Fatalf("cannot close file: %s", err)
	}
}

// tinyAddress is a type representing a small address value.
type tinyAddress byte

// GetAddress converts the tinyAddress to the output common.Address.
// It assures all bytes of the output are filled with non-empty value,
// while the output being deterministic for all inputs.
// It does this by first getting the Keccak256 hash of the tinyAddress byte and then copying
// the resulting hash into the addr variable of type common.Address.
// Addresses are already pre-computed, i.e., calls to this method are fast.
func (a tinyAddress) GetAddress() common.Address {
	return tinyAddressLookup[a]
}

// accountPayload comprises account address and account info.
type accountPayload struct {
	address tinyAddress
	info    AccountInfo
}

// Serialise lays out the account data as: <shortAddress><nonce><balance><codeHash>
func (a *accountPayload) Serialise() []byte {
	addr := a.SerialiseAddress()
	res := make([]byte, 0, len(addr)+len(a.info.Nonce)+amount.BytesLength+len(a.info.CodeHash))
	res = append(res, addr...)
	res = append(res, a.info.Nonce[:]...)
	b := a.info.Balance.Bytes32()
	res = append(res, b[:]...)
	res = append(res, a.info.CodeHash[:]...)
	return res
}

// SerialiseAddress serializes the address of an accountPayload.
// The serialized data is laid out as <tinyAddress>.
func (a *accountPayload) SerialiseAddress() []byte {
	return []byte{byte(a.address)}
}

// storageOpType is an operation type to be applied to the storage of a contract.
type storageOpType byte

const (
	setStorage storageOpType = iota
	getStorage
	deleteStorage
	deleteStorageAccount
)

// fuzzLiveTrieRandomAccountStorageOps is a function that performs random operations on live trie storage.
// It sets, gets, and deletes storage slots from the live trie and compares it with a shadow map.
// This function is intended for use in fuzz testing of the live trie.
//
// Parameters:
// - f: The testing.F parameter for the fuzzLiveTrieRandomAccountOps function.
func fuzzLiveTrieRandomAccountStorageOps(f *testing.F) {
	accountInfo := AccountInfo{Balance: amount.New(9)}
	var createAccountIfNotExists = func(value storagePayload, t fuzzing.TestingT, c *liveTrieStorageFuzzingContext) {
		if account := c.shadow[value.address]; account == nil {
			if err := c.liveTrie.SetAccountInfo(value.address.GetAddress(), accountInfo); err != nil {
				t.Errorf("cannot create account: %s", err)
			}
			c.shadow[value.address] = make([]common.Value, 256)
		}
	}
	var opSet = func(_ storageOpType, value storagePayload, t fuzzing.TestingT, c *liveTrieStorageFuzzingContext) {
		createAccountIfNotExists(value, t, c)
		if err := c.liveTrie.SetValue(value.address.GetAddress(), value.key.GetKey(), value.value); err != nil {
			t.Errorf("error to set value: %s", err)
		}
		// assign a new value, it can be also empty
		c.shadow[value.address][value.key] = value.value
	}

	var opGet = func(_ storageOpType, value storagePayload, t fuzzing.TestingT, c *liveTrieStorageFuzzingContext) {
		slotValue, err := c.liveTrie.GetValue(value.address.GetAddress(), value.key.GetKey())
		if err != nil {
			t.Errorf("cannot get value: %s", err)
		}
		shadow := c.shadow[value.address]
		var empty common.Value
		if shadow == nil {
			if slotValue != empty {
				t.Errorf("value for non existing account is not empty: %v-> %v != %v", value.address, slotValue, empty)
			}
			return
		}

		shadowVal := shadow[value.key]
		if shadowVal != slotValue {
			t.Errorf("values do not match: %v -> got: %v != want: %v", value.address, shadowVal, slotValue)
		}
	}

	var opDelete = func(_ storageOpType, value storagePayload, t fuzzing.TestingT, c *liveTrieStorageFuzzingContext) {
		var empty common.Value
		if err := c.liveTrie.SetValue(value.address.GetAddress(), value.key.GetKey(), empty); err != nil {
			t.Errorf("error to clear value: %s", err)
		}
		if account := c.shadow[value.address]; account != nil {
			c.shadow[value.address][value.key] = common.Value{}
		}
	}

	var opDeleteAccount = func(_ storageOpType, value storagePayload, t fuzzing.TestingT, c *liveTrieStorageFuzzingContext) {
		if err := c.liveTrie.SetAccountInfo(value.address.GetAddress(), AccountInfo{}); err != nil {
			t.Errorf("error to set account: %s", err)
		}
		c.shadow[value.address] = nil
	}

	serialise := func(payload storagePayload) []byte {
		return payload.Serialise()
	}
	serialiseAddress := func(payload storagePayload) []byte {
		return payload.SerialiseAddressKey()
	}

	deserialiseAddress := func(b *[]byte) storagePayload {
		var addr tinyAddress
		var key tinyKey
		if len(*b) >= 1 {
			addr = tinyAddress((*b)[0])
			*b = (*b)[1:]
		}
		if len(*b) >= 1 {
			key = tinyKey((*b)[0])
			*b = (*b)[1:]
		}

		return storagePayload{address: addr, key: key}
	}

	deserialise := func(b *[]byte) storagePayload {
		p := deserialiseAddress(b)
		var value common.Value
		if len(*b) >= len(value) {
			value = common.Value((*b)[0:len(value)])
			*b = (*b)[len(value):]
		}

		return storagePayload{p.address, p.key, value}
	}

	registry := fuzzing.NewRegistry[storageOpType, liveTrieStorageFuzzingContext]()
	fuzzing.RegisterDataOp(registry, setStorage, serialise, deserialise, opSet)
	fuzzing.RegisterDataOp(registry, getStorage, serialiseAddress, deserialiseAddress, opGet)
	fuzzing.RegisterDataOp(registry, deleteStorage, serialiseAddress, deserialiseAddress, opDelete)
	fuzzing.RegisterDataOp(registry, deleteStorageAccount, serialiseAddress, deserialiseAddress, opDeleteAccount)

	init := func(registry fuzzing.OpsFactoryRegistry[storageOpType, liveTrieStorageFuzzingContext]) []fuzzing.OperationSequence[liveTrieStorageFuzzingContext] {
		var val1 common.Value
		var val2 common.Value
		var val3 common.Value

		for i := 0; i < common.ValueSize; i++ {
			val2[i] = byte(i + 1)
			val3[i] = byte(0xFF)
		}

		var seed []fuzzing.OperationSequence[liveTrieStorageFuzzingContext]
		{
			var sequence fuzzing.OperationSequence[liveTrieStorageFuzzingContext]
			for _, addr := range []tinyAddress{0, 1, 2, 5, 10, 255} {
				for _, key := range []tinyKey{0, 1, 2, 5, 10, 255} {
					for _, value := range []common.Value{val1, val2, val3} {
						sequence = append(sequence, registry.CreateDataOp(setStorage, storagePayload{addr, key, value}))
					}
				}
			}
			seed = append(seed, sequence)
		}

		{
			var sequence fuzzing.OperationSequence[liveTrieStorageFuzzingContext]
			var emptyValue common.Value
			for _, addr := range []tinyAddress{0, 1, 2, 5, 10, 255} {
				for _, key := range []tinyKey{0, 1, 2, 5, 10, 255} {
					sequence = append(sequence, registry.CreateDataOp(getStorage, storagePayload{addr, key, emptyValue}))
				}
			}
			seed = append(seed, sequence)
		}

		{
			var sequence fuzzing.OperationSequence[liveTrieStorageFuzzingContext]
			var emptyValue common.Value
			for _, addr := range []tinyAddress{0, 1, 2, 5, 10, 255} {
				for _, key := range []tinyKey{0, 1, 2, 5, 10, 255} {
					sequence = append(sequence, registry.CreateDataOp(deleteStorage, storagePayload{addr, key, emptyValue}))
				}
			}
			seed = append(seed, sequence)
		}

		{
			var sequence fuzzing.OperationSequence[liveTrieStorageFuzzingContext]
			var emptyValue common.Value
			for _, addr := range []tinyAddress{0, 1, 2, 5, 10, 255} {
				sequence = append(sequence, registry.CreateDataOp(deleteStorageAccount, storagePayload{addr, 0, emptyValue}))
			}
			seed = append(seed, sequence)
		}

		return seed
	}

	create := func(liveTrie *LiveTrie) *liveTrieStorageFuzzingContext {
		shadow := make([][]common.Value, 256)
		return &liveTrieStorageFuzzingContext{liveTrie, shadow}
	}

	fuzzing.Fuzz[liveTrieStorageFuzzingContext](f, &liveTrieAccountFuzzingCampaign[storageOpType, liveTrieStorageFuzzingContext]{registry: registry, init: init, create: create})
}

// liveTrieStorageFuzzingContext is a context for fuzzing storage operations on a LiveTrie data structure.
// It contains a reference to the LiveTrie object and a shadow map
// that stores the expected values for each storage address.
type liveTrieStorageFuzzingContext struct {
	liveTrie *LiveTrie
	shadow   [][]common.Value // indexes are tinyAddress -> tinyKey -> common.Value
}

// tinyKey is the storage address shrunk to 1-bytes to limit the address space.
type tinyKey byte

// GetKey converts the tinyKey to the output common.Key.
// It assures all bytes of the output are filled with non-empty value,
// while the output is deterministic for all inputs.
// It does this by first getting the Keccak256 hash of the tinyKey byte and then copying
// the resulting hash into the addr variable of type common.Key.
// Keys are already pre-computed, i.e., calls to this method are fast.
func (a tinyKey) GetKey() common.Key {
	return tinyKeyLookup[a]
}

// storagePayload comprises a fraction of an account address, a short key and a value
type storagePayload struct {
	address tinyAddress
	key     tinyKey
	value   common.Value
}

// Serialise lays out the account data as: <shortAddress><shortKey><value>
func (a *storagePayload) Serialise() []byte {
	addressKey := a.SerialiseAddressKey()
	res := make([]byte, 0, len(addressKey)+len(a.value))
	res = append(res, addressKey...)
	res = append(res, a.value[:]...)
	return res
}

// SerialiseAddressKey serializes the storagePayload address and key into a byte array.
// It creates a byte array with the capacity of 1 (tinyAddress) +1 (tinyKey), then appends the byte representation of
// a.address and the elements of a.key to the array. The resulting byte array is returned.
func (a *storagePayload) SerialiseAddressKey() []byte {
	res := make([]byte, 0, 1+1)
	res = append(res, byte(a.address))
	res = append(res, byte(a.key))
	return res
}

// tinyAddressLookup is an array where the index is a tinyAddress pointing to the full common.Address.
var tinyAddressLookup []common.Address

// tinyKeyLookup is an array where the index is a tinyKey pointing to the full common.Key.
var tinyKeyLookup []common.Key

func init() {
	tinyAddressLookup = make([]common.Address, 256)
	tinyKeyLookup = make([]common.Key, 256)

	for i := 0; i < 256; i++ {
		{
			var addr common.Address
			hash := common.GetKeccak256Hash([]byte{byte(i)})
			copy(addr[:], hash[:])
			tinyAddressLookup[i] = addr
		}

		{
			var key common.Key
			hash := common.GetKeccak256Hash([]byte{byte(i)})
			copy(key[:], hash[:])
			tinyKeyLookup[i] = key
		}
	}
}

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
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"testing"

	_ "embed"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// The reference hashes in this file have been generated using Geth's MPT.

func TestS5RootHash_EmptyTrie(t *testing.T) {
	t.Parallel()
	state, err := OpenGoMemoryState(t.TempDir(), S5LiveConfig, TrieConfig{CacheCapacity: 1024})
	if err != nil {
		t.Fatalf("failed to open empty state: %v", err)
	}
	hash, err := state.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash for empty state: %v", err)
	}
	want := "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
	if got := fmt.Sprintf("%x", hash); want != got {
		t.Errorf("invalid hash\nexpected %v\n     got %v", want, got)
	}
}

func TestS5RootHash_SingleAccount(t *testing.T) {
	t.Parallel()
	state, err := OpenGoMemoryState(t.TempDir(), S5LiveConfig, TrieConfig{CacheCapacity: 1024})
	if err != nil {
		t.Fatalf("failed to open empty state: %v", err)
	}
	balance, _ := common.ToBalance(big.NewInt(12))
	state.SetNonce(common.Address{1}, common.ToNonce(10))
	state.SetBalance(common.Address{1}, balance)
	hash, err := state.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash for empty state: %v", err)
	}

	want := "b2a1a4be2813ecd7b3e312d080e0f94b00a3247d361ddde75d926411660e042e"
	if got := fmt.Sprintf("%x", hash); want != got {
		t.Errorf("invalid hash\nexpected %v\n     got %v", want, got)
	}
}

func TestS5RootHash_SingleAccountWithSingleValue(t *testing.T) {
	t.Parallel()
	state, err := OpenGoMemoryState(t.TempDir(), S5LiveConfig, TrieConfig{CacheCapacity: 1024})
	if err != nil {
		t.Fatalf("failed to open empty state: %v", err)
	}
	balance, _ := common.ToBalance(big.NewInt(12))
	state.SetNonce(common.Address{1}, common.ToNonce(10))
	state.SetBalance(common.Address{1}, balance)
	state.SetStorage(common.Address{1}, common.Key{1}, common.Value{2})
	hash, err := state.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash for empty state: %v", err)
	}

	want := "a175fd37774a9f29ce92f6ded173ed65340434c22af8d480a688f0dfd3980446"
	if got := fmt.Sprintf("%x", hash); want != got {
		t.Errorf("invalid hash\nexpected %v\n     got %v", want, got)
	}
}

func TestS5RootHash_TwoAccounts(t *testing.T) {
	t.Parallel()
	state, err := OpenGoMemoryState(t.TempDir(), S5LiveConfig, TrieConfig{CacheCapacity: 1024})
	if err != nil {
		t.Fatalf("failed to open empty state: %v", err)
	}
	balance, _ := common.ToBalance(big.NewInt(12))
	state.SetNonce(common.Address{1}, common.ToNonce(10))
	state.SetBalance(common.Address{2}, balance)
	hash, err := state.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash for empty state: %v", err)
	}

	want := "35cbb888517267cce1de8b870042f3777ecabf1b6f37ff9d9a68c1d2b74178c6"
	if got := fmt.Sprintf("%x", hash); want != got {
		t.Errorf("invalid hash\nexpected %v\n     got %v", want, got)
	}
}

func TestS5RootHash_TwoAccountsWithValues(t *testing.T) {
	t.Parallel()
	state, err := OpenGoMemoryState(t.TempDir(), S5LiveConfig, TrieConfig{CacheCapacity: 1024})
	if err != nil {
		t.Fatalf("failed to open empty state: %v", err)
	}
	balance, _ := common.ToBalance(big.NewInt(12))
	state.SetNonce(common.Address{1}, common.ToNonce(10))
	state.trie.SetValue(common.Address{1}, common.Key{1}, common.Value{0, 0, 1})
	state.trie.SetValue(common.Address{1}, common.Key{2}, common.Value{2})

	state.SetBalance(common.Address{2}, balance)
	state.trie.SetValue(common.Address{2}, common.Key{1}, common.Value{0, 0, 1})
	state.trie.SetValue(common.Address{2}, common.Key{2}, common.Value{2})
	hash, err := state.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash for empty state: %v", err)
	}

	want := "78a69b87179abb4bd16a4a4a565330ef531d8c3bb7258565d0cea2393e2c8adb"
	if got := fmt.Sprintf("%x", hash); want != got {
		t.Errorf("invalid hash\nexpected %v\n     got %v", want, got)
	}
}

func TestS5RootHash_TwoAccountsWithExtensionNodeWithEvenLength(t *testing.T) {
	t.Parallel()
	// These two addresses have the same first byte when hashed:
	addr1 := common.Address{0x04}
	addr2 := common.Address{0x2F}
	if a, b := common.Keccak256(addr1[:])[0], common.Keccak256(addr2[:])[0]; a != b {
		t.Fatalf("invalid setup, addresses do not have common prefix")
	}

	state, err := OpenGoMemoryState(t.TempDir(), S5LiveConfig, TrieConfig{CacheCapacity: 1024})
	if err != nil {
		t.Fatalf("failed to open empty state: %v", err)
	}
	balance, _ := common.ToBalance(big.NewInt(12))
	state.SetNonce(addr1, common.ToNonce(10))
	state.SetBalance(addr2, balance)
	hash, err := state.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash for empty state: %v", err)
	}

	want := "1fbd95cd060ea80f90255236f46f2d1ec829a13124befababc35222f10eb1af4"
	if got := fmt.Sprintf("%x", hash); want != got {
		t.Errorf("invalid hash\nexpected %v\n     got %v", want, got)
	}
}

func TestS5RootHash_TwoAccountsWithExtensionNodeWithOddLength(t *testing.T) {
	t.Parallel()
	// These two addresses have the same first nibble when hashed:
	addr1 := common.Address{0x02}
	addr2 := common.Address{0x07}
	if a, b := common.Keccak256(addr1[:])[0], common.Keccak256(addr2[:])[0]; (a>>4) != (b>>4) && a == b {
		t.Fatalf("invalid setup, addresses do not have single prefix bit prefix")
	}

	state, err := OpenGoMemoryState(t.TempDir(), S5LiveConfig, TrieConfig{CacheCapacity: 1024})
	if err != nil {
		t.Fatalf("failed to open empty state: %v", err)
	}
	balance, _ := common.ToBalance(big.NewInt(12))
	state.SetNonce(addr1, common.ToNonce(10))
	state.SetBalance(addr2, balance)
	hash, err := state.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash for empty state: %v", err)
	}

	want := "3edbb6b46662d53fe8739d1b6c5b517dd93fc07d4f90f8fbfb3bf7bdc9b07696"
	if got := fmt.Sprintf("%x", hash); want != got {
		t.Errorf("invalid hash\nexpected %v\n     got %v", want, got)
	}
}

//go:embed root_hash_test_address_and_keys.txt
var hashesForAddressAndKeys string

func TestS5RootHash_AddressAndKeys(t *testing.T) {
	t.Parallel()
	// Parse the reference hashes.
	hashes := strings.Split(hashesForAddressAndKeys, "\n")
	nextHash := func() string {
		res := hashes[0]
		hashes = hashes[1:]
		return res
	}

	const N = 100

	trie, err := OpenGoMemoryState(t.TempDir(), S5LiveConfig, TrieConfig{CacheCapacity: 1024})
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	address := getTestAddresses(N)
	keys := getTestKeys(N)

	// Fill the tree.
	for i, addr := range address {

		if err := trie.SetNonce(addr, common.ToNonce(uint64(i)+1)); err != nil {
			t.Fatalf("failed to insert account: %v", err)
		}

		for i, key := range keys {
			if err := trie.SetStorage(addr, key, common.Value{byte(i)}); err != nil {
				t.Fatalf("failed to insert value: %v", err)
			}

			hash, err := trie.GetHash()
			if err != nil {
				t.Fatalf("failed to compute hash: %v", err)
			}

			want := nextHash()
			if got := fmt.Sprintf("%x", hash); want != got {
				t.Fatalf("invalid hash after insert #%d\nexpected %v\n     got %v", i, want, got)
			}
		}
	}

	// Delete all accounts.
	for i, addr := range address {
		if err := trie.DeleteAccount(addr); err != nil {
			t.Fatalf("failed to delete account: %v", err)
		}

		hash, err := trie.GetHash()
		if err != nil {
			t.Fatalf("failed to compute hash: %v", err)
		}

		want := nextHash()
		if got := fmt.Sprintf("%x", hash); want != got {
			t.Fatalf("invalid hash after delete #%d\nexpected %v\n     got %v", i, want, got)
		}
	}
}

//go:embed root_hash_test_values.txt
var hashesForValues string

func TestS5RootHash_Values(t *testing.T) {
	t.Parallel()
	const N = 1000

	// Parse the reference hashes.
	hashes := strings.Split(hashesForValues, "\n")
	nextHash := func() string {
		res := hashes[0]
		hashes = hashes[1:]
		return res
	}

	trie, err := OpenGoMemoryState(t.TempDir(), S5LiveConfig, TrieConfig{CacheCapacity: 1024})
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	trgAddr := common.Address{}
	keys := getTestKeys(N)

	// Create a single account.
	if err := trie.SetNonce(trgAddr, common.ToNonce(1)); err != nil {
		t.Fatalf("failed to insert account: %v", err)
	}

	// Insert keys one-by-one.
	for i, key := range keys {
		if err := trie.SetStorage(trgAddr, key, common.Value{byte(i)}); err != nil {
			t.Fatalf("failed to insert value: %v", err)
		}

		hash, err := trie.GetHash()
		if err != nil {
			t.Fatalf("failed to compute hash: %v", err)
		}

		want := nextHash()
		if got := fmt.Sprintf("%x", hash); want != got {
			t.Fatalf("invalid hash after insert #%d\nexpected %v\n     got %v", i, want, got)
		}
	}

	// Delete all values.
	for i, key := range keys {
		if err := trie.SetStorage(trgAddr, key, common.Value{}); err != nil {
			t.Fatalf("failed to insert value: %v", err)
		}

		hash, err := trie.GetHash()
		if err != nil {
			t.Fatalf("failed to compute hash: %v", err)
		}

		want := nextHash()
		if got := fmt.Sprintf("%x", hash); want != got {
			t.Fatalf("invalid hash after delete #%d\nexpected %v\n     got %v", i, want, got)
		}
	}
}

func TestHashing_S5EmbeddedValuesAreHandledCorrectly(t *testing.T) {
	// This test case covers a bug identified as the cause for issue #769.
	// In MPT modes where hashes are stored with the nodes, the info
	// whether nodes are embedded or not is also not stored with the parent.
	// If a node is loaded, all embedded information needs to be considered
	// outdated, and to be recomputed, even if the child node itself is not
	// modified.

	// These keys have a common hash prefix of 4 bytes. Thus, there are only 28 bytes
	// to be encoded in value nodes if they are stored in a branch node, causing the
	// values to be small enough to be embedded.
	key1 := hexToKey("c76547ce3912f8c25a9943819c2992169865dfd500bed5213c8a92ceff5db5e3")
	key2 := hexToKey("2968f9295ca3ab4960ae553a18f47567e56f2777ad762ee1d639421728926a37")

	hash1 := common.Keccak256ForKey(key1)
	hash2 := common.Keccak256ForKey(key2)
	if !bytes.Equal(hash1[:4], hash2[:4]) {
		t.Fatalf("keys do not have a common hash-prefix")
	}

	val1 := common.Value{}
	val1[31] = 1
	val2 := common.Value{}
	val2[31] = 2

	// The issue was only found in Archive mode, since in the live mode embedded
	// node information is stored with the parents. However, for cross-validation,
	// both modes are tested.
	for _, config := range []MptConfig{S5LiveConfig, S5ArchiveConfig} {

		// -- Setup --
		// To prepare the stage for the issue, a branch node with two embedded
		// values is created.
		directory := t.TempDir()
		state, err := OpenGoMemoryState(directory, config, TrieConfig{CacheCapacity: 1024})
		if err != nil {
			t.Fatalf("failed to open trie: %v", err)
		}

		addr := common.Address{}
		state.SetNonce(addr, common.Nonce{1})
		state.SetStorage(addr, key1, val1)
		state.SetStorage(addr, key2, val1)

		hash, _, err := state.UpdateHashes()
		if err != nil {
			t.Fatalf("failed to update hashes")
		}

		// This hash certifies that both values are correctly embedded.
		want := "b58a6f08d2494a88aba8e766cce73e877e6bbd9bccd50732bf298d3969c9892b"
		if got := fmt.Sprintf("%x", hash[:]); want != got {
			t.Errorf("invalid hash, wanted %v, got %v", want, got)
		}

		// We close and re-open the state to clear caches and to force all nodes
		// to be re-loaded from disk.
		if err := state.Close(); err != nil {
			t.Fatalf("failed to close state: %v", err)
		}

		// --- Error Case Recreation ---
		// The error causing issue #769 is caused by loading nodes from disk not
		// including embedded information and not correctly recomputing those when
		// computing hashes.
		state, err = OpenGoMemoryState(directory, config, TrieConfig{CacheCapacity: 1024})
		if err != nil {
			t.Fatalf("failed to open trie: %v", err)
		}

		// Only one value is updated, the other remains untouched. The bug was
		// that the untouched value was incorrectly considered non-embedded as a
		// side-effect.
		state.SetStorage(addr, key1, val2)

		hash, _, err = state.UpdateHashes()
		if err != nil {
			t.Fatalf("failed to update hashes")
		}

		// This hash certifies that both values are correctly embedded.
		want = "19b93a208ed79c5eba88eb748a19f7506dd09423fc66ee41d40fc0b8e8e56aca"
		if got := fmt.Sprintf("%x", hash[:]); want != got {
			t.Errorf("invalid hash, wanted %v, got %v", want, got)
		}

		state.trie.Dump()

		if err := state.Close(); err != nil {
			t.Fatalf("failed to close state: %v", err)
		}
	}
}

func hexToKey(s string) common.Key {
	var key common.Key
	hex.Decode(key[:], []byte(s))
	return key
}

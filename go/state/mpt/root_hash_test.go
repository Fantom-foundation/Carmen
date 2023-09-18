package mpt

import (
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
	state, err := OpenGoMemoryState(t.TempDir(), S5Config)
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
	state, err := OpenGoMemoryState(t.TempDir(), S5Config)
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
	state, err := OpenGoMemoryState(t.TempDir(), S5Config)
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
	state, err := OpenGoMemoryState(t.TempDir(), S5Config)
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
	state, err := OpenGoMemoryState(t.TempDir(), S5Config)
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
	if a, b := keccak256(addr1[:])[0], keccak256(addr2[:])[0]; a != b {
		t.Fatalf("invalid setup, addresses do not have common prefix")
	}

	state, err := OpenGoMemoryState(t.TempDir(), S5Config)
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
	if a, b := keccak256(addr1[:])[0], keccak256(addr2[:])[0]; (a>>4) != (b>>4) && a == b {
		t.Fatalf("invalid setup, addresses do not have single prefix bit prefix")
	}

	state, err := OpenGoMemoryState(t.TempDir(), S5Config)
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

	trie, err := OpenGoMemoryState(t.TempDir(), S5Config)
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

	trie, err := OpenGoMemoryState(t.TempDir(), S5Config)
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

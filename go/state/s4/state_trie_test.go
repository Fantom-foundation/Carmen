package s4

import (
	"math/rand"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestStateTrie_EmptyTrieIsConsistent(t *testing.T) {
	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	if err := trie.Check(); err != nil {
		t.Fatalf("empty try has consistency problems: %v", err)
	}
}

func TestStateTrie_NonExistingAccountsHaveEmptyInfo(t *testing.T) {
	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	if err := trie.Check(); err != nil {
		t.Fatalf("empty try has consistency problems: %v", err)
	}

	addr1 := common.Address{1}
	if info, exists, err := trie.GetAccountInfo(addr1); err != nil || exists || info != (AccountInfo{}) {
		t.Errorf("failed to get default account infor from empty state, got %v, exists %v, err: %v", info, exists, err)
	}
}

func TestStateTrie_SetAndGetSingleAccountInformationWorks(t *testing.T) {
	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	if err := trie.Check(); err != nil {
		t.Fatalf("empty try has consistency problems: %v", err)
	}

	addr := common.Address{1}
	info := AccountInfo{
		Nonce:    common.Nonce{1},
		Balance:  common.Balance{2},
		CodeHash: common.Hash{3},
	}

	if err := trie.SetAccountInfo(addr, info); err != nil {
		t.Errorf("failed to set info of account: %v", err)
	}

	if err := trie.Check(); err != nil {
		trie.Dump()
		t.Errorf("trie corrupted after insert: %v", err)
	}

	if recovered, exists, err := trie.GetAccountInfo(addr); err != nil || !exists || recovered != info {
		t.Errorf("failed to recover account information, wanted %v, got %v, exists %v, err %v", info, recovered, exists, err)
	}

	if err := trie.Check(); err != nil {
		trie.Dump()
		t.Errorf("trie corrupted after read: %v", err)
	}
}

func TestStateTrie_SetAndGetMultipleAccountInformationWorks(t *testing.T) {
	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	if err := trie.Check(); err != nil {
		t.Fatalf("empty try has consistency problems: %v", err)
	}

	addr1 := common.Address{1}
	addr2 := common.Address{2}
	addr3 := common.Address{0, 0, 0, 0, 0, 0, 3}

	if err := trie.SetAccountInfo(addr1, AccountInfo{Nonce: common.Nonce{1}}); err != nil {
		t.Errorf("failed to set info of account: %v", err)
	}
	if err := trie.Check(); err != nil {
		trie.Dump()
		t.Errorf("trie corrupted after insert: %v", err)
	}

	if err := trie.SetAccountInfo(addr2, AccountInfo{Nonce: common.Nonce{2}}); err != nil {
		t.Errorf("failed to set info of account: %v", err)
	}
	if err := trie.Check(); err != nil {
		trie.Dump()
		t.Errorf("trie corrupted after insert: %v", err)
	}

	if err := trie.SetAccountInfo(addr3, AccountInfo{Nonce: common.Nonce{3}}); err != nil {
		t.Errorf("failed to set info of account: %v", err)
	}
	if err := trie.Check(); err != nil {
		trie.Dump()
		t.Errorf("trie corrupted after insert: %v", err)
	}
}

func TestStateTrie_NonExistingValueHasZeroValue(t *testing.T) {
	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	addr := common.Address{1}
	key := common.Key{1}

	// If the account does not exist, the result should be empty.
	if value, err := trie.GetValue(addr, key); value != (common.Value{}) || err != nil {
		t.Errorf("expected value of non-existing account to be empty, got %v, err: %v", value, err)
	}

	// Also, if the account exists, the result should be empty.
	if err := trie.SetAccountInfo(addr, AccountInfo{Nonce: common.Nonce{1}}); err != nil {
		t.Fatalf("failed to create an account")
	}
	if value, err := trie.GetValue(addr, key); value != (common.Value{}) || err != nil {
		t.Errorf("expected value of uninitialized slot to be empty, got %v, err: %v", value, err)
	}
}

func TestStateTrie_ValuesCanBeSetAndRetrieved(t *testing.T) {
	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	addr := common.Address{1}
	key := common.Key{1}
	value := common.Value{1}

	// If the account does not exist, the write has no effect.
	if err := trie.SetValue(addr, key, value); err != nil {
		t.Errorf("writing to non-existing account failed: %v", err)
	}
	if got, err := trie.GetValue(addr, key); got != (common.Value{}) || err != nil {
		t.Errorf("wanted %v, got %v", common.Value{}, got)
	}

	// Create the account.
	if err := trie.SetAccountInfo(addr, AccountInfo{Nonce: common.Nonce{1}}); err != nil {
		t.Fatalf("failed to create account for test: %v", err)
	}
	if err := trie.SetValue(addr, key, value); err != nil {
		t.Errorf("writing to existing account failed: %v", err)
	}
	if got, err := trie.GetValue(addr, key); value != got || err != nil {
		t.Errorf("wanted %v, got %v", value, got)
	}
}

func TestStateTrie_SameContentProducesSameHash(t *testing.T) {
	trie1, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	trie2, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}

	hash1, err := trie1.GetHash()
	if err != nil {
		t.Errorf("failed to fetch hash of empty trie: %v", err)
	}
	hash2, err := trie2.GetHash()
	if err != nil {
		t.Errorf("failed to fetch hash of empty trie: %v", err)
	}
	if hash1 != hash2 {
		t.Errorf("Expected empty tries to have same hash, got %v and %v", hash1, hash2)
	}

	info1 := AccountInfo{Nonce: common.ToNonce(1)}
	info2 := AccountInfo{Nonce: common.ToNonce(2)}
	trie1.SetAccountInfo(common.Address{1}, info1)
	trie2.SetAccountInfo(common.Address{2}, info2)

	hash1, err = trie1.GetHash()
	if err != nil {
		t.Errorf("failed to fetch hash of non-empty trie: %v", err)
	}
	hash2, err = trie2.GetHash()
	if err != nil {
		t.Errorf("failed to fetch hash of non-empty trie: %v", err)
	}
	if hash1 == hash2 {
		t.Errorf("Expected different tries to have different hashes, got %v and %v", hash1, hash2)
	}

	// Update tries to contain same data.
	trie1.SetAccountInfo(common.Address{2}, info2)
	trie2.SetAccountInfo(common.Address{1}, info1)

	hash1, err = trie1.GetHash()
	if err != nil {
		t.Errorf("failed to fetch hash of non-empty trie: %v", err)
	}
	hash2, err = trie2.GetHash()
	if err != nil {
		t.Errorf("failed to fetch hash of non-empty trie: %v", err)
	}
	if hash1 != hash2 {
		t.Errorf("Expected equal tries to have same hashes, got %v and %v", hash1, hash2)
	}
}

func TestStateTrie_ChangeInTrieSubstructureUpdatesHash(t *testing.T) {
	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}

	info1 := AccountInfo{Nonce: common.ToNonce(1)}
	info2 := AccountInfo{Nonce: common.ToNonce(2)}
	trie.SetAccountInfo(common.Address{1}, info1)
	trie.SetAccountInfo(common.Address{2}, info2)

	hash1, err := trie.GetHash()
	if err != nil {
		t.Errorf("failed to fetch hash of empty trie: %v", err)
	}

	// The next update does not change anything in the root node, but the hash should
	// still be updated.
	trie.SetAccountInfo(common.Address{1}, info2)

	hash2, err := trie.GetHash()
	if err != nil {
		t.Errorf("failed to fetch hash of empty trie: %v", err)
	}
	if hash1 == hash2 {
		t.Errorf("Nested modification should have caused a change in hashes, got %v and %v", hash1, hash2)
	}
}

func TestStateTrie_InsertLotsOfData(t *testing.T) {
	t.Parallel()
	const N = 100

	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	address := getTestAddresses(N)
	keys := getTestKeys(N)

	// Fill the tree.
	for i, addr := range address {
		if err := trie.SetAccountInfo(addr, AccountInfo{Nonce: common.ToNonce(uint64(i) + 1)}); err != nil {
			t.Fatalf("failed to insert account: %v", err)
		}
		if err := trie.Check(); err != nil {
			trie.Dump()
			t.Fatalf("trie inconsistent after account insert:\n%v", err)
		}

		for i, key := range keys {
			if err := trie.SetValue(addr, key, common.Value{byte(i)}); err != nil {
				t.Fatalf("failed to insert value: %v", err)
			}
			if err := trie.Check(); err != nil {
				trie.Dump()
				t.Fatalf("trie inconsistent after value insert:\n%v", err)
			}
		}
	}

	// Check its content.
	for i, addr := range address {
		if info, _, err := trie.GetAccountInfo(addr); int(info.Nonce.ToUint64()) != i+1 || err != nil {
			t.Fatalf("wrong value, wanted %v, got %v, err %v", i+1, int(info.Nonce.ToUint64()), err)
		}
		for i, key := range keys {
			if value, err := trie.GetValue(addr, key); value[0] != byte(i) || err != nil {
				t.Fatalf("wrong value, wanted %v, got %v, err %v", byte(i), value[0], err)
			}
		}
	}

	// Delete all accounts.
	for _, addr := range address {
		if err := trie.SetAccountInfo(addr, AccountInfo{}); err != nil {
			t.Fatalf("failed to delete account: %v", err)
		}
		if err := trie.Check(); err != nil {
			trie.Dump()
			t.Fatalf("trie inconsistent after account deletion:\n%v\nDeleted account: %v", err, addr)
		}
	}
}

func TestStateTrie_InsertLotsOfValues(t *testing.T) {
	t.Parallel()
	const N = 10000

	trie, err := OpenInMemoryTrie(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()

	addr := common.Address{}
	keys := getTestKeys(N)

	// Fill a single account.
	if err := trie.SetAccountInfo(addr, AccountInfo{Nonce: common.ToNonce(1)}); err != nil {
		t.Fatalf("failed to insert account: %v", err)
	}
	if err := trie.Check(); err != nil {
		trie.Dump()
		t.Fatalf("trie inconsistent after account insert:\n%v", err)
	}

	for i, key := range keys {
		if err := trie.SetValue(addr, key, common.Value{byte(i)}); err != nil {
			t.Fatalf("failed to insert value: %v", err)
		}
		if err := trie.Check(); err != nil {
			trie.Dump()
			t.Fatalf("trie inconsistent after value insert:\n%v", err)
		}
	}

	// Check its content.
	for i, key := range keys {
		if value, err := trie.GetValue(addr, key); value[0] != byte(i) || err != nil {
			t.Fatalf("wrong value, wanted %v, got %v, err %v", byte(i), value[0], err)
		}
	}

	// Delete all values.
	for _, key := range keys {
		if err := trie.SetValue(addr, key, common.Value{}); err != nil {
			t.Fatalf("failed to delete value: %v", err)
		}
		if err := trie.Check(); err != nil {
			trie.Dump()
			t.Fatalf("trie inconsistent after value deletion:\n%v\nDeleted value: %v", err, key)
		}
	}
}

func getTestAddresses(number int) []common.Address {
	res := make([]common.Address, number)
	for i := range res {
		j := i * i
		res[i][0] = byte(j)
		res[i][1] = byte(j >> 8)
		res[i][2] = byte(j >> 16)
		res[i][3] = byte(j >> 24)
	}
	rand.Seed(0)
	rand.Shuffle(len(res), func(i, j int) {
		res[i], res[j] = res[j], res[i]
	})
	return res
}

func getTestKeys(number int) []common.Key {
	res := make([]common.Key, number)
	for i := range res {
		j := i * i
		res[i][0] = byte(j)
		res[i][1] = byte(j >> 8)
		res[i][2] = byte(j >> 16)
		res[i][3] = byte(j >> 24)
	}
	rand.Seed(0)
	rand.Shuffle(len(res), func(i, j int) {
		res[i], res[j] = res[j], res[i]
	})
	return res
}

func benchmarkValueInsertion(trie *StateTrie, b *testing.B) {
	accounts := getTestAddresses(100)
	keys := getTestKeys(100)

	info := AccountInfo{Nonce: common.ToNonce(1)}
	val1 := common.Value{1}
	for i := 0; i < b.N; i++ {
		for _, account := range accounts {
			if err := trie.SetAccountInfo(account, info); err != nil {
				b.Fatalf("failed to create account %v: %v", account, err)
			}
			for _, key := range keys {
				if err := trie.SetValue(account, key, val1); err != nil {
					b.Fatalf("insertion failed: %v", err)
				}
			}
		}
		for _, account := range accounts {
			for _, key := range keys {
				if value, err := trie.GetValue(account, key); value != val1 || err != nil {
					b.Fatalf("invalid element in trie, wanted %v, got %v, err %v", val1, value, err)
				}
			}
		}
		for _, account := range accounts {
			for _, key := range keys {
				if err := trie.SetValue(account, key, common.Value{}); err != nil {
					b.Fatalf("deletion failed: %v", err)
				}
			}
		}
	}
}

func BenchmarkValueInsertionInMemoryTrie(b *testing.B) {
	trie, err := OpenInMemoryTrie(b.TempDir())
	if err != nil {
		b.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()
	benchmarkValueInsertion(trie, b)
}

func BenchmarkValueInsertionInFileTrie(b *testing.B) {
	trie, err := OpenFileTrie(b.TempDir())
	if err != nil {
		b.Fatalf("failed to open trie: %v", err)
	}
	defer trie.Close()
	benchmarkValueInsertion(trie, b)
}

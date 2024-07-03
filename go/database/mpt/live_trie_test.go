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
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"go.uber.org/mock/gomock"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestLiveTrie_EmptyTrieIsConsistent(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			trie, err := OpenInMemoryLiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024})
			if err != nil {
				t.Fatalf("failed to open trie: %v", err)
			}
			defer trie.Close()

			if err := trie.Check(); err != nil {
				t.Fatalf("empty try has consistency problems: %v", err)
			}
		})
	}
}

func TestLiveTrie_Cannot_Open_Memory(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			// corrupt meta
			if err := os.WriteFile(filepath.Join(dir, "forest.json"), []byte("Hello, World!"), 0644); err != nil {
				t.Fatalf("cannot update meta: %v", err)
			}

			if _, err := OpenInMemoryLiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}); err == nil {
				t.Errorf("opening trie should fail")
			}

		})
	}
}

func TestLiveTrie_Cannot_Open_File(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			// corrupt meta
			if err := os.WriteFile(filepath.Join(dir, "forest.json"), []byte("Hello, World!"), 0644); err != nil {
				t.Fatalf("cannot update meta: %v", err)
			}

			if _, err := OpenFileLiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}); err == nil {
				t.Errorf("opening trie should fail")
			}

		})
	}
}

func TestLiveTrie_Cannot_MakeTrie_CorruptMeta(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			// corrupt meta
			if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte("Hello, World!"), 0644); err != nil {
				t.Fatalf("cannot update meta: %v", err)
			}

			if _, err := OpenFileLiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}); err == nil {
				t.Errorf("opening trie should fail")
			}

		})
	}
}

func TestLiveTrie_Cannot_MakeTrie_CannotReadMeta(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			// corrupt meta
			if err := os.Mkdir(filepath.Join(dir, "meta.json"), 0644); err != nil {
				t.Fatalf("cannot update meta: %v", err)
			}

			if _, err := OpenFileLiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}); err == nil {
				t.Errorf("opening trie should fail")
			}

		})
	}
}

func TestLiveTrie_Cannot_Verify(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			// corrupt meta
			if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte("Hello, World!"), 0644); err != nil {
				t.Fatalf("cannot update roots: %v", err)
			}

			if err := VerifyFileLiveTrie(dir, config, NilVerificationObserver{}); err == nil {
				t.Errorf("opening trie should fail")
			}

		})
	}
}

func TestLiveTrie_Cannot_MakeTrie_CorruptForest(t *testing.T) {
	dir := t.TempDir()
	forest, err := OpenInMemoryForest(dir, S5LiveConfig, ForestConfig{})
	if err != nil {
		t.Fatalf("failed to create test forest")
	}
	defer forest.Close()

	injectedError := fmt.Errorf("injected error")
	forest.errors = []error{injectedError}

	_, err = makeTrie(dir, forest)
	if want, got := injectedError, err; !errors.Is(got, want) {
		t.Errorf("opening erroneous forest should have failed, wanted %v, got %v", want, got)
	}
}

func TestLiveTrie_Fail_Read_Data(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()

			mpt, err := OpenFileLiveTrie(dir, config, NodeCacheConfig{Capacity: 1024})
			if err != nil {
				t.Fatalf("cannot open trie: %s", err)
			}

			// inject failing stock to trigger an error applying the update
			var injectedErr = errors.New("injectedError")
			ctrl := gomock.NewController(t)
			db := NewMockDatabase(ctrl)
			db.EXPECT().SetAccountInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(NodeReference{}, injectedErr)
			db.EXPECT().GetAccountInfo(gomock.Any(), gomock.Any()).Return(AccountInfo{}, false, injectedErr)
			db.EXPECT().SetValue(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(NodeReference{}, injectedErr)
			db.EXPECT().GetValue(gomock.Any(), gomock.Any(), gomock.Any()).Return(common.Value{}, injectedErr)
			db.EXPECT().ClearStorage(gomock.Any(), gomock.Any()).Return(NodeReference{}, injectedErr)
			db.EXPECT().VisitTrie(gomock.Any(), gomock.Any()).Return(injectedErr)
			mpt.forest = db

			if err := mpt.SetAccountInfo(common.Address{1}, AccountInfo{}); !errors.Is(err, injectedErr) {
				t.Errorf("setting account should fail")
			}
			if _, _, err := mpt.GetAccountInfo(common.Address{1}); !errors.Is(err, injectedErr) {
				t.Errorf("getting account should fail")
			}
			if err := mpt.SetValue(common.Address{1}, common.Key{2}, common.Value{}); !errors.Is(err, injectedErr) {
				t.Errorf("setting value should fail")
			}
			if _, err := mpt.GetValue(common.Address{1}, common.Key{2}); !errors.Is(err, injectedErr) {
				t.Errorf("getting value should fail")
			}
			if err := mpt.ClearStorage(common.Address{1}); !errors.Is(err, injectedErr) {
				t.Errorf("getting account should fail")
			}
			nodeVisitor := NewMockNodeVisitor(ctrl)
			if err := mpt.VisitTrie(nodeVisitor); !errors.Is(err, injectedErr) {
				t.Errorf("getting account should fail")
			}
		})
	}
}

func TestLiveTrie_Cannot_Flush_Metadata(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()

			mpt, err := OpenFileLiveTrie(dir, config, NodeCacheConfig{Capacity: 1024})
			if err != nil {
				t.Fatalf("opening trie should not fail: %s", err)
			}

			// corrupt meta
			if err := os.Mkdir(filepath.Join(dir, "meta.json"), os.FileMode(0644)); err != nil {
				t.Fatalf("cannot change meta")
			}

			if err := mpt.Flush(); err == nil {
				t.Errorf("flush should fail")
			}
		})
	}
}

func TestLiveTrie_Cannot_Flush_Hashes(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()

			mpt, err := OpenFileLiveTrie(dir, config, NodeCacheConfig{Capacity: 1024})
			if err != nil {
				t.Fatalf("opening trie should not fail: %s", err)
			}

			// inject failing stock to trigger an error applying the update
			var injectedErr = errors.New("injectedError")
			ctrl := gomock.NewController(t)
			db := NewMockDatabase(ctrl)
			db.EXPECT().updateHashesFor(gomock.Any()).Return(common.Hash{}, nil, injectedErr)
			mpt.forest = db

			if err := mpt.Flush(); !errors.Is(err, injectedErr) {
				t.Errorf("flush should fail")
			}
		})
	}
}

func TestLiveTrie_NonExistingAccountsHaveEmptyInfo(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			trie, err := OpenInMemoryLiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024})
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
		})
	}
}

func TestLiveTrie_SetAndGetSingleAccountInformationWorks(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			trie, err := OpenInMemoryLiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024})
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
				Balance:  amount.New(2),
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
		})
	}
}

func TestLiveTrie_SetAndGetMultipleAccountInformationWorks(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			trie, err := OpenInMemoryLiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024})
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
		})
	}
}

func TestLiveTrie_NonExistingValueHasZeroValue(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			trie, err := OpenInMemoryLiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024})
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
		})
	}
}

func TestLiveTrie_ValuesCanBeSetAndRetrieved(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			trie, err := OpenInMemoryLiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024})
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
		})
	}
}

func TestLiveTrie_SameContentProducesSameHash(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			trie1, err := OpenInMemoryLiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024})
			if err != nil {
				t.Fatalf("failed to open trie: %v", err)
			}
			trie2, err := OpenInMemoryLiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024})
			if err != nil {
				t.Fatalf("failed to open trie: %v", err)
			}

			hash1, _, err := trie1.UpdateHashes()
			if err != nil {
				t.Errorf("failed to fetch hash of empty trie: %v", err)
			}
			hash2, _, err := trie2.UpdateHashes()
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

			hash1, _, err = trie1.UpdateHashes()
			if err != nil {
				t.Errorf("failed to fetch hash of non-empty trie: %v", err)
			}
			hash2, _, err = trie2.UpdateHashes()
			if err != nil {
				t.Errorf("failed to fetch hash of non-empty trie: %v", err)
			}
			if hash1 == hash2 {
				t.Errorf("Expected different tries to have different hashes, got %v and %v", hash1, hash2)
			}

			// Update tries to contain same data.
			trie1.SetAccountInfo(common.Address{2}, info2)
			trie2.SetAccountInfo(common.Address{1}, info1)

			hash1, _, err = trie1.UpdateHashes()
			if err != nil {
				t.Errorf("failed to fetch hash of non-empty trie: %v", err)
			}
			hash2, _, err = trie2.UpdateHashes()
			if err != nil {
				t.Errorf("failed to fetch hash of non-empty trie: %v", err)
			}
			if hash1 != hash2 {
				t.Errorf("Expected equal tries to have same hashes, got %v and %v", hash1, hash2)
			}
		})
	}
}

func TestLiveTrie_ChangeInTrieSubstructureUpdatesHash(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			trie, err := OpenInMemoryLiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024})
			if err != nil {
				t.Fatalf("failed to open trie: %v", err)
			}

			info1 := AccountInfo{Nonce: common.ToNonce(1)}
			info2 := AccountInfo{Nonce: common.ToNonce(2)}
			trie.SetAccountInfo(common.Address{1}, info1)
			trie.SetAccountInfo(common.Address{2}, info2)

			hash1, _, err := trie.UpdateHashes()
			if err != nil {
				t.Errorf("failed to fetch hash of empty trie: %v", err)
			}

			// The next update does not change anything in the root node, but the hash should
			// still be updated.
			trie.SetAccountInfo(common.Address{1}, info2)

			hash2, _, err := trie.UpdateHashes()
			if err != nil {
				t.Errorf("failed to fetch hash of empty trie: %v", err)
			}
			if hash1 == hash2 {
				t.Errorf("Nested modification should have caused a change in hashes, got %v and %v", hash1, hash2)
			}
		})
	}
}

func TestLiveTrie_InsertLotsOfData(t *testing.T) {
	for _, config := range allMptConfigs {
		config := config
		t.Run(config.Name, func(t *testing.T) {
			t.Parallel()
			const N = 30

			trie, err := OpenInMemoryLiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024 * 1024})
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
		})
	}
}

func TestLiveTrie_InsertLotsOfValues(t *testing.T) {
	for _, config := range allMptConfigs {
		config := config
		t.Run(config.Name, func(t *testing.T) {
			t.Parallel()
			const N = 500

			trie, err := OpenInMemoryLiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024})
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
		})
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
	rand := rand.New(rand.NewSource(0))
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
	rand := rand.New(rand.NewSource(0))
	rand.Shuffle(len(res), func(i, j int) {
		res[i], res[j] = res[j], res[i]
	})
	return res
}

func TestLiveTrie_DeleteLargeAccount(t *testing.T) {
	for _, config := range allMptConfigs {
		config := config
		t.Run(config.Name, func(t *testing.T) {
			t.Parallel()
			const N = 200000

			trie, err := OpenInMemoryLiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024 * 1024})
			if err != nil {
				t.Fatalf("failed to open trie: %v", err)
			}

			// Create a single account with a large storage.
			addr := common.Address{1, 2, 3}
			if err := trie.SetAccountInfo(addr, AccountInfo{Nonce: common.Nonce{1}}); err != nil {
				t.Fatalf("failed to create account: %v", err)
			}
			for i := 0; i < N; i++ {
				if err := trie.SetValue(addr, common.Key{byte(i), byte(i >> 8), byte(i >> 16)}, common.Value{1}); err != nil {
					t.Fatalf("failed to insert value: %v", err)
				}
				if i%100 == 0 {
					if _, _, err := trie.UpdateHashes(); err != nil {
						t.Fatalf("failed to update hashes: %v", err)
					}
				}
			}

			if _, _, err := trie.UpdateHashes(); err != nil {
				t.Fatalf("failed to update hashes: %v", err)
			}

			// There should be N value nodes now.
			ids, err := trie.forest.(*Forest).values.GetIds()
			if err != nil {
				t.Fatalf("failed to get list of value IDs: %v", err)
			}
			if want, got := N, getSize(ids); want != got {
				t.Errorf("unexpected number of values, wanted %d, got %d", want, got)
			}

			// Deleting the account storage should be fast (not blocking until the entire storage tree is released).
			start := time.Now()
			if err := trie.ClearStorage(addr); err != nil {
				t.Errorf("failed to clear storage: %v", err)
			}
			// If done wrong, the delete takes > 1 second.
			if duration, limit := time.Since(start), 50*time.Millisecond; duration > limit {
				t.Errorf("delete took too long: %v, limit %v", duration, limit)
			}

			if _, _, err := trie.UpdateHashes(); err != nil {
				t.Fatalf("failed to update hashes: %v", err)
			}

			if err := trie.Flush(); err != nil {
				t.Fatalf("failed to flush trie: %v", err)
			}

			// check the number of stored nodes to make sure everything but the account got released
			ids, err = trie.forest.(*Forest).accounts.GetIds()
			if err != nil {
				t.Fatalf("failed to get list of account IDs: %v", err)
			}
			if want, got := 1, getSize(ids); want != got {
				t.Errorf("unexpected number of accounts, wanted %d, got %d", want, got)
			}

			ids, err = trie.forest.(*Forest).branches.GetIds()
			if err != nil {
				t.Fatalf("failed to get list of branch IDs: %v", err)
			}
			if want, got := 0, getSize(ids); want != got {
				t.Errorf("unexpected number of branches, wanted %d, got %d", want, got)
			}

			ids, err = trie.forest.(*Forest).extensions.GetIds()
			if err != nil {
				t.Fatalf("failed to get list of extension IDs: %v", err)
			}
			if want, got := 0, getSize(ids); want != got {
				t.Errorf("unexpected number of extensions, wanted %d, got %d", want, got)
			}

			ids, err = trie.forest.(*Forest).values.GetIds()
			if err != nil {
				t.Fatalf("failed to get list of value IDs: %v", err)
			}
			if want, got := 0, getSize(ids); want != got {
				t.Errorf("unexpected number of values, wanted %d, got %d", want, got)
			}

			if err := trie.Close(); err != nil {
				t.Fatalf("failed to close trie: %v", err)
			}
		})
	}
}

func getSize(set stock.IndexSet[uint64]) int {
	counter := 0
	for i := set.GetLowerBound(); i < set.GetUpperBound(); i++ {
		if set.Contains(i) {
			counter++
		}
	}
	return counter
}

func TestLiveTrie_VerificationOfEmptyDirectoryPasses(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			if err := VerifyFileLiveTrie(dir, config, NilVerificationObserver{}); err != nil {
				t.Errorf("an empty directory should be fine, got: %v", err)
			}
		})
	}
}

func TestLiveTrie_VerificationOfFreshArchivePasses(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			trie, err := OpenFileLiveTrie(dir, config, NodeCacheConfig{Capacity: 1024})
			if err != nil {
				t.Fatalf("failed to create empty trie, err %v", err)
			}

			// Add some data.
			trie.SetAccountInfo(common.Address{1}, AccountInfo{Nonce: common.ToNonce(1), CodeHash: emptyCodeHash})
			trie.SetAccountInfo(common.Address{2}, AccountInfo{Nonce: common.ToNonce(2), CodeHash: emptyCodeHash})
			trie.SetAccountInfo(common.Address{3}, AccountInfo{Nonce: common.ToNonce(3), CodeHash: emptyCodeHash})

			trie.SetValue(common.Address{1}, common.Key{1}, common.Value{1})

			trie.SetValue(common.Address{2}, common.Key{1}, common.Value{1})
			trie.SetValue(common.Address{2}, common.Key{2}, common.Value{1})

			trie.SetValue(common.Address{3}, common.Key{1}, common.Value{1})
			trie.SetValue(common.Address{3}, common.Key{2}, common.Value{1})
			trie.SetValue(common.Address{3}, common.Key{3}, common.Value{1})

			// Delete some data.
			trie.SetAccountInfo(common.Address{2}, AccountInfo{})

			if err := trie.Close(); err != nil {
				t.Fatalf("failed to close trie: %v", err)
			}

			if err := VerifyFileLiveTrie(dir, config, NilVerificationObserver{}); err != nil {
				t.Errorf("a freshly closed LiveTrie should be fine, got: %v", err)
			}
		})
	}
}

func TestLiveTrie_VerificationOfLiveTrieWithMissingFileFails(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			trie, err := OpenFileLiveTrie(dir, config, NodeCacheConfig{Capacity: 1024})
			if err != nil {
				t.Fatalf("failed to create empty trie, err %v", err)
			}

			// Add some data.
			trie.SetAccountInfo(common.Address{1}, AccountInfo{Nonce: common.ToNonce(1)})
			trie.SetAccountInfo(common.Address{2}, AccountInfo{Nonce: common.ToNonce(2)})
			trie.SetAccountInfo(common.Address{3}, AccountInfo{Nonce: common.ToNonce(3)})

			trie.SetValue(common.Address{1}, common.Key{1}, common.Value{1})

			trie.SetValue(common.Address{2}, common.Key{1}, common.Value{1})
			trie.SetValue(common.Address{2}, common.Key{2}, common.Value{1})

			trie.SetValue(common.Address{3}, common.Key{1}, common.Value{1})
			trie.SetValue(common.Address{3}, common.Key{2}, common.Value{1})
			trie.SetValue(common.Address{3}, common.Key{3}, common.Value{1})

			// Delete some data.
			trie.SetAccountInfo(common.Address{2}, AccountInfo{})

			if err := trie.Close(); err != nil {
				t.Fatalf("failed to close trie: %v", err)
			}

			if err := os.Remove(dir + "/branches/freelist.dat"); err != nil {
				t.Fatalf("failed to delete file")
			}

			if err := VerifyFileLiveTrie(dir, config, NilVerificationObserver{}); err == nil {
				t.Errorf("missing file should be detected")
			}
		})
	}
}

func TestLiveTrie_VerificationOfLiveTrieWithCorruptedFileFails(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			trie, err := OpenFileLiveTrie(dir, config, NodeCacheConfig{Capacity: 1024})
			if err != nil {
				t.Fatalf("failed to create empty trie, err %v", err)
			}

			// Add some data.
			trie.SetAccountInfo(common.Address{1}, AccountInfo{Nonce: common.ToNonce(1)})
			trie.SetAccountInfo(common.Address{2}, AccountInfo{Nonce: common.ToNonce(2)})
			trie.SetAccountInfo(common.Address{3}, AccountInfo{Nonce: common.ToNonce(3)})

			trie.SetValue(common.Address{1}, common.Key{1}, common.Value{1})

			trie.SetValue(common.Address{2}, common.Key{1}, common.Value{1})
			trie.SetValue(common.Address{2}, common.Key{2}, common.Value{1})

			trie.SetValue(common.Address{3}, common.Key{1}, common.Value{1})
			trie.SetValue(common.Address{3}, common.Key{2}, common.Value{1})
			trie.SetValue(common.Address{3}, common.Key{3}, common.Value{1})

			// Delete some data.
			trie.SetAccountInfo(common.Address{2}, AccountInfo{})

			if err := trie.Close(); err != nil {
				t.Fatalf("failed to close trie: %v", err)
			}

			// manipulate one of the files
			filename := dir + "/branches/values.dat"
			data, err := os.ReadFile(filename)
			if err != nil {
				t.Fatalf("failed to load data from file: %v", err)
			}
			data[0]++
			if err := os.WriteFile(filename, data, 0600); err != nil {
				t.Fatalf("failed to modify file")
			}

			if err := VerifyFileLiveTrie(dir, config, NilVerificationObserver{}); err == nil {
				t.Errorf("corrupted file should have been detected")
			}
		})
	}
}

func TestLiveTrie_HasEmptyStorage(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()

			addr := common.Address{0x1}
			ctrl := gomock.NewController(t)
			db := NewMockDatabase(ctrl)
			db.EXPECT().HasEmptyStorage(gomock.Any(), addr)

			mpt, err := OpenFileLiveTrie(dir, config, NodeCacheConfig{Capacity: 1024})
			if err != nil {
				t.Fatalf("failed to open live trie: %v", err)
			}
			mpt.forest = db

			mpt.HasEmptyStorage(addr)
		})
	}

}

func TestLiveTrie_CreateWitnessProof(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			addr := common.Address{1}
			accountNode := &AccountNode{
				address:    addr,
				info:       AccountInfo{Balance: amount.New(1)},
				pathLength: 64,
			}

			db := NewMockDatabase(ctrl)
			db.EXPECT().getViewAccess(gomock.Any()).Return(shared.MakeShared[Node](accountNode).GetViewHandle(), nil)
			db.EXPECT().getConfig().Return(config)
			db.EXPECT().hashAddress(gomock.Any()).Return(common.Keccak256(addr[:])).AnyTimes()

			mpt, err := OpenInMemoryLiveTrie(t.TempDir(), config, NodeCacheConfig{})
			if err != nil {
				t.Fatalf("failed to open live trie: %v", err)
			}

			mpt.forest = db

			proof, err := mpt.CreateWitnessProof(addr)
			if err != nil {
				t.Errorf("failed to create witness proof: %v", err)
			}

			// mock root hash from the single node tree
			rlp, err := encodeToRlp(accountNode, db, []byte{})
			if err != nil {
				t.Fatalf("failed to encode account node: %v", err)
			}
			root := common.Keccak256(rlp)

			balance, complete, err := proof.GetBalance(root, addr)
			if err != nil {
				t.Fatalf("failed to get balance: %v", err)
			}
			if !complete {
				t.Errorf("proof should be complete")
			}

			if got, want := balance, amount.New(1); got != want {
				t.Errorf("unexpected balance, got %v, want %v", got, want)
			}
		})
	}

}

func benchmarkValueInsertion(trie *LiveTrie, b *testing.B) {
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
	for _, config := range allMptConfigs {
		b.Run(config.Name, func(b *testing.B) {
			b.StopTimer()
			trie, err := OpenInMemoryLiveTrie(b.TempDir(), config, NodeCacheConfig{Capacity: 1024})
			if err != nil {
				b.Fatalf("failed to open trie: %v", err)
			}
			defer trie.Close()
			b.StartTimer()
			benchmarkValueInsertion(trie, b)
			b.StopTimer()
		})
	}
}

func BenchmarkValueInsertionInFileTrie(b *testing.B) {
	for _, config := range allMptConfigs {
		b.Run(config.Name, func(b *testing.B) {
			b.StopTimer()
			trie, err := OpenFileLiveTrie(b.TempDir(), config, NodeCacheConfig{Capacity: 1024})
			if err != nil {
				b.Fatalf("failed to open trie: %v", err)
			}
			defer trie.Close()
			b.StartTimer()
			benchmarkValueInsertion(trie, b)
			b.StopTimer()
		})
	}
}

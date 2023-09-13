package mpt

import (
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

// Note: most properties of the ArchiveTrie are tested through the common
// test infrastructure in /backend/archive.
//
// TODO: generalize common archive tests in /backend/archive such that they
// can be executed as part of this package's test suite

func TestArchiveTrie_OpenAndClose(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			archive, err := OpenArchiveTrie(t.TempDir(), config)
			if err != nil {
				t.Fatalf("failed to open empty archive: %v", err)
			}
			if err := archive.Close(); err != nil {
				t.Fatalf("failed to close empty archive: %v", err)
			}
		})
	}
}

func TestArchiveTrie_CanHandleMultipleBlocks(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			archive, err := OpenArchiveTrie(t.TempDir(), config)
			if err != nil {
				t.Fatalf("failed to open empty archive: %v", err)
			}
			defer archive.Close()

			addr1 := common.Address{1}
			blc0 := common.Balance{0}
			blc1 := common.Balance{1}
			blc2 := common.Balance{2}

			archive.Add(1, common.Update{
				CreatedAccounts: []common.Address{addr1},
				Balances: []common.BalanceUpdate{
					{Account: addr1, Balance: blc1},
				},
			})

			archive.Add(3, common.Update{
				Balances: []common.BalanceUpdate{
					{Account: addr1, Balance: blc2},
				},
			})

			want := []common.Balance{blc0, blc1, blc1, blc2}
			for i, want := range want {
				got, err := archive.GetBalance(uint64(i), addr1)
				if err != nil || got != want {
					t.Errorf("wrong balance for block %d, got %v, wanted %v, err %v", i, got, want, err)
				}
			}
		})
	}
}

func TestArchiveTrie_HistoryIsConsistent(t *testing.T) {
	for _, config := range allMptConfigs {
		config := config
		for _, cacheSize := range []int{10, 1000, config.NodeCacheSize} {
			config.NodeCacheSize = cacheSize
			config.AddressHashCacheSize = cacheSize
			config.HashCacheSize = cacheSize
			t.Run(fmt.Sprintf("%s/%d", config.Name, cacheSize), func(t *testing.T) {
				archive, err := OpenArchiveTrie(t.TempDir(), config)
				if err != nil {
					t.Fatalf("failed to create empty archive: %v", err)
				}

				// Add 100 blocks of history, record hashes, and check consistency.
				hashes := make([]common.Hash, 0, 100)
				for i := 0; i < 100; i++ {

					addr1 := common.Address{byte(i >> 8), byte(i)}
					addr2 := common.Address{0, byte(i >> 8), byte(i)}
					update := common.Update{
						CreatedAccounts: []common.Address{addr1, addr2},
						Nonces: []common.NonceUpdate{
							{Account: addr1, Nonce: common.ToNonce(uint64(i + 1))},
							{Account: addr1, Nonce: common.ToNonce(uint64(i + 2))},
						},
					}

					if err := archive.Add(uint64(i), update); err != nil {
						t.Fatalf("failed to add block update for block %d: %v", i, err)
					}

					// Record the current hash of the archive.
					hash, err := archive.GetHash(uint64(i))
					if err != nil {
						t.Errorf("failed to get hash for block %d", i)
					}
					hashes = append(hashes, hash)

					// Check the entire history of the archive.
					if err := archive.(*ArchiveTrie).Check(); err != nil {
						t.Fatalf("inconsistencies encountered in archive:\n%v", err)
					}
				}

				// Check that the hashes of the individual blocks can be reproduced.
				for i, want := range hashes {
					got, err := archive.GetHash(uint64(i))
					if err != nil {
						t.Fatalf("failed to recompute hash of block %d: %v", i, err)
					}
					if want != got {
						t.Fatalf("recomputed invalid hash for block %d, wanted %v, got %v", i, want, got)
					}
				}

				if err := archive.Close(); err != nil {
					t.Fatalf("failed to close the archive: %v", err)
				}
			})
		}
	}
}

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
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/backend/utils/checkpoint"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"golang.org/x/exp/maps"

	"go.uber.org/mock/gomock"

	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Note: most properties of the ArchiveTrie are tested through the common
// test infrastructure in /backend/archive.
//
// TODO [cleanup]: generalize common archive tests in /backend/archive such that they
// can be executed as part of this package's test suite

func TestArchiveTrie_OpenAndClose(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			archive, err := OpenArchiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to open empty archive: %v", err)
			}
			if err := archive.Close(); err != nil {
				t.Fatalf("failed to close empty archive: %v", err)
			}
		})
	}
}

func TestArchiveTrie_CanOnlyBeOpenedOnce(t *testing.T) {
	dir := t.TempDir()
	archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
	if err != nil {
		t.Fatalf("failed to open test archive: %v", err)
	}
	if _, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{}); err == nil {
		t.Fatalf("archive should not be accessible by more than one instance")
	}
	if err := archive.Close(); err != nil {
		t.Errorf("failed to close the archive: %v", err)
	}
}

func TestArchiveTrie_CanBeReOpened(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 5; i++ {
		archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
		if err != nil {
			t.Fatalf("failed to open test archive: %v", err)
		}
		if err := archive.Close(); err != nil {
			t.Errorf("failed to close the archive: %v", err)
		}
	}
}

func TestArchiveTrie_Open_Fails_Wrong_Roots(t *testing.T) {
	dir := t.TempDir()
	archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
	if err != nil {
		t.Fatalf("cannot init archive trie: %v", err)
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("failed to close archive trie: %v", err)
	}

	// corrupt the roots
	if err := os.WriteFile(filepath.Join(dir, fileNameArchiveRoots), []byte("Hello, World!"), 0644); err != nil {
		t.Fatalf("cannot update roots: %v", err)
	}

	if archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{}); err == nil {
		_ = archive.Close()
		t.Errorf("opening archive should not succeed")
	}
}

func TestArchiveTrie_Open_Fails_Too_Short_Roots(t *testing.T) {
	dir := t.TempDir()
	archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
	if err != nil {
		t.Fatalf("cannot init archive trie: %v", err)
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("cannot init archive trie: %v", err)
	}

	// corrupt the roots
	if err := os.WriteFile(filepath.Join(dir, fileNameArchiveRoots), []byte("H"), 0644); err != nil {
		t.Fatalf("cannot update roots: %v", err)
	}

	if archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{}); err == nil {
		_ = archive.Close()
		t.Errorf("opening archive should not succeed")
	}
}

func TestArchiveTrie_Open_Fails_CannotOpen_Roots(t *testing.T) {
	dir := t.TempDir()
	archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
	if err != nil {
		t.Fatalf("cannot init archive trie: %v", err)
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("cannot init archive trie: %v", err)
	}

	// remove read access
	if err := os.Chmod(filepath.Join(dir, fileNameArchiveRoots), 0); err != nil {
		t.Fatalf("cannot chmod roots file: %v", err)
	}

	if archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{}); err == nil {
		_ = archive.Close()
		t.Errorf("opening archive should not succeed")
	}
}

func TestArchiveTrie_Open_Fails_Wrong_ForestMeta(t *testing.T) {
	dir := t.TempDir()
	archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
	if err != nil {
		t.Fatalf("cannot init archive trie: %v", err)
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("cannot init archive trie: %v", err)
	}

	// corrupt the meta
	if err := os.WriteFile(filepath.Join(dir, "forest.json"), []byte("Hello, World!"), 0644); err != nil {
		t.Fatalf("cannot update meta: %v", err)
	}

	if archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{}); err == nil {
		_ = archive.Close()
		t.Errorf("opening archive should not succeed")
	}
}

func TestArchiveTrie_Open_Fails_Wrong_TrieMeta(t *testing.T) {
	dir := t.TempDir()
	archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
	if err != nil {
		t.Fatalf("cannot init archive trie: %v", err)
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("cannot init archive trie: %v", err)
	}

	// corrupt the meta
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte("Hello, World!"), 0644); err != nil {
		t.Fatalf("cannot update meta: %v", err)
	}

	if archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{}); err == nil {
		_ = archive.Close()
		t.Errorf("opening archive should not succeed")
	}
}

func TestArchiveTrie_Open_Fails_Wrong_Codes(t *testing.T) {
	dir := t.TempDir()
	archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
	if err != nil {
		t.Fatalf("cannot init archive trie: %v", err)
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("cannot init archive trie: %v", err)
	}

	// corrupt the codes
	if err := os.WriteFile(filepath.Join(dir, "codes.dat"), []byte("Hello, World!"), 0644); err != nil {
		t.Fatalf("cannot update codes: %v", err)
	}

	if archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{}); err == nil {
		_ = archive.Close()
		t.Errorf("opening archive should not succeed")
	}
}

func TestArchiveTrie_Open_Fails_InconsistentCheckpoints(t *testing.T) {
	openForest := func(dir string) (*Forest, error) {
		cacheConfig := NodeCacheConfig{Capacity: 1024}
		forestConfig := ForestConfig{Mode: Immutable, NodeCacheConfig: cacheConfig}
		config := S5ArchiveConfig
		return OpenFileForest(dir, config, forestConfig)
	}

	// This test checks that misalignments in the checkpoints of various
	// components of the archive are detected and cause the opening of the
	// archive to fail. Implicitly, it also checks that all relevant parts
	// of the archive are covered by the checkpoint mechanism.
	tests := map[string]func(dir string) error{
		"invalid top-level checkpoint": func(dir string) error {
			checkpointDirectory := filepath.Join(dir, fileNameArchiveCheckpointDirectory)
			coordinator, err := checkpoint.NewCoordinator(checkpointDirectory)
			if err != nil {
				return err
			}
			_, err = coordinator.CreateCheckpoint()
			return err
		},
		"invalid accounts checkpoint": func(dir string) error {
			forest, err := openForest(dir)
			if err != nil {
				return err
			}
			checkpoint := checkpoint.Checkpoint(1)
			return errors.Join(
				forest.accounts.Prepare(checkpoint),
				forest.accounts.Commit(checkpoint),
				forest.Close(),
			)
		},
		"invalid branches checkpoint": func(dir string) error {
			forest, err := openForest(dir)
			if err != nil {
				return err
			}
			checkpoint := checkpoint.Checkpoint(1)
			return errors.Join(
				forest.branches.Prepare(checkpoint),
				forest.branches.Commit(checkpoint),
				forest.Close(),
			)
		},
		"invalid extension checkpoint": func(dir string) error {
			forest, err := openForest(dir)
			if err != nil {
				return err
			}
			checkpoint := checkpoint.Checkpoint(1)
			return errors.Join(
				forest.extensions.Prepare(checkpoint),
				forest.extensions.Commit(checkpoint),
				forest.Close(),
			)
		},
		"invalid values checkpoint": func(dir string) error {
			forest, err := openForest(dir)
			if err != nil {
				return err
			}
			checkpoint := checkpoint.Checkpoint(1)
			return errors.Join(
				forest.values.Prepare(checkpoint),
				forest.values.Commit(checkpoint),
				forest.Close(),
			)
		},
		"invalid codes checkpoint": func(dir string) error {
			codes, err := openCodes(dir)
			if err != nil {
				return err
			}
			checkpoint := checkpoint.Checkpoint(1)
			return errors.Join(
				codes.Prepare(checkpoint),
				codes.Commit(checkpoint),
			)
		},
		"invalid roots checkpoint": func(dir string) error {
			roots, err := loadRoots(dir)
			if err != nil {
				return err
			}
			checkpoint := checkpoint.Checkpoint(1)
			return errors.Join(
				roots.Prepare(checkpoint),
				roots.Commit(checkpoint),
			)
		},
	}

	for name, corrupt := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("cannot init archive trie: %v", err)
			}
			if err := archive.Close(); err != nil {
				t.Fatalf("cannot init archive trie: %v", err)
			}

			// corrupt the codes
			if err := corrupt(dir); err != nil {
				t.Fatalf("failed to corrupt test Archive: %v", err)
			}

			if archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{}); err == nil {
				_ = archive.Close()
				t.Errorf("opening archive should not succeed")
			}
		})
	}
}

func TestArchiveTrie_CanTrackBlocksHeight(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			archive, err := OpenArchiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to open empty archive: %v", err)
			}
			defer archive.Close()

			block, empty, err := archive.GetBlockHeight()
			if err != nil {
				t.Fatalf("failed to get block height: %v", err)
			}
			if !empty || block != 0 {
				t.Errorf("archive should be initially empty, got %t, %d", empty, block)
			}

			archive.Add(1, common.Update{}, nil)

			block, empty, err = archive.GetBlockHeight()
			if err != nil {
				t.Fatalf("failed to get block height: %v", err)
			}
			if empty || block != 1 {
				t.Errorf("archive should be have height 1, got %t, %d", empty, block)
			}

			archive.Add(3, common.Update{}, nil)

			block, empty, err = archive.GetBlockHeight()
			if err != nil {
				t.Fatalf("failed to get block height: %v", err)
			}
			if empty || block != 3 {
				t.Errorf("archive should be have height 3, got %t, %d", empty, block)
			}
		})
	}
}

func TestArchiveTrie_CanHandleMultipleBlocks(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			archive, err := OpenArchiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to open empty archive: %v", err)
			}
			defer archive.Close()

			addr1 := common.Address{1}
			blc0 := amount.New()
			blc1 := amount.New(1)
			blc2 := amount.New(2)

			archive.Add(1, common.Update{
				CreatedAccounts: []common.Address{addr1},
				Balances: []common.BalanceUpdate{
					{Account: addr1, Balance: blc1},
				},
			}, nil)

			archive.Add(3, common.Update{
				Balances: []common.BalanceUpdate{
					{Account: addr1, Balance: blc2},
				},
			}, nil)

			want := []amount.Amount{blc0, blc1, blc1, blc2}
			for i, want := range want {
				got, err := archive.GetBalance(uint64(i), addr1)
				if err != nil || got != want {
					t.Errorf("wrong balance for block %d, got %v, wanted %v, err %v", i, got, want, err)
				}
			}
		})
	}
}

func TestArchiveTrie_CanHandleEmptyBlocks(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			archive, err := OpenArchiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to open empty archive: %v", err)
			}
			defer archive.Close()

			addr := common.Address{1}
			balance := amount.New()

			// Block 1 adds an actual change.
			err = archive.Add(1, common.Update{
				CreatedAccounts: []common.Address{addr},
				Balances: []common.BalanceUpdate{
					{Account: addr, Balance: balance},
				},
			}, nil)
			if err != nil {
				t.Errorf("failed to add block: %v", err)
			}

			// Block 2 is skipped.

			// Block 3 is empty, without hints.
			if err := archive.Add(3, common.Update{}, nil); err != nil {
				t.Errorf("failed to add block: %v", err)
			}

			// Block 4 is empty, with hints.
			if err := archive.Add(4, common.Update{}, nil); err != nil {
				t.Errorf("failed to add block: %v", err)
			}

			for i := 0; i < 5; i++ {
				got, err := archive.GetBalance(uint64(i), addr)
				if err != nil || got != balance {
					t.Errorf("wrong balance for block %d, got %v, wanted %v, err %v", i, got, balance, err)
				}
			}
		})
	}
}

func TestArchiveTrie_VerifyArchive_Failure_Meta(t *testing.T) {
	dir := t.TempDir()
	archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
	if err != nil {
		t.Fatalf("cannot init archive trie: %v", err)
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("cannot init archive trie: %v", err)
	}

	// corrupt the roots
	if err := os.WriteFile(filepath.Join(dir, fileNameArchiveRoots), []byte("Hello, World!"), 0644); err != nil {
		t.Fatalf("cannot update roots: %v", err)
	}

	if err := VerifyArchiveTrie(dir, S5ArchiveConfig, NilVerificationObserver{}); err == nil {
		t.Errorf("verification should fail")
	}
}

func TestArchiveTrie_CanProcessPrecomputedHashes(t *testing.T) {
	for _, config := range allMptConfigs {
		if config.HashStorageLocation != HashStoredWithNode {
			continue
		}
		t.Run(config.Name, func(t *testing.T) {
			live, err := OpenGoMemoryState(t.TempDir(), config, NodeCacheConfig{Capacity: 1024})
			if err != nil {
				t.Fatalf("failed to open live trie: %v", err)
			}

			archiveDir := t.TempDir()
			archive, err := OpenArchiveTrie(archiveDir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to open empty archive: %v", err)
			}

			addr1 := common.Address{1}
			addr2 := common.Address{2}
			blc1 := amount.New(1)
			blc2 := amount.New(2)

			// Block 1
			update := common.Update{
				CreatedAccounts: []common.Address{addr1, addr2},
				Balances: []common.BalanceUpdate{
					{Account: addr1, Balance: blc1},
					{Account: addr2, Balance: blc2},
				},
			}
			hints, err := live.Apply(1, update)
			if err != nil {
				t.Fatalf("failed to update live db: %v", err)
			}
			err = archive.Add(1, update, hints)
			if err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			// Block 2
			update = common.Update{
				Balances: []common.BalanceUpdate{{Account: addr1, Balance: blc2}},
			}
			hints, err = live.Apply(2, update)
			if err != nil {
				t.Fatalf("failed to update live db: %v", err)
			}

			err = archive.Add(2, update, hints)
			if err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			// Block 4 -- larger range of data
			update = common.Update{}
			for i := 0; i < 100; i++ {
				addr := common.Address{byte(i + 10)}
				err = errors.Join(
					live.CreateAccount(addr),
					live.SetBalance(addr, blc1),
				)
				if err != nil {
					t.Fatalf("failed to update live db: %v", err)
				}
				update.CreatedAccounts = append(update.CreatedAccounts, addr)
				update.Balances = append(update.Balances, common.BalanceUpdate{Account: addr, Balance: blc1})
			}
			hints, err = live.Apply(4, update)
			if err != nil {
				t.Fatalf("failed to update live db: %v", err)
			}
			err = archive.Add(4, update, hints)
			if err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			if err := errors.Join(live.Close(), archive.Close()); err != nil {
				t.Fatalf("failed to close resources: %v", err)
			}

			if err := VerifyArchiveTrie(archiveDir, config, NilVerificationObserver{}); err != nil {
				t.Errorf("failed to verify archive: %v", err)
			}
		})
	}
}

func TestArchiveTrie_VerificationOfEmptyDirectoryPasses(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			if err := VerifyArchiveTrie(dir, config, NilVerificationObserver{}); err != nil {
				t.Errorf("an empty directory should be fine, got: %v", err)
			}
		})
	}
}

func TestArchiveTrie_VerificationOfFreshArchivePasses(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			err = archive.Add(2, common.Update{
				CreatedAccounts: []common.Address{{1}, {2}},
				Nonces: []common.NonceUpdate{
					{Account: common.Address{1}, Nonce: common.ToNonce(1)},
					{Account: common.Address{2}, Nonce: common.ToNonce(2)},
				},
				Slots: []common.SlotUpdate{
					{Account: common.Address{1}, Key: common.Key{1}, Value: common.Value{3}},
					{Account: common.Address{1}, Key: common.Key{2}, Value: common.Value{2}},
					{Account: common.Address{1}, Key: common.Key{3}, Value: common.Value{1}},
				},
			}, nil)
			if err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			err = archive.Add(4, common.Update{
				CreatedAccounts: []common.Address{{3}},
				Nonces: []common.NonceUpdate{
					{Account: common.Address{3}, Nonce: common.ToNonce(3)},
				},
				Slots: []common.SlotUpdate{
					{Account: common.Address{3}, Key: common.Key{2}, Value: common.Value{2}},
					{Account: common.Address{3}, Key: common.Key{3}, Value: common.Value{1}},
				},
			}, nil)
			if err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			if err := archive.Close(); err != nil {
				t.Fatalf("failed to close archive: %v", err)
			}

			if err := VerifyArchiveTrie(dir, config, NilVerificationObserver{}); err != nil {
				t.Errorf("a freshly closed archive should be fine, got: %v", err)
			}
		})
	}
}

func TestArchiveTrie_Add_DuplicatedBlock(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()

			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			if err = archive.Add(2, common.Update{
				CreatedAccounts: []common.Address{{1}, {2}},
			}, nil); err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			if err = archive.Add(2, common.Update{
				CreatedAccounts: []common.Address{{1}, {2}},
			}, nil); err == nil {
				t.Errorf("adding duplicate block should fail")
			}
		})
	}
}

func TestArchiveTrie_Add_UpdateFailsHashing(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()

			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			// inject a failing hasher
			var injectedError = errors.New("injectedError")
			ctrl := gomock.NewController(t)
			db := NewMockDatabase(ctrl)
			db.EXPECT().Freeze(gomock.Any())
			live := NewMockLiveState(ctrl)
			live.EXPECT().GetHash().Return(common.Hash{}, nil)
			live.EXPECT().Root().AnyTimes()
			live.EXPECT().UpdateHashes().Return(common.Hash{}, nil, injectedError)
			archive.head = live
			archive.forest = db

			// fails for computing missing blocks
			if err = archive.Add(20, common.Update{}, nil); !errors.Is(err, injectedError) {
				t.Errorf("applying update should fail")
			}
		})
	}
}

func TestArchiveTrie_Add_CreatesCheckpointPeriodically(t *testing.T) {
	for _, interval := range []int{1, 2, 3, 5, 7, 11} {
		t.Run(fmt.Sprintf("interval-%d", interval), func(t *testing.T) {
			dir := t.TempDir()
			archiveConfig := ArchiveConfig{
				CheckpointInterval: interval,
			}
			archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, archiveConfig)
			if err != nil {
				t.Fatalf("failed to open empty archive: %v", err)
			}
			defer archive.Close()

			for i := 0; i < 20; i++ {
				if err := archive.Add(uint64(i), common.Update{}, nil); err != nil {
					t.Fatalf("failed to apply update: %v", err)
				}
				cpWanted := checkpoint.Checkpoint(i / interval)
				cpGot := archive.checkpointCoordinator.GetCurrentCheckpoint()
				if cpWanted != cpGot {
					t.Errorf("wrong checkpoint after block %d, want %d, have %d", i, cpWanted, cpGot)
				}

				cpBlockWanted := int(cpWanted) * interval
				cpBlockGot, err := GetCheckpointBlock(dir)

				if cpBlockWanted == 0 {
					if err == nil {
						t.Fatalf("expected error indicating no available checkpoint")
					}
				} else {
					if err != nil {
						t.Fatalf("failed to get checkpoint block: %v", err)
					}

					if cpBlockWanted != int(cpBlockGot) {
						t.Errorf("wrong checkpoint block, want %d, have %d", cpBlockWanted, cpBlockGot)
					}
				}
			}
		})
	}
}

func TestArchiveTrie_Add_CheckpointsAreCreatedForMissingBlocks(t *testing.T) {
	dir := t.TempDir()
	archiveConfig := ArchiveConfig{
		CheckpointInterval: 5,
	}
	archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, archiveConfig)
	if err != nil {
		t.Fatalf("failed to open empty archive: %v", err)
	}
	defer archive.Close()

	// Adding block 4 should not create a checkpoint.
	if err := archive.Add(4, common.Update{}, nil); err != nil {
		t.Fatalf("failed to apply update: %v", err)
	}

	if want, got := checkpoint.Checkpoint(0), archive.checkpointCoordinator.GetCurrentCheckpoint(); want != got {
		t.Fatalf("wrong checkpoint, want %d, have %d", want, got)
	}

	// Adding block 7 should create a checkpoint, although it is not a multiple of 5.
	if err := archive.Add(7, common.Update{}, nil); err != nil {
		t.Fatalf("failed to apply update: %v", err)
	}

	if want, got := checkpoint.Checkpoint(1), archive.checkpointCoordinator.GetCurrentCheckpoint(); want != got {
		t.Fatalf("wrong checkpoint, want %d, have %d", want, got)
	}

	if got, err := GetCheckpointBlock(dir); err != nil || got != 7 {
		t.Fatalf("wrong checkpoint block, want %d, have %d, err %v", 7, got, err)
	}

	// Adding block 50 should should also create a checkpoint, although many blocks have been skipped.
	if err := archive.Add(50, common.Update{}, nil); err != nil {
		t.Fatalf("failed to apply update: %v", err)
	}

	if want, got := checkpoint.Checkpoint(2), archive.checkpointCoordinator.GetCurrentCheckpoint(); want != got {
		t.Fatalf("wrong checkpoint, want %d, have %d", want, got)
	}

	if got, err := GetCheckpointBlock(dir); err != nil || got != 50 {
		t.Fatalf("wrong checkpoint block, want %d, have %d, err %v", 50, got, err)
	}
}

func TestArchiveTrie_Add_FailingToCreateCheckpointsIsDetected(t *testing.T) {
	dir := t.TempDir()
	archiveConfig := ArchiveConfig{
		CheckpointInterval: 5,
	}
	archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1024}, archiveConfig)
	if err != nil {
		t.Fatalf("failed to open empty archive: %v", err)
	}
	defer archive.Close()

	// by creating a checkpoint only for the roots, the checkpoints
	// of the various components will be out of sync, sabotaging the
	// creation of a new checkpoint.
	cp := checkpoint.Checkpoint(1)
	err = errors.Join(
		archive.roots.Prepare(cp),
		archive.roots.Commit(cp),
	)
	if err != nil {
		t.Fatalf("failed to create checkpoint for the root list: %v", err)
	}

	// Adding block 4 should not create a checkpoint, and thus passes.
	if err := archive.Add(4, common.Update{}, nil); err != nil {
		t.Fatalf("failed to apply update: %v", err)
	}

	// Adding block 5 should add a checkpoint, and an error should be detected.
	if err := archive.Add(5, common.Update{}, nil); err == nil || !strings.Contains(err.Error(), "checkpoint mismatch") {
		t.Errorf("adding block should fail due to checkpoint creation issue, got: %v", err)
	}
}

func TestArchiveTrie_RootsGrowSubLinearly(t *testing.T) {
	dir := t.TempDir()
	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to create empty root list: %v", err)
	}

	// Golang slices grow by the factor of 2 when they are small,
	// while they grow slower when they become huge.
	// The slice 'archive.roots' contains millions of elements
	// stored as GiBs in memory.
	// This test verifies that the slices grow slower for huge arrays
	// to ensure that memory consumption is not doubled every time
	// the slice grows.
	// This feature cannot be customized, i.e., this test verifies
	// that the described assumption will hold in future versions
	// of golang and/or runtime configurations.

	const size = 100_000
	const threshold = 10_000
	const factor = 1.3

	var prevCap int
	for i := 0; i < size; i++ {
		roots.append(Root{})

		if i > threshold {
			if got, want := cap(roots.roots), int(factor*float64(prevCap)); got >= want {
				t.Fatalf("array grows too fast: %d >= %d", got, want)
			}
		}

		prevCap = cap(roots.roots)
	}
}

func TestArchiveTrie_Add_LiveStateFailsHashing(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()

			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			// inject a failing hasher
			var injectedError = errors.New("injectedError")
			ctrl := gomock.NewController(t)
			live := NewMockLiveState(ctrl)
			live.EXPECT().GetHash().Return(common.Hash{}, injectedError)
			archive.head = live

			// fails for computing missing blocks
			if err = archive.Add(20, common.Update{
				CreatedAccounts: []common.Address{{1}, {2}},
			}, nil); !errors.Is(err, injectedError) {
				t.Errorf("applying update should fail")
			}
		})
	}
}

func TestArchiveTrie_Add_LiveStateFailsCreateAccount(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()

			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			// inject a failing hasher
			var injectedError = errors.New("injectedError")
			ctrl := gomock.NewController(t)
			live := NewMockLiveState(ctrl)
			live.EXPECT().CreateAccount(gomock.Any()).Return(injectedError)
			archive.head = live

			// fails for computing this block
			if err = archive.Add(0, common.Update{
				CreatedAccounts: []common.Address{{1}, {2}},
			}, nil); !errors.Is(err, injectedError) {
				t.Errorf("applying update should fail")
			}
		})
	}
}

func TestArchiveTrie_Add_FreezingFails(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()

			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to open archive, err %v", err)
			}

			// inject failing stock to trigger an error applying the update
			var injectedErr = errors.New("failed to get value from stock")
			ctrl := gomock.NewController(t)
			db := NewMockDatabase(ctrl)
			db.EXPECT().Freeze(gomock.Any()).Return(injectedErr)
			live := NewMockLiveState(ctrl)
			live.EXPECT().Root().Return(NewNodeReference(ValueId(123)))
			archive.head = live
			archive.forest = db

			// update to freeze a node fails
			if err = archive.Add(0, common.Update{}, nil); !errors.Is(err, injectedErr) {
				t.Errorf("applying update should fail")
			}

		})
	}
}

func TestArchiveTrie_GettingView_Block_OutOfRange(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			if _, err := archive.Exists(100, common.Address{1}); err == nil {
				t.Errorf("block out of range should fail")
			}
			if _, err := archive.GetBalance(100, common.Address{1}); err == nil {
				t.Errorf("block out of range should fail")
			}
			if _, err := archive.GetCode(100, common.Address{1}); err == nil {
				t.Errorf("block out of range should fail")
			}
			if _, err := archive.GetNonce(100, common.Address{1}); err == nil {
				t.Errorf("block out of range should fail")
			}
			if _, err := archive.GetStorage(100, common.Address{1}, common.Key{2}); err == nil {
				t.Errorf("block out of range should fail")
			}
		})
	}
}

func TestArchiveTrie_GetCodes(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()

			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			codes := archive.GetCodes()
			if len(codes) != 0 {
				t.Errorf("unexpected number of codes in archive, expected 0, got %d", len(codes))
			}

			addr1 := common.Address{1}
			addr2 := common.Address{2}
			code1 := []byte{1, 2, 3}
			code2 := []byte{4, 5}
			if err = archive.Add(0, common.Update{
				CreatedAccounts: []common.Address{addr1, addr2},
				Codes: []common.CodeUpdate{
					{Account: addr1, Code: code1},
					{Account: addr2, Code: code2},
				},
			}, nil); err != nil {
				t.Fatalf("cannot apply update: %s", err)
			}

			codes = archive.GetCodes()
			if len(codes) != 2 {
				t.Errorf("unexpected number of codes in archive, wanted 2, got %d", len(codes))
			}
			if code, found := codes[common.Keccak256(code1)]; !found || !bytes.Equal(code, code1) {
				t.Errorf("expected code %x in codes, found %t, got %x", code1, found, code)
			}
			if code, found := codes[common.Keccak256(code2)]; !found || !bytes.Equal(code, code2) {
				t.Errorf("expected code %x in codes, found %t, got %x", code2, found, code)
			}

			if err := archive.Close(); err != nil {
				t.Fatalf("cannot close archive: %v", err)
			}
		})
	}
}

func TestArchiveTrie_GetHash(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()

			{
				archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
				if err != nil {
					t.Fatalf("failed to create empty archive, err %v", err)
				}

				if err = archive.Add(0, common.Update{
					CreatedAccounts: []common.Address{{1}},
				}, nil); err != nil {
					t.Fatalf("cannot apply update: %s", err)
				}

				if err := archive.Close(); err != nil {
					t.Fatalf("cannot close archive: %v", err)
				}
			}

			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			hash, err := archive.GetHash(0)
			if err != nil {
				t.Errorf("cannot compute hash: %v", err)
			}
			var empty common.Hash
			if hash == empty {
				t.Errorf("hash is empty")
			}

			if _, err := archive.GetHash(100); err == nil {
				t.Errorf("getting hash of non-existing hash should fail")
			}
		})
	}
}

func TestArchiveTrie_CannotGet_AccountHash(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}
			if _, err := archive.GetAccountHash(0, common.Address{1}); err == nil {
				t.Errorf("getting account hash should always fail")
			}
		})
	}
}

func TestArchiveTrie_CreateWitnessProof(t *testing.T) {
	for _, config := range []MptConfig{S5LiveConfig, S5ArchiveConfig} {
		t.Run(config.Name, func(t *testing.T) {
			arch, err := OpenArchiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			defer func() {
				if err := arch.Close(); err != nil {
					t.Fatalf("failed to close archive; %s", err)
				}
			}()

			if err := arch.Add(1, common.Update{
				CreatedAccounts: []common.Address{{1}},
				Balances: []common.BalanceUpdate{
					{Account: common.Address{1}, Balance: amount.New(12)},
				},
				Slots: []common.SlotUpdate{
					{Account: common.Address{1}, Key: common.Key{2}, Value: common.Value{3}},
				},
			}, nil); err != nil {
				t.Fatalf("failed to add block: %v", err)
			}

			proof, err := arch.CreateWitnessProof(1, common.Address{1}, common.Key{2})
			if err != nil {
				if errors.Is(err, archive.ErrWitnessProofNotSupported) {
					t.Skip(err)
				}
				t.Fatalf("failed to create witness proof; %s", err)
			}
			if !proof.IsValid() {
				t.Errorf("invalid proof")
			}

			hash, err := arch.GetHash(1)
			if err != nil {
				t.Fatalf("failed to get hash; %s", err)
			}
			balance, complete, err := proof.GetBalance(hash, common.Address{1})
			if err != nil {
				t.Fatalf("failed to get balance; %s", err)
			}
			if !complete {
				t.Errorf("balance proof is incomplete")
			}
			if got, want := balance, amount.New(12); got != want {
				t.Errorf("unexpected balance; got: %x, want: %x", got, want)
			}
			value, complete, err := proof.GetState(hash, common.Address{1}, common.Key{2})
			if err != nil {
				t.Fatalf("failed to get state; %s", err)
			}
			if !complete {
				t.Errorf("state proof is incomplete")
			}
			if got, want := value, (common.Value{3}); got != want {
				t.Errorf("unexpected value; got: %x, want: %x", got, want)
			}
		})
	}
}

func TestArchiveTrie_GetDiffProducesValidResults(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}
			defer archive.Close()

			addr1 := common.Address{1}
			addr2 := common.Address{2}
			nonce1 := common.Nonce{1}
			nonce2 := common.Nonce{2}

			err = archive.Add(1, common.Update{
				CreatedAccounts: []common.Address{addr1},
				Nonces:          []common.NonceUpdate{{Account: addr1, Nonce: nonce1}},
			}, nil)
			if err != nil {
				t.Fatalf("failed to create block in archive: %v", err)
			}

			err = archive.Add(3, common.Update{
				CreatedAccounts: []common.Address{addr2},
				Nonces:          []common.NonceUpdate{{Account: addr2, Nonce: nonce2}},
			}, nil)
			if err != nil {
				t.Fatalf("failed to create block in archive: %v", err)
			}

			expectations := []struct {
				from, to uint64
				diff     Diff
			}{
				{0, 0, Diff{}},
				{0, 1, Diff{
					addr1: &AccountDiff{
						Nonce: &nonce1,
						Code:  &emptyCodeHash,
					},
				}},
				{0, 2, Diff{
					addr1: &AccountDiff{
						Nonce: &nonce1,
						Code:  &emptyCodeHash,
					},
				}},
				{0, 3, Diff{
					addr1: &AccountDiff{
						Nonce: &nonce1,
						Code:  &emptyCodeHash,
					},
					addr2: &AccountDiff{
						Nonce: &nonce2,
						Code:  &emptyCodeHash,
					},
				}},
				{1, 1, Diff{}},
				{1, 2, Diff{}},
				{1, 3, Diff{
					addr2: &AccountDiff{
						Nonce: &nonce2,
						Code:  &emptyCodeHash,
					},
				}},

				{2, 2, Diff{}},
				{2, 3, Diff{
					addr2: &AccountDiff{
						Nonce: &nonce2,
						Code:  &emptyCodeHash,
					},
				}},

				// The source can also be after the target.
				{3, 1, Diff{
					addr2: &AccountDiff{Reset: true},
				}},
				{3, 0, Diff{
					addr1: &AccountDiff{Reset: true},
					addr2: &AccountDiff{Reset: true},
				}},
			}

			for _, expectation := range expectations {
				diff, err := archive.GetDiff(expectation.from, expectation.to)
				if err != nil {
					t.Fatalf("failed to produce diff between block %d and %d: %v", expectation.from, expectation.to, err)
				}
				want := expectation.diff
				if !diff.Equal(want) {
					t.Fatalf("unexpected diff between block %d and %d, wanted %v, got %v", expectation.from, expectation.to, want, diff)
				}
			}
		})
	}
}

func TestArchiveTrie_GetDiffDetectsInvalidInput(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}
			defer archive.Close()

			addr1 := common.Address{1}
			addr2 := common.Address{2}
			nonce1 := common.Nonce{1}
			nonce2 := common.Nonce{2}

			err = archive.Add(1, common.Update{
				CreatedAccounts: []common.Address{addr1},
				Nonces:          []common.NonceUpdate{{Account: addr1, Nonce: nonce1}},
			}, nil)
			if err != nil {
				t.Fatalf("failed to create block in archive: %v", err)
			}

			err = archive.Add(3, common.Update{
				CreatedAccounts: []common.Address{addr2},
				Nonces:          []common.NonceUpdate{{Account: addr2, Nonce: nonce2}},
			}, nil)
			if err != nil {
				t.Fatalf("failed to create block in archive: %v", err)
			}

			expectations := []struct {
				from, to     uint64
				errorMessage string
			}{
				{4, 0, "source block 4 not present in archive, highest block is 3"},
				{0, 4, "target block 4 not present in archive, highest block is 3"},
			}

			for _, expectation := range expectations {
				_, err := archive.GetDiff(expectation.from, expectation.to)
				if err == nil {
					t.Errorf("expected operation to fail, but operation passed")
				} else if !strings.Contains(err.Error(), expectation.errorMessage) {
					t.Errorf("unexpected error message, wanted string containing '%s', got '%s'", expectation.errorMessage, err.Error())
				}
			}
		})
	}
}

func TestArchiveTrie_GetDiffForBlockProducesValidResults(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}
			defer archive.Close()

			addr1 := common.Address{1}
			addr2 := common.Address{2}
			nonce1 := common.Nonce{1}
			nonce2 := common.Nonce{2}

			err = archive.Add(0, common.Update{
				CreatedAccounts: []common.Address{addr1},
				Nonces:          []common.NonceUpdate{{Account: addr1, Nonce: nonce1}},
			}, nil)
			if err != nil {
				t.Fatalf("failed to create block in archive: %v", err)
			}

			err = archive.Add(2, common.Update{
				CreatedAccounts: []common.Address{addr2},
				Nonces:          []common.NonceUpdate{{Account: addr2, Nonce: nonce2}},
			}, nil)
			if err != nil {
				t.Fatalf("failed to create block in archive: %v", err)
			}

			expectations := []Diff{
				{
					addr1: &AccountDiff{
						Nonce: &nonce1,
						Code:  &emptyCodeHash,
					},
				},
				{},
				{
					addr2: &AccountDiff{
						Nonce: &nonce2,
						Code:  &emptyCodeHash,
					},
				},
			}

			for block, want := range expectations {
				diff, err := archive.GetDiffForBlock(uint64(block))
				if err != nil {
					t.Fatalf("failed to produce diff for block %d: %v", block, err)
				}
				if !diff.Equal(want) {
					t.Fatalf("unexpected diff for block %d, wanted %v, got %v", block, want, diff)
				}
			}
		})
	}
}

func TestArchiveTrie_GetDiffForBlockDetectsEmptyArchive(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}
			defer archive.Close()

			_, err = archive.GetDiffForBlock(0)
			if err == nil {
				t.Errorf("expected an error when loading diff for block 0 from an empty archive")
			}
		})
	}
}

func TestArchiveTrie_GetMemoryFootprint(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			mf := archive.GetMemoryFootprint()
			if child := mf.GetChild("head"); child == nil {
				t.Errorf("memory footprint not provided")
			}
			if child := mf.GetChild("roots"); child == nil {
				t.Errorf("memory footprint not provided")
			}
		})
	}
}

func TestArchiveTrie_Dump(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			if err = archive.Add(0, common.Update{
				CreatedAccounts: []common.Address{{1}},
			}, nil); err != nil {
				t.Fatalf("cannot apply update: %s", err)
			}

			archive.Dump()
		})
	}
}

func TestArchiveTrie_VerificationOfArchiveWithMissingFileFails(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			err = archive.Add(2, common.Update{
				CreatedAccounts: []common.Address{{1}, {2}},
				Nonces: []common.NonceUpdate{
					{Account: common.Address{1}, Nonce: common.ToNonce(1)},
					{Account: common.Address{2}, Nonce: common.ToNonce(2)},
				},
				Slots: []common.SlotUpdate{
					{Account: common.Address{1}, Key: common.Key{1}, Value: common.Value{3}},
					{Account: common.Address{1}, Key: common.Key{2}, Value: common.Value{2}},
					{Account: common.Address{1}, Key: common.Key{3}, Value: common.Value{1}},
				},
			}, nil)
			if err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			if err := archive.Close(); err != nil {
				t.Fatalf("failed to close archive: %v", err)
			}

			if err := os.Remove(dir + "/branches/freelist.dat"); err != nil {
				t.Fatalf("failed to delete file")
			}

			if err := VerifyArchiveTrie(dir, config, NilVerificationObserver{}); err == nil {
				t.Errorf("missing file should be detected")
			}
		})
	}
}

func TestArchiveTrie_VerificationOfArchiveWithCorruptedFileFails(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			err = archive.Add(2, common.Update{
				CreatedAccounts: []common.Address{{1}, {2}},
				Nonces: []common.NonceUpdate{
					{Account: common.Address{1}, Nonce: common.ToNonce(1)},
					{Account: common.Address{2}, Nonce: common.ToNonce(2)},
				},
				Slots: []common.SlotUpdate{
					{Account: common.Address{1}, Key: common.Key{1}, Value: common.Value{3}},
					{Account: common.Address{1}, Key: common.Key{2}, Value: common.Value{2}},
					{Account: common.Address{1}, Key: common.Key{3}, Value: common.Value{1}},
				},
			}, nil)
			if err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			if err := archive.Close(); err != nil {
				t.Fatalf("failed to close archive: %v", err)
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

			if err := VerifyArchiveTrie(dir, config, NilVerificationObserver{}); err == nil {
				t.Errorf("corrupted file should have been detected")
			}
		})
	}
}

func TestArchiveTrie_CanLoadRootsFromJunkySource(t *testing.T) {

	roots := []Root{
		{NewNodeReference(ValueId(12)), common.Hash{12}},
		{NewNodeReference(ValueId(14)), common.Hash{14}},
	}

	var b bytes.Buffer
	writer := bufio.NewWriter(&b)
	storeRootsTo(writer, roots)
	writer.Flush()

	for _, size := range []int{1, 2, 4, 1024} {
		reader := utils.NewChunkReader(b.Bytes(), size)
		res, err := loadRootsFrom(reader)
		if err != nil {
			t.Fatalf("error loading roots: %v", err)
		}
		if !reflect.DeepEqual(roots, res) {
			t.Errorf("failed to restore roots, wanted %v, got %v", roots, res)
		}
	}
}

func TestArchiveTrie_StoreLoadRoots(t *testing.T) {
	dir := t.TempDir()
	original, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}
	if original.length() != 0 {
		t.Errorf("unexpected number of roots, wanted 0, got %d", original.length())
	}
	for i := 0; i < 48; i++ {
		id := NodeId(uint64(1) << i)
		original.append(Root{NodeRef: NewNodeReference(id)})
		id = NodeId((uint64(1) << (i + 1)) - 1)
		original.append(Root{NodeRef: NewNodeReference(id)})
	}

	if err := original.storeRoots(); err != nil {
		t.Fatalf("failed to store roots: %v", err)
	}

	restored, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}
	if want, got := original.length(), restored.length(); want != got {
		t.Fatalf("invalid number of restored roots, wanted %d, got %d", want, got)
	}

	for i := 0; i < original.length(); i++ {
		want := original.roots[i].NodeRef.Id()
		got := restored.roots[i].NodeRef.Id()
		if want != got {
			t.Errorf("invalid restored root at position %d, wanted %v, got %v", i, want, got)
		}
	}
}

func TestArchiveTrie_RootListStoreOnlyWritesNewRoots(t *testing.T) {
	dir := t.TempDir()
	list, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	// The first write establishes a list of roots in the output file.
	list.append(Root{})
	list.append(Root{})
	list.append(Root{})
	if err := list.storeRoots(); err != nil {
		t.Fatalf("failed to store roots: %v", err)
	}

	// We redirect the incremental update into another file to see what is written.
	oldFile := list.filename
	newFile := filepath.Join(dir, "new-roots.dat")
	list.filename = newFile
	list.append(Root{})
	list.append(Root{})
	if err := list.storeRoots(); err != nil {
		t.Fatalf("failed to store roots: %v", err)
	}

	if err := os.Rename(newFile, oldFile); err != nil {
		t.Fatalf("failed to rename file: %v", err)
	}

	// Loading the second file should only produce 2 roots.
	restored, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}
	if want, got := 2, restored.length(); want != got {
		t.Fatalf("invalid number of restored roots, wanted %d, got %d", want, got)
	}
}

func TestArchiveTrie_IncrementalRootListUpdates(t *testing.T) {
	dir := t.TempDir()
	list, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}
	if list.length() != 0 {
		t.Errorf("unexpected number of roots, wanted 0, got %d", list.length())
	}

	counter := 0
	for i := 0; i < 5; i++ {
		for j := 0; j < 10; j++ {
			id := NodeId(counter)
			list.append(Root{NodeRef: NewNodeReference(id)})
			counter++
		}
		if err := list.storeRoots(); err != nil {
			t.Fatalf("failed to store roots: %v", err)
		}

		restored, err := loadRoots(dir)
		if err != nil {
			t.Fatalf("failed to reload roots: %v", err)
		}

		if !reflect.DeepEqual(list, restored) {
			t.Fatalf("failed to restore roots, wanted %v, got %v", list, restored)
		}
	}
}

func TestArchiveTrie_DirectlyStoredRootsCanBeRestored(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, fileNameArchiveRoots)
	roots := []Root{
		{NewNodeReference(ValueId(12)), common.Hash{12}},
		{NewNodeReference(ValueId(14)), common.Hash{14}},
	}

	if err := StoreRoots(file, roots); err != nil {
		t.Fatalf("failed to store roots: %v", err)
	}
	restored, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}
	if !slices.Equal(roots, restored.roots) {
		t.Errorf("failed to restore roots, wanted %v, got %v", roots, restored.roots)
	}
}

func TestArchiveTrie_FileAccessErrorWhenStoringRootsIsDetected(t *testing.T) {
	dir := t.TempDir()
	list, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}
	if err := list.storeRoots(); err != nil {
		t.Fatalf("failed to store empty roots file: %v", err)
	}

	// remove write access
	if err := os.Chmod(list.filename, 0x400); err != nil {
		t.Fatalf("cannot chmod roots file: %v", err)
	}

	list.append(Root{})
	if err := list.storeRoots(); err == nil {
		t.Errorf("expected an error when storing roots into non-accessible file")
	}
}

func TestRootList_CanParticipateToCheckpointOperations(t *testing.T) {
	dir := t.TempDir()
	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	roots.append(Root{NodeRef: NewNodeReference(ValueId(1))})
	roots.append(Root{NodeRef: NewNodeReference(ValueId(2))})

	if want, got := 2, roots.length(); want != got {
		t.Fatalf("invalid number of roots, wanted %d, got %d", want, got)
	}

	coordinator, err := checkpoint.NewCoordinator(t.TempDir(), roots)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	_, err = coordinator.CreateCheckpoint()
	if err != nil {
		t.Fatalf("failed to create checkpoint: %v", err)
	}

	roots.append(Root{NodeRef: NewNodeReference(ValueId(3))})
	if err := roots.storeRoots(); err != nil {
		t.Fatalf("failed to store roots: %v", err)
	}

	if want, got := 3, roots.length(); want != got {
		t.Fatalf("invalid number of roots, wanted %d, got %d", want, got)
	}
}

func TestArchiveTrie_RecreateAccount_ClearStorage(t *testing.T) {
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			archive, err := OpenArchiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to open empty archive: %v", err)
			}

			addr := common.Address{0xA}
			key := common.Key{0xB}
			val := common.Value{0xC}

			// create an account with a non-empty slot
			update := common.Update{}
			update.AppendCreateAccount(addr)
			update.AppendSlotUpdate(addr, key, val)
			if err := archive.Add(0, update, nil); err != nil {
				t.Errorf("cannot add update: %s", err)
			}

			// re-create an account in the next block
			update = common.Update{}
			update.AppendCreateAccount(addr)
			if err := archive.Add(1, update, nil); err != nil {
				t.Errorf("cannot add update: %s", err)
			}

			// verify that the account is re-created with an empty slot
			exists, err := archive.Exists(1, addr)
			if err != nil {
				t.Errorf("cannot check account existence: %s", err)
			}
			if !exists {
				t.Errorf("account does not exist")
			}
			storage, err := archive.GetStorage(1, addr, key)
			if err != nil {
				t.Errorf("cannot get slot value: %s", err)
			}

			var empty common.Value
			if storage != empty {
				t.Errorf("value is not empty, but it is: %v", storage)
			}

			if err := archive.Close(); err != nil {
				t.Fatalf("failed to close empty archive: %v", err)
			}
		})
	}
}

func TestArchiveTrie_QueryLoadTest(t *testing.T) {
	// Goal: stress-test an archive with a limited node cache.
	archive, err := OpenArchiveTrie(t.TempDir(), S5ArchiveConfig, NodeCacheConfig{Capacity: 30_000}, ArchiveConfig{})
	if err != nil {
		t.Fatalf("failed to create archive: %v", err)
	}

	// We fill the archive with N blocks, each with N accounts and N slots.
	const N = 100
	for b := 0; b < N; b++ {
		update := common.Update{}
		for a := 0; a < N; a++ {
			addr := common.Address{byte(a)}
			if b == 0 {
				update.AppendCreateAccount(addr)
			}
			for k := 0; k < N; k++ {
				update.AppendSlotUpdate(addr, common.Key{byte(k)}, common.Value{byte(b), byte(a), byte(k)})
			}
		}
		if err := archive.Add(uint64(b), update, nil); err != nil {
			t.Errorf("failed to add update to archive: %v", err)
		}
	}

	// In a second step, random queries are send concurrently into the archive.
	const Q = 10_000
	P := runtime.NumCPU()
	var wg sync.WaitGroup
	wg.Add(P)
	for i := 0; i < P; i++ {
		go func(seed int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(seed)))
			for i := 0; i < Q; i++ {
				block := uint64(r.Intn(N))
				addr := common.Address{byte(r.Intn(N))}
				key := common.Key{byte(r.Intn(N))}

				live, err := archive.getView(block)
				if err != nil {
					t.Errorf("failed to get view to block %d: %v", block, err)
					continue
				}
				value, err := live.GetValue(addr, key)
				if err != nil {
					t.Errorf("failed to get value from archive: %d/%v/%v: %v", block, addr, key, err)
				}
				if want, got := (common.Value{byte(block), addr[0], key[0]}), value; want != got {
					t.Errorf("wrong result for lookup %d/%v/%v: wanted %v, got %v", block, addr, key, want, got)
				}
			}
		}(i)
	}
	wg.Wait()

	if err := archive.Close(); err != nil {
		t.Fatalf("failed to close archive: %v", err)
	}
}

func TestStoreRootsTo_WriterFailures(t *testing.T) {
	var roots []Root
	for i := 0; i < 48; i++ {
		id := NodeId(uint64(1) << i)
		roots = append(roots, Root{NodeRef: NewNodeReference(id)})
	}

	var injectedErr = errors.New("write error")
	ctrl := gomock.NewController(t)
	osfile := utils.NewMockOsFile(ctrl)
	osfile.EXPECT().Write(gomock.Any()).Return(0, injectedErr)

	if err := storeRootsTo(osfile, roots); !errors.Is(err, injectedErr) {
		t.Errorf("writing roots should fail")
	}
}

func TestStoreRootsTo_SecondWriterFailures(t *testing.T) {
	var roots []Root
	for i := 0; i < 48; i++ {
		id := NodeId(uint64(1) << i)
		roots = append(roots, Root{NodeRef: NewNodeReference(id)})
	}

	var injectedErr = errors.New("write error")
	ctrl := gomock.NewController(t)
	osfile := utils.NewMockOsFile(ctrl)
	gomock.InOrder(
		osfile.EXPECT().Write(gomock.Any()).Return(0, nil),
		osfile.EXPECT().Write(gomock.Any()).Return(0, injectedErr),
	)

	if err := storeRootsTo(osfile, roots); !errors.Is(err, injectedErr) {
		t.Errorf("writing roots should fail")
	}
}

func TestStoreRoots_Cannot_Create(t *testing.T) {
	var roots []Root
	dir := t.TempDir()
	file := filepath.Join(dir, fileNameArchiveRootsCheckpointDirectory)
	if err := os.Mkdir(file, os.FileMode(0644)); err != nil {
		t.Fatalf("cannot create dir: %s", err)
	}
	if err := StoreRoots(file, roots); err == nil {
		t.Errorf("writing roots should fail")
	}

}

func TestArchiveTrie_FailingGetterOperation_InvalidatesArchive(t *testing.T) {
	injectedErr := fmt.Errorf("injectedError")

	rotate := func(arr []string, k int) []string {
		k = k % len(arr)
		cp := make([]string, len(arr))
		copy(cp, arr)
		return append(cp[k:], cp[:k]...)
	}

	names := maps.Keys(archiveGetters)

	// rotate getters to start the experiment from all existing getters.
	for i := 0; i < len(archiveGetters); i++ {
		i := i
		t.Run(fmt.Sprintf("rotation_%d", i), func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			db := NewMockDatabase(ctrl)
			db.EXPECT().Freeze(gomock.Any())
			db.EXPECT().CheckAll(gomock.Any())
			db.EXPECT().GetAccountInfo(gomock.Any(), gomock.Any()).Return(AccountInfo{}, false, injectedErr).MaxTimes(1)
			db.EXPECT().GetValue(gomock.Any(), gomock.Any(), gomock.Any()).Return(common.Value{}, injectedErr).MaxTimes(1)

			archive, err := OpenArchiveTrie(t.TempDir(), S4ArchiveConfig, NodeCacheConfig{Capacity: 1000}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("cannot open archive: %v", err)
			}
			archive.forest = db // inject mock

			if err := archive.Add(2, common.Update{CreatedAccounts: []common.Address{{0xA}}}, nil); err != nil {
				t.Fatalf("failed to update archive: %v", err)
			}

			// all operations must fail
			for _, name := range rotate(names, i) {
				if got, want := archiveGetters[name](archive), injectedErr; !errors.Is(got, want) {
					t.Errorf("expected error does not match: %v != %v for op: %s", got, want, name)
				}
			}

			// adding an update as well as all flush and close checks must fail
			update := common.Update{
				CreatedAccounts: []common.Address{{0xB}},
			}

			if err := archive.Add(0, update, nil); !errors.Is(err, injectedErr) {
				t.Errorf("expected failure did not happen: got: %v != want: %v", err, injectedErr)
			}
			if err := archive.CheckErrors(); !errors.Is(err, injectedErr) {
				t.Errorf("check should fail")
			}
			if err := archive.Check(); !errors.Is(err, injectedErr) {
				t.Errorf("check should fail")
			}
			if err := archive.Flush(); !errors.Is(err, injectedErr) {
				t.Errorf("check should fail")
			}
			if err := archive.Close(); !errors.Is(err, injectedErr) {
				t.Errorf("check should fail")
			}
		})
	}
}

func TestArchiveTrie_FailingLiveStateUpdate_InvalidatesArchive(t *testing.T) {
	injectedErr := fmt.Errorf("injectedError")

	liveStateOps := []struct {
		name            string
		addExpectations func(db *MockLiveState, injectedErr error)
	}{{"DeleteAccount", func(db *MockLiveState, injectedErr error) {
		db.EXPECT().DeleteAccount(gomock.Any()).Return(injectedErr)
	},
	}, {"CreateAccount", func(db *MockLiveState, injectedErr error) {
		db.EXPECT().CreateAccount(gomock.Any()).Return(injectedErr)
	},
	}, {"SetBalance", func(db *MockLiveState, injectedErr error) {
		db.EXPECT().SetBalance(gomock.Any(), gomock.Any()).Return(injectedErr)
	},
	}, {"SetNonce", func(db *MockLiveState, injectedErr error) {
		db.EXPECT().SetNonce(gomock.Any(), gomock.Any()).Return(injectedErr)
	},
	}, {"SetCode", func(db *MockLiveState, injectedErr error) {
		db.EXPECT().SetCode(gomock.Any(), gomock.Any()).Return(injectedErr)
	},
	}, {"SetStorage", func(db *MockLiveState, injectedErr error) {
		db.EXPECT().SetStorage(gomock.Any(), gomock.Any(), gomock.Any()).Return(injectedErr)
	},
	}, {"UpdateHashes", func(db *MockLiveState, injectedErr error) {
		db.EXPECT().UpdateHashes().Return(common.Hash{}, nil, injectedErr)
	},
	},
	}

	for i, liveStateOp := range liveStateOps {
		i := i
		t.Run(fmt.Sprintf("liveOp_%s", liveStateOp.name), func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			liveState := NewMockLiveState(ctrl)
			liveState.EXPECT().Flush().AnyTimes()
			liveState.EXPECT().closeWithError(gomock.Any())
			liveState.EXPECT().Root().AnyTimes()

			db := NewMockDatabase(ctrl)
			db.EXPECT().Freeze(gomock.Any()).AnyTimes()
			db.EXPECT().CheckAll(gomock.Any()).AnyTimes()

			archive, err := OpenArchiveTrie(t.TempDir(), S4ArchiveConfig, NodeCacheConfig{Capacity: 1000}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("cannot open archive: %v", err)
			}
			archive.head = liveState
			archive.forest = db

			// mock up to the current loop
			for j, liveOp := range liveStateOps {
				if j == i {
					liveOp.addExpectations(liveState, injectedErr)
					break
				} else {
					liveOp.addExpectations(liveState, nil)
				}
			}

			// trigger error from the livedb
			update := common.Update{
				DeletedAccounts: []common.Address{{0xA}},
				CreatedAccounts: []common.Address{{0xB}},
				Balances:        []common.BalanceUpdate{{common.Address{0xA}, amount.New(1)}},
				Nonces:          []common.NonceUpdate{{common.Address{0xA}, common.Nonce{0x1}}},
				Codes:           []common.CodeUpdate{{common.Address{0xA}, []byte{0x1}}},
				Slots:           []common.SlotUpdate{{common.Address{0xA}, common.Key{0xB}, common.Value{0x1}}},
			}
			if err := archive.Add(0, update, nil); !errors.Is(err, injectedErr) {
				t.Errorf("expected failure did not happen: got: %v != want: %v", err, injectedErr)
			}

			// all getters must fail ¨
			for name, getter := range archiveGetters {
				if err := getter(archive); !errors.Is(err, injectedErr) {
					t.Errorf("expected error does not match: %v != %v for op: %s", err, injectedErr, name)
				}
			}

			if err := archive.Check(); !errors.Is(err, injectedErr) {
				t.Errorf("check should fail")
			}
			if err := archive.CheckErrors(); !errors.Is(err, injectedErr) {
				t.Errorf("check should fail")
			}
			if err := archive.Flush(); !errors.Is(err, injectedErr) {
				t.Errorf("check should fail")
			}
			if err := archive.Close(); !errors.Is(err, injectedErr) {
				t.Errorf("check should fail")
			}
		})
	}
}

var archiveGetters = map[string]func(archive archive.Archive) error{
	"exists": func(archive archive.Archive) error {
		_, err := archive.Exists(uint64(0), common.Address{})
		return err
	},
	"balance": func(archive archive.Archive) error {
		_, err := archive.GetBalance(uint64(0), common.Address{})
		return err
	},
	"code": func(archive archive.Archive) error {
		_, err := archive.GetCode(uint64(0), common.Address{})
		return err
	},
	"nonce": func(archive archive.Archive) error {
		_, err := archive.GetNonce(uint64(0), common.Address{})
		return err
	},
	"storage": func(archive archive.Archive) error {
		_, err := archive.GetStorage(uint64(0), common.Address{}, common.Key{})
		return err
	},
}

func TestArchiveTrie_VisitTrie_CorrectDataIsVisited(t *testing.T) {
	addr := common.Address{1}

	tests := []struct {
		name         string
		visitedBlock uint64
		visitFunc    func(Node, NodeInfo) bool
	}{
		{
			name:         "empty-block",
			visitedBlock: 0,
			visitFunc: func(node Node, info NodeInfo) bool {
				switch node.(type) {
				case EmptyNode:
					return true
				}
				return false
			},
		},
		{
			name:         "filled-block",
			visitedBlock: 1,
			visitFunc: func(node Node, info NodeInfo) bool {
				switch n := node.(type) {
				case *AccountNode:
					a := n.Address()
					if a != addr {
						t.Fatalf("unexpected address node, got: %s, want: %s", a, addr)
					}
					return true
				}
				return false
			},
		},
	}

	for _, test := range tests {
		for _, config := range allMptConfigs {
			t.Run(config.Name+" "+test.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)

				archive, err := OpenArchiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
				if err != nil {
					t.Fatalf("failed to open empty archive: %v", err)
				}
				defer archive.Close()

				err = archive.Add(1, common.Update{
					CreatedAccounts: []common.Address{addr},
					Nonces: []common.NonceUpdate{
						{Account: addr, Nonce: common.ToNonce(1)},
					},
				}, nil)
				if err != nil {
					t.Fatalf("failed to add update: %v", err)
				}

				var found bool

				nodeVisitor := NewMockNodeVisitor(ctrl)
				nodeVisitor.EXPECT().Visit(gomock.Any(), gomock.Any()).Do(func(node Node, info NodeInfo) {
					if test.visitFunc(node, info) {
						found = true
						return
					}
				}).MinTimes(1)

				err = archive.VisitTrie(test.visitedBlock, nodeVisitor)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if !found {
					t.Error("node not found")
				}
			})
		}
	}
}

func TestArchiveTrie_VisitTrie_InvalidBlock(t *testing.T) {
	for _, config := range allMptConfigs {

		t.Run(config.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			nodeVisitor := NewMockNodeVisitor(ctrl)

			archive, err := OpenArchiveTrie(t.TempDir(), config, NodeCacheConfig{Capacity: 1024}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to open empty archive: %v", err)
			}
			defer archive.Close()

			addr := common.Address{1}

			err = archive.Add(0, common.Update{
				CreatedAccounts: []common.Address{addr},
				Nonces: []common.NonceUpdate{
					{Account: addr, Nonce: common.ToNonce(1)},
				},
			}, nil)
			if err != nil {
				t.Fatalf("failed to add update: %v", err)
			}

			err = archive.VisitTrie(1, nodeVisitor)
			if err == nil {
				t.Fatal("error is expected")
			}

			if got, want := err.Error(), fmt.Sprintf("invalid block: %d >= %d", 1, 1); !strings.EqualFold(got, want) {
				t.Errorf("unexpected error, got: %v, want: %v", got, want)
			}

		})
	}
}

func TestArchiveTrie_createCheckpoint_forwardsErrors(t *testing.T) {
	tests := map[string]func(archive *ArchiveTrie) error{
		"failing flush": func(archive *ArchiveTrie) error {
			// Registering a pre-observed error causes the flush operation to fail.
			archive.addError(fmt.Errorf("injected error"))
			return nil
		},
		"out-of-sync components": func(archive *ArchiveTrie) error {
			cp := checkpoint.Checkpoint(1)
			return errors.Join(
				archive.roots.Prepare(cp),
				archive.roots.Commit(cp),
			)
		},
	}

	for name, sabotage := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1000}, ArchiveConfig{})
			if err != nil {
				t.Fatalf("cannot open archive: %v", err)
			}
			defer archive.Close()

			if err := sabotage(archive); err != nil {
				t.Fatalf("failed to sabotage archive: %v", err)
			}

			if err := archive.createCheckpoint(); err == nil {
				t.Errorf("expected checkpoint creation to fail")
			}
		})
	}
}

func TestArchiveTrie_GetCheckpointBlock(t *testing.T) {
	dir := t.TempDir()

	subDir := filepath.Join(dir, fileNameArchiveRootsCheckpointDirectory)
	if err := os.Mkdir(subDir, 0700); err != nil {
		t.Fatalf("failed to create sub-directory: %v", err)
	}
	checkpointFile := filepath.Join(subDir, fileNameArchiveRootsCommittedCheckpoint)
	if err := utils.WriteJsonFile(checkpointFile, rootListCheckpointData{NumRoots: 42}); err != nil {
		t.Fatalf("failed to write checkpoint file: %v", err)
	}

	block, err := GetCheckpointBlock(dir)
	if err != nil {
		t.Fatalf("failed to get checkpoint block: %v", err)
	}
	if want, got := uint64(41), block; want != got {
		t.Errorf("unexpected checkpoint block, got: %d, want: %d", got, want)
	}
}

func TestArchiveTrie_GetCheckpointBlock_CorruptedCoordinatorIsDetected(t *testing.T) {
	dir := t.TempDir()

	// trigger error when creating coordinator by blocking the needed
	// directory location with a file.
	coordinatorDirectory := filepath.Join(dir, fileNameArchiveCheckpointDirectory)
	if err := os.WriteFile(coordinatorDirectory, []byte("corrupted"), 0600); err != nil {
		t.Fatalf("failed to write coordinator file: %v", err)
	}

	if _, err := GetCheckpointBlock(dir); err == nil {
		t.Fatalf("expected error when checkpoint file is corrupted")
	}
}

func TestArchiveTrie_GetCheckpointBlock_MissingFileIsDetected(t *testing.T) {
	dir := t.TempDir()
	if _, err := GetCheckpointBlock(dir); err == nil {
		t.Fatalf("expected error when checkpoint file is missing")
	}
}

func TestArchiveTrie_RestoreBlockHeight(t *testing.T) {

	addBlocks := func(archive *ArchiveTrie, from int, to int) error {
		for i := from; i < to; i++ {
			err := archive.Add(uint64(i), common.Update{
				CreatedAccounts: []common.Address{{byte(i)}},
				Nonces: []common.NonceUpdate{
					{Account: common.Address{byte(i)}, Nonce: common.Nonce{byte(i)}},
				},
				Codes: []common.CodeUpdate{
					{Account: common.Address{byte(i)}, Code: []byte{byte(i)}},
				},
			}, nil)
			if err != nil {
				return err
			}
		}
		return nil
	}

	tests := map[string]func(*ArchiveTrie) error{
		"clean_close": func(archive *ArchiveTrie) error {
			return archive.Close()
		},
		"clean_close_with_extra_blocks": func(archive *ArchiveTrie) error {
			return errors.Join(
				addBlocks(archive, 92, 98),
				archive.Close(),
			)
		},
		"no_close": func(archive *ArchiveTrie) error {
			// In order to allow the recovery to access the directory, the lock needs to be released
			return archive.head.(*MptState).lock.Release()
		},
		"no_close_with_extra_blocks": func(archive *ArchiveTrie) error {
			return errors.Join(
				addBlocks(archive, 92, 98),
				archive.head.(*MptState).lock.Release(),
			)
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()

			// Create an archive and fill it with blocks.
			{
				archive, err := OpenArchiveTrie(
					dir, S5ArchiveConfig,
					NodeCacheConfig{
						Capacity: 1000,
					},
					ArchiveConfig{
						CheckpointInterval: 10,
					},
				)
				if err != nil {
					t.Fatalf("cannot open archive: %v", err)
				}

				if err := addBlocks(archive, 0, 92); err != nil {
					t.Fatalf("failed to add blocks: %v", err)
				}

				// check that the archive has a checkpoint at block 90
				checkpoint, err := GetCheckpointBlock(dir)
				if err != nil {
					t.Fatalf("failed to get checkpoint block: %v", err)
				}
				if want, got := uint64(90), checkpoint; want != got {
					t.Errorf("unexpected checkpoint block, wanted %d, got %d", want, got)
				}

				if err := test(archive); err != nil {
					t.Fatalf("failed to close archive: %v", err)
				}
			}

			// The archive can not be reset to a block after the last checkpoint.
			if err := RestoreBlockHeight(dir, S5ArchiveConfig, 91); err == nil {
				t.Fatalf("expected error when restoring to block after last checkpoint")
			}

			// Blocks older than the last checkpoint can be restored.
			lastBlock := uint64(92)
			for _, block := range []uint64{90, 89, 85, 85, 47} {
				if err := RestoreBlockHeight(dir, S5ArchiveConfig, block); err != nil {
					t.Fatalf("failed to restore block height %d: %v", block, err)
				}

				// Check that the archive can be verified.
				if err := VerifyArchiveTrie(dir, S5ArchiveConfig, nil); err != nil {
					t.Fatalf("failed to verify archive after reset to block %d: %v", block, err)
				}

				// Check that the correct block has been restored.
				archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1000}, ArchiveConfig{})
				if err != nil {
					t.Fatalf("failed to open archive: %v", err)
				}
				height, _, err := archive.GetBlockHeight()
				if err != nil {
					t.Fatalf("failed to get block height: %v", err)
				}
				if want, got := block, height; want != got {
					t.Fatalf("unexpected block height, wanted %d, got %d", want, got)
				}
				if err := archive.Close(); err != nil {
					t.Fatalf("failed to close archive: %v", err)
				}

				lastBlock = block
			}

			// Check that restored archive can be opened again.
			{
				archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1000}, ArchiveConfig{})
				if err != nil {
					t.Fatalf("cannot open archive: %v", err)
				}

				block, _, err := archive.GetBlockHeight()
				if err != nil {
					t.Fatalf("cannot get block height: %v", err)
				}
				if want, got := lastBlock, block; want != got {
					t.Errorf("unexpected block height, wanted %d, got %d", want, got)
				}

				// check that the archive has a checkpoint at block 90
				checkpoint, err := GetCheckpointBlock(dir)
				if err != nil {
					t.Fatalf("failed to get checkpoint block: %v", err)
				}
				if want, got := uint64(47), checkpoint; want != got {
					t.Errorf("unexpected checkpoint block, wanted %d, got %d", want, got)
				}

				// additional blocks can be added
				if err := addBlocks(archive, int(lastBlock+2), int(lastBlock+7)); err != nil {
					t.Fatalf("failed to add blocks: %v", err)
				}

				if err := archive.Close(); err != nil {
					t.Fatalf("failed to close archive: %v", err)
				}
			}

			// Check that the restored and extended archive can be verified.
			if err := VerifyArchiveTrie(dir, S5ArchiveConfig, nil); err != nil {
				t.Fatalf("failed to verify archive: %v", err)
			}

		})
	}
}

func TestArchiveTrie_RestoreBlockHeightFailsOnEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := RestoreBlockHeight(dir, S5ArchiveConfig, 0); err == nil {
		t.Fatalf("expected error when restoring block height in empty directory")
	}
}

func TestArchiveTrie_RestoreBlockHeightFailsOnBlockBeyondTheLastCheckpoint(t *testing.T) {
	dir := t.TempDir()
	archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1000}, ArchiveConfig{
		CheckpointInterval: 10,
	})
	if err != nil {
		t.Fatalf("cannot open archive: %v", err)
	}

	if err := archive.Add(10, common.Update{}, nil); err != nil {
		t.Fatalf("failed to add update: %v", err)
	}

	if err := archive.Add(15, common.Update{}, nil); err != nil {
		t.Fatalf("failed to add update: %v", err)
	}

	cpHeight, err := GetCheckpointBlock(dir)
	if err != nil || cpHeight != 10 {
		t.Fatalf("unexpected checkpoint block: %d, %v", cpHeight, err)
	}

	if err := archive.Close(); err != nil {
		t.Fatalf("failed to close archive: %v", err)
	}

	for _, block := range []uint64{11, 12, 15, 16, 20, 123} {
		if err := RestoreBlockHeight(dir, S5ArchiveConfig, block); err == nil || !strings.Contains(err.Error(), "beyond the last checkpoint") {
			t.Fatalf("expected error when restoring block height beyond the last checkpoint, got %v", err)
		}
	}
}

func TestArchiveTrie_RestoreBlockHeight_DetectsIssuesAndForwardsThose(t *testing.T) {
	tests := map[string]struct {
		sabotage                     func(t *testing.T, dir string) error
		expectedErrorMessageFragment string
		directoryShouldBeDirtyAfter  bool
	}{
		"directory in use": {
			sabotage: func(_ *testing.T, dir string) error {
				_, err := LockDirectory(dir)
				return err
			},
			expectedErrorMessageFragment: "exclusive access",
		},
		"unable to determine num roots in checkpoint": {
			sabotage: func(_ *testing.T, dir string) error {
				return os.Remove(filepath.Join(dir,
					fileNameArchiveRootsCheckpointDirectory,
					fileNameArchiveRootsCommittedCheckpoint,
				))
			},
			expectedErrorMessageFragment: "failed to get checkpoint height",
		},
		"failed checkpoint restore": {
			sabotage: func(_ *testing.T, dir string) error {
				// The checkpoint recovery fails when the checkpoint data is corrupted.
				rootsFile := filepath.Join(dir, fileNameArchiveRoots)
				return os.WriteFile(rootsFile, []byte("invalid data"), 0644)
			},
			expectedErrorMessageFragment: "failed to restore checkpoint",
			directoryShouldBeDirtyAfter:  true,
		},
		"missing permissions": {
			sabotage: func(t *testing.T, dir string) error {
				t.Cleanup(func() {
					_ = os.Chmod(dir, 0700)
				})
				return os.Chmod(dir, 0500)
			},
		},
		"fail to mark as dirty": {
			sabotage: func(t *testing.T, dir string) error {
				return os.Mkdir(filepath.Join(dir, dirtyFileName), 0700)
			},
			expectedErrorMessageFragment: "failed to mark directory",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1000}, ArchiveConfig{
				CheckpointInterval: 10,
			})
			if err != nil {
				t.Fatalf("cannot open archive: %v", err)
			}

			if err := archive.Add(10, common.Update{}, nil); err != nil {
				t.Fatalf("failed to add update: %v", err)
			}

			if err := archive.Add(15, common.Update{}, nil); err != nil {
				t.Fatalf("failed to add update: %v", err)
			}
			if err := archive.Close(); err != nil {
				t.Fatalf("failed to close archive: %v", err)
			}

			if err := test.sabotage(t, dir); err != nil {
				t.Fatalf("failed to sabotage archive: %v", err)
			}

			if err := RestoreBlockHeight(dir, S5ArchiveConfig, 10); err == nil || !strings.Contains(err.Error(), test.expectedErrorMessageFragment) {
				t.Fatalf("expected error containing \"%s\", got %v", test.expectedErrorMessageFragment, err)
			}

			dirty, err := isDirty(dir)
			if err != nil {
				t.Fatalf("failed to check if directory is dirty: %v", err)
			}
			if want, got := test.directoryShouldBeDirtyAfter, dirty; want != got {
				t.Errorf("expected dirty state of directory to be %t, got %t", want, got)
			}
		})
	}
}

func TestArchiveTrie_RestoredTrieCanBeReused(t *testing.T) {
	// This test creates an archive with 100 blocks, reset it to 50,
	// and then adds 100 new blocks on top. In the end it checks that
	// the archive contains the correct data for each block.

	dir := t.TempDir()
	archive, err := OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1000}, ArchiveConfig{
		CheckpointInterval: 10,
	})
	if err != nil {
		t.Fatalf("cannot open archive: %v", err)
	}

	counter := 0
	address := common.Address{}
	for i := 0; i < 100; i++ {
		counter++
		err := archive.Add(uint64(i), common.Update{
			CreatedAccounts: []common.Address{address},
			Nonces: []common.NonceUpdate{
				{Account: address, Nonce: common.Nonce{byte(counter)}},
			},
			Codes: []common.CodeUpdate{
				{Account: address, Code: []byte{byte(counter)}},
			},
		}, nil)
		if err != nil {
			t.Fatalf("failed to add update: %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		nonce, err := archive.GetNonce(uint64(i), address)
		if err != nil {
			t.Fatalf("failed to get nonce: %v", err)
		}
		if want, got := (common.Nonce{byte(i + 1)}), nonce; want != got {
			t.Errorf("unexpected nonce, wanted %d, got %d", want, got)
		}
		code, err := archive.GetCode(uint64(i), address)
		if err != nil {
			t.Fatalf("failed to get nonce: %v", err)
		}
		if want, got := []byte{byte(i + 1)}, code; !bytes.Equal(want, got) {
			t.Errorf("unexpected nonce, wanted %d, got %d", want, got)
		}
	}

	if err := archive.Close(); err != nil {
		t.Fatalf("failed to close archive: %v", err)
	}

	if err := RestoreBlockHeight(dir, S5ArchiveConfig, 50); err != nil {
		t.Fatalf("failed to restore block height: %v", err)
	}

	archive, err = OpenArchiveTrie(dir, S5ArchiveConfig, NodeCacheConfig{Capacity: 1000}, ArchiveConfig{
		CheckpointInterval: 10,
	})
	if err != nil {
		t.Fatalf("cannot open archive: %v", err)
	}

	for i := 51; i < 150; i++ {
		counter++
		err := archive.Add(uint64(i), common.Update{
			CreatedAccounts: []common.Address{address},
			Nonces: []common.NonceUpdate{
				{Account: address, Nonce: common.Nonce{byte(counter)}},
			},
			Codes: []common.CodeUpdate{
				{Account: address, Code: []byte{byte(counter)}},
			},
		}, nil)
		if err != nil {
			t.Fatalf("failed to add update: %v", err)
		}
	}

	for i := 0; i < 150; i++ {
		nonce, err := archive.GetNonce(uint64(i), address)
		if err != nil {
			t.Fatalf("failed to get nonce: %v", err)
		}
		want := byte(i + 1)
		if i > 50 {
			want += 49 // 49 blocks got removed during the reset
		}
		if want, got := (common.Nonce{want}), nonce; want != got {
			t.Errorf("unexpected nonce, wanted %d, got %d", want, got)
		}
		code, err := archive.GetCode(uint64(i), address)
		if err != nil {
			t.Fatalf("failed to get nonce: %v", err)
		}
		if want, got := []byte{want}, code; !bytes.Equal(want, got) {
			t.Errorf("unexpected nonce, wanted %d, got %d", want, got)
		}
	}

	if err := archive.Close(); err != nil {
		t.Fatalf("failed to close archive: %v", err)
	}
}

func TestRootList_LoadRoots_ForwardsIoIssues(t *testing.T) {
	tests := map[string]func(t *testing.T, dir string) error{
		"fail to create directory": func(t *testing.T, dir string) error {
			// this case creates a file with the given directory name,
			// making it impossible to re-create the directory.
			if err := os.RemoveAll(dir); err != nil {
				return err
			}
			t.Cleanup(func() {
				os.Remove(dir)
			})
			return os.WriteFile(dir, []byte{}, 0700)
		},
		"corrupted checkpoint file": func(_ *testing.T, dir string) error {
			return utils.WriteJsonFile(
				filepath.Join(dir,
					fileNameArchiveRootsCheckpointDirectory,
					fileNameArchiveRootsCommittedCheckpoint,
				), "corrupted data")
		},
	}

	for name, sabotage := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			roots, err := loadRoots(dir)
			if err != nil {
				t.Fatalf("failed to load roots: %v", err)
			}
			roots.append(Root{NodeRef: NewNodeReference(ValueId(1))})

			cp := checkpoint.Checkpoint(1)
			err = errors.Join(
				roots.Prepare(cp),
				roots.Commit(cp),
				roots.storeRoots(),
			)
			if err != nil {
				t.Fatalf("failed to prepare test list for roots: %v", err)
			}

			if err := sabotage(t, dir); err != nil {
				t.Fatalf("failed to sabotage roots list: %v", err)
			}

			if _, err := loadRoots(dir); err == nil {
				t.Fatalf("expected error when loading roots")
			}
		})
	}
}

func TestRootList_storeRootsTo_HandlesWriteIssues(t *testing.T) {
	ctrl := gomock.NewController(t)
	writeCounter := utils.NewMockOsFile(ctrl)

	roots := make([]Root, 10)

	counter := 0
	writeCounter.EXPECT().Write(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
		counter++
		return len(p), nil
	}).AnyTimes()

	if err := storeRootsTo(writeCounter, roots); err != nil {
		t.Fatalf("failed to store roots: %v", err)
	}

	if counter == 0 {
		t.Fatalf("expected write to be called")
	}

	for i := 0; i < counter; i++ {
		t.Run(fmt.Sprintf("write_%d", i), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			out := utils.NewMockOsFile(ctrl)

			inducedError := fmt.Errorf("induced error")
			gomock.InOrder(
				out.EXPECT().Write(gomock.Any()).Return(0, nil).Times(i),
				out.EXPECT().Write(gomock.Any()).Return(0, inducedError),
			)

			if err := storeRootsTo(out, roots); !errors.Is(err, inducedError) {
				t.Fatalf("expected error when writing roots")
			}
		})
	}
}

func TestRootList_GuaranteeCheckpoint_EmptyListSupportsCheckpointZero(t *testing.T) {
	dir := t.TempDir()
	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	if err := roots.GuaranteeCheckpoint(checkpoint.Checkpoint(0)); err != nil {
		t.Fatalf("failed to guarantee checkpoint: %v", err)
	}
}

func TestRootList_GuaranteeCheckpoint_CreatedCheckpointsCanBeGuaranteed(t *testing.T) {
	dir := t.TempDir()
	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	for i := 0; i < 10; i++ {
		cp := checkpoint.Checkpoint(i + 1)
		err := errors.Join(
			roots.Prepare(cp),
			roots.Commit(cp),
		)
		if err != nil {
			t.Fatalf("failed to create new checkpoint: %v", err)
		}
		if err := roots.GuaranteeCheckpoint(cp); err != nil {
			t.Fatalf("failed to guarantee checkpoint: %v", err)
		}
	}
}

func TestRootList_GuaranteeCheckpoint_FailsForNonExistingCheckpoint(t *testing.T) {
	dir := t.TempDir()
	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	for i := 0; i < 10; i++ {
		cp := checkpoint.Checkpoint(i + 1)
		err := errors.Join(
			roots.Prepare(cp),
			roots.Commit(cp),
		)
		if err != nil {
			t.Fatalf("failed to create new checkpoint: %v", err)
		}
	}

	// should not work for outdated checkpoint
	cp := checkpoint.Checkpoint(9)
	if err := roots.GuaranteeCheckpoint(cp); err == nil {
		t.Fatalf("expected error when checking outdated checkpoint")
	}

	// should also fail for future checkpoint
	cp = checkpoint.Checkpoint(11)
	if err := roots.GuaranteeCheckpoint(cp); err == nil {
		t.Fatalf("expected error when checking future checkpoint")
	}
}

func TestRootList_GuaranteeCheckpoint_CommitsPendingCheckpoint(t *testing.T) {
	dir := t.TempDir()
	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	cp0 := checkpoint.Checkpoint(0)
	cp1 := checkpoint.Checkpoint(1)
	if err := roots.Prepare(cp1); err != nil {
		t.Fatalf("failed to prepare checkpoint: %v", err)
	}

	if want, got := cp0, roots.checkpoint; want != got {
		t.Fatalf("unexpected checkpoint, wanted %v, got %v", want, got)
	}

	if err := roots.GuaranteeCheckpoint(cp1); err != nil {
		t.Fatalf("failed to guarantee checkpoint: %v", err)
	}

	if want, got := cp1, roots.checkpoint; want != got {
		t.Fatalf("unexpected checkpoint, wanted %v, got %v", want, got)
	}
}

func TestRootList_Prepare_OnlyAcceptsIncrementalCheckpoints(t *testing.T) {
	dir := t.TempDir()
	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	cp := checkpoint.Checkpoint(2)
	if err := roots.Prepare(cp); err == nil {
		t.Fatalf("expected error when preparing non-incremental checkpoint")
	}
}

func TestRootList_Prepare_FailsOnIOError(t *testing.T) {
	dir := t.TempDir()
	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	prepareFile := filepath.Join(dir, fileNameArchiveRootsCheckpointDirectory, fileNameArchiveRootsPreparedCheckpoint)
	if err := os.Mkdir(prepareFile, 0700); err != nil {
		t.Fatalf("failed to create prepare directory blocking prepare file: %v", err)
	}

	cp := checkpoint.Checkpoint(1)
	if err := roots.Prepare(cp); err == nil {
		t.Fatalf("expected error when prepare file cannot be created")
	}
}

func TestRootList_Commit_OnlyAcceptsIncrementalCheckpoints(t *testing.T) {
	dir := t.TempDir()
	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	cp := checkpoint.Checkpoint(2)
	if err := roots.Commit(cp); err == nil {
		t.Fatalf("expected error when committing non-incremental checkpoint")
	}
}

func TestRootList_Commit_FailsOnMissingPreparationStep(t *testing.T) {
	dir := t.TempDir()
	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	cp := checkpoint.Checkpoint(1)
	if err := roots.Commit(cp); err == nil {
		t.Fatalf("expected error when committing non-incremental checkpoint")
	}
}

func TestRootList_Commit_FailsOnUnreadablePreparationFile(t *testing.T) {
	dir := t.TempDir()
	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	prepareFile := filepath.Join(dir, fileNameArchiveRootsCheckpointDirectory, fileNameArchiveRootsPreparedCheckpoint)
	if err := os.WriteFile(prepareFile, []byte("not in JSON format"), 0700); err != nil {
		t.Fatalf("failed to create unreadable prepare file: %v", err)
	}

	cp := checkpoint.Checkpoint(1)
	if err := roots.Commit(cp); err == nil {
		t.Fatalf("expected error when committing non-incremental checkpoint")
	}
}

func TestRootList_Abort_DeletesPendingCheckpointFile(t *testing.T) {
	dir := t.TempDir()
	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	prepareFile := filepath.Join(dir, fileNameArchiveRootsCheckpointDirectory, fileNameArchiveRootsPreparedCheckpoint)
	if err := os.WriteFile(prepareFile, []byte{}, 0700); err != nil {
		t.Fatalf("failed to create prepare file: %v", err)
	}

	cp := checkpoint.Checkpoint(1)
	if err := roots.Abort(cp); err != nil {
		t.Fatalf("failed to abort checkpoint: %v", err)
	}

	if _, err := os.Stat(prepareFile); !os.IsNotExist(err) {
		t.Fatalf("expected prepare file to be deleted")
	}
}

func TestRootList_Abort_OnlyAcceptsIncrementalCheckpoints(t *testing.T) {
	dir := t.TempDir()
	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	cp := checkpoint.Checkpoint(2)
	if err := roots.Abort(cp); err == nil {
		t.Fatalf("expected error when aborting non-incremental checkpoint")
	}
}

func TestRootList_Restore_CanRecoverCorruptedRoots(t *testing.T) {
	for _, name := range []string{"prepared", "committed"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			roots, err := loadRoots(dir)
			if err != nil {
				t.Fatalf("failed to load roots: %v", err)
			}
			roots.append(Root{NodeRef: NewNodeReference(ValueId(123))})
			roots.append(Root{NodeRef: NewNodeReference(ValueId(123))})

			if err := roots.storeRoots(); err != nil {
				t.Fatalf("failed to store roots: %v", err)
			}

			cp := checkpoint.Checkpoint(1)
			if err := roots.Prepare(cp); err != nil {
				t.Fatalf("failed to prepare checkpoint: %v", err)
			}
			if name == "committed" {
				if err := roots.Commit(cp); err != nil {
					t.Fatalf("failed to commit checkpoint: %v", err)
				}
			}

			backup, err := os.ReadFile(roots.filename)
			if err != nil {
				t.Fatalf("failed to read backup file: %v", err)
			}

			modified := append(backup, []byte("corrupted data")...)
			if err := os.WriteFile(roots.filename, modified, 0700); err != nil {
				t.Fatalf("failed to corrupt roots file: %v", err)
			}

			if err := getRootListRestorer(dir).Restore(cp); err != nil {
				t.Fatalf("failed to restore roots: %v", err)
			}

			restored, err := os.ReadFile(roots.filename)
			if err != nil {
				t.Fatalf("failed to read restored file: %v", err)
			}

			if !bytes.Equal(backup, restored) {
				t.Fatalf("unexpected restored file content")
			}
		})
	}
}

func TestRootList_Restore_FailsIfCheckpointFileCanNotBeRead(t *testing.T) {
	dir := t.TempDir()

	directory := filepath.Join(dir, fileNameArchiveRootsCheckpointDirectory)
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatalf("failed to create checkpoint directory: %v", err)
	}
	checkpointFile := filepath.Join(directory, fileNameArchiveRootsCommittedCheckpoint)
	if err := os.WriteFile(checkpointFile, []byte("corrupted data"), 0700); err != nil {
		t.Fatalf("failed to create corrupted checkpoint file: %v", err)
	}

	cp := checkpoint.Checkpoint(1)
	if err := getRootListRestorer(dir).Restore(cp); err == nil {
		t.Fatalf("expected recovery error due to invalid checkpoint file")
	}
}

func TestRootList_Restore_FailsIfAskedToRecoverUnknownCheckpoint(t *testing.T) {
	dir := t.TempDir()

	cp := checkpoint.Checkpoint(5)
	if err := getRootListRestorer(dir).Restore(cp); err == nil || !strings.Contains(err.Error(), "unknown checkpoint") {
		t.Fatalf("expected recovery error due to unknown checkpoint, got %v", err)
	}
}

func TestRootList_getNumRootsInCheckpoint_RetrievesRootHeightFromCommittedOrPendingCheckpoint(t *testing.T) {
	dir := t.TempDir()

	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}
	roots.append(Root{NodeRef: NewNodeReference(ValueId(1))})
	roots.append(Root{NodeRef: NewNodeReference(ValueId(2))})

	err = errors.Join(
		roots.Prepare(1),
		roots.Commit(1),
	)
	if err != nil {
		t.Fatalf("failed to create checkpoint: %v", err)
	}

	roots.append(Root{NodeRef: NewNodeReference(ValueId(3))})
	if err := roots.Prepare(2); err != nil {
		t.Fatalf("failed to prepare checkpoint: %v", err)
	}

	restorer := getRootListRestorer(dir)

	if _, err := restorer.getNumRootsInCheckpoint(0); err == nil {
		t.Errorf("expected error when checkpoint does not exist")
	}

	if res, err := restorer.getNumRootsInCheckpoint(1); err != nil && res != 2 {
		t.Errorf("expected 2 roots in checkpoint 1, got %d, err %v", res, err)
	}

	if res, err := restorer.getNumRootsInCheckpoint(2); err != nil && res != 3 {
		t.Errorf("expected 3 roots in checkpoint 2, got %d, err %v", res, err)
	}

	if _, err := restorer.getNumRootsInCheckpoint(3); err == nil {
		t.Errorf("expected error when checkpoint does not exist")
	}
}

func TestRootList_getNumRootsInCheckpoint_ReturnsAnErrorIfThereIsNoCheckpoint(t *testing.T) {
	dir := t.TempDir()
	restorer := getRootListRestorer(dir)
	if _, err := restorer.getNumRootsInCheckpoint(0); err == nil {
		t.Errorf("expected error when there is no checkpoint")
	}
}

func TestRootList_truncate_ShortensTheRootFileAndUpdatesTheLastCheckpoint(t *testing.T) {
	dir := t.TempDir()
	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	roots.append(Root{NodeRef: NewNodeReference(ValueId(1))})
	roots.append(Root{NodeRef: NewNodeReference(ValueId(2))})
	roots.append(Root{NodeRef: NewNodeReference(ValueId(3))})

	if err := roots.storeRoots(); err != nil {
		t.Fatalf("failed to store roots: %v", err)
	}
	coordinator, err := checkpoint.NewCoordinator(
		filepath.Join(dir, fileNameArchiveCheckpointDirectory),
		roots,
	)
	if err != nil {
		t.Fatalf("failed to create checkpoint coordinator: %v", err)
	}
	if _, err := coordinator.CreateCheckpoint(); err != nil {
		t.Fatalf("failed to create a checkpoint: %v", err)
	}

	got, err := GetCheckpointBlock(dir)
	if err != nil {
		t.Fatalf("failed to get checkpoint block: %v", err)
	}
	if want := uint64(2); got != want {
		t.Fatalf("unexpected checkpoint block, wanted %d, got %d", want, got)
	}

	if want, got := 3, roots.length(); want != got {
		t.Fatalf("unexpected number of roots, wanted %d, got %d", want, got)
	}

	// truncate the roots list to height 1 (=length of 2)
	if err := getRootListRestorer(dir).truncate(2); err != nil {
		t.Fatalf("failed to truncate roots: %v", err)
	}

	got, err = GetCheckpointBlock(dir)
	if err != nil {
		t.Fatalf("failed to get checkpoint block: %v", err)
	}
	if want := uint64(1); got != want {
		t.Fatalf("unexpected checkpoint block, wanted %d, got %d", want, got)
	}

	roots, err = loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	if want, got := 2, roots.length(); want != got {
		t.Fatalf("unexpected number of roots, wanted %d, got %d", want, got)
	}
}

func TestRootList_truncate_FailsIfCurrentListIsTooShort(t *testing.T) {
	dir := t.TempDir()
	roots, err := loadRoots(dir)
	if err != nil {
		t.Fatalf("failed to load roots: %v", err)
	}

	roots.append(Root{NodeRef: NewNodeReference(ValueId(1))})
	roots.append(Root{NodeRef: NewNodeReference(ValueId(2))})
	roots.append(Root{NodeRef: NewNodeReference(ValueId(3))})

	if err := roots.storeRoots(); err != nil {
		t.Fatalf("failed to store roots: %v", err)
	}
	cp := checkpoint.Checkpoint(1)
	err = errors.Join(
		roots.Prepare(cp),
		roots.Commit(cp),
	)
	if err != nil {
		t.Fatalf("failed to create a checkpoint: %v", err)
	}

	if err := getRootListRestorer(dir).truncate(4); err == nil {
		t.Fatalf("expected error when truncating would lead to an extension")
	}
}

func TestRootList_truncate_FailsIfThereIsNoCheckpoint(t *testing.T) {
	dir := t.TempDir()
	if err := getRootListRestorer(dir).truncate(4); err == nil {
		t.Fatalf("expected error when truncating without a checkpoint")
	}
}

func TestRootList_truncateRootsFile_FailsForNonExistingFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "non-existing-file")
	if err := truncateRootsFile(file, 4); err == nil {
		t.Fatalf("expected error when truncating non-existing file")
	}
}

func BenchmarkArchiveFlush_Roots(b *testing.B) {
	archive, err := OpenArchiveTrie(b.TempDir(), S5ArchiveConfig, NodeCacheConfig{Capacity: 1000}, ArchiveConfig{})
	if err != nil {
		b.Fatalf("cannot open archive: %v", err)
	}
	defer archive.Close()
	archive.Add(1_000_000, common.Update{}, nil)
	for i := 0; i < b.N; i++ {
		if err := archive.Flush(); err != nil {
			b.Fatalf("failed to flush archive: %v", err)
		}
	}
}

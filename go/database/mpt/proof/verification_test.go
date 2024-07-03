// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package proof

import (
	"context"
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/common/interrupt"
	"github.com/Fantom-foundation/Carmen/go/common/witness"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"go.uber.org/mock/gomock"
	"os"
	"strings"
	"testing"
)

func TestVerification_VerifyProofArchiveTrie(t *testing.T) {
	const blocks = 100
	const slots = 100
	for _, config := range []mpt.MptConfig{mpt.S5LiveConfig, mpt.S5ArchiveConfig} {
		t.Run(config.Name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			archive, err := mpt.OpenArchiveTrie(dir, config, mpt.NodeCacheConfig{Capacity: 1024}, mpt.ArchiveConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			addr := common.Address{1}
			for i := 0; i <= blocks; i++ {
				slotUpdates := make([]common.SlotUpdate, 0, slots)
				for j := 0; j < slots; j++ {
					slotUpdates = append(slotUpdates, common.SlotUpdate{Account: addr, Key: common.Key{byte(i), byte(j)}, Value: common.Value{byte(j)}})
				}

				update := common.Update{
					CreatedAccounts: []common.Address{addr},
					Nonces:          []common.NonceUpdate{{Account: addr, Nonce: common.Nonce{byte(i)}}},
					Balances:        []common.BalanceUpdate{{Account: addr, Balance: amount.New(uint64(i))}},
					Slots:           slotUpdates,
				}

				if err := archive.Add(uint64(i), update, nil); err != nil {
					t.Errorf("failed to add block 1; %s", err)
				}
			}

			if err := archive.Close(); err != nil {
				t.Fatalf("failed to close archive: %v", err)
			}

			var blockHeightCorrect bool
			ctrl := gomock.NewController(t)
			observer := mpt.NewMockVerificationObserver(ctrl)
			observer.EXPECT().StartVerification()
			observer.EXPECT().Progress(gomock.Any()).Do(func(msg string) {
				if strings.Contains(msg, fmt.Sprintf("Verifying total block range [%d;%d]", 0, blocks)) {
					blockHeightCorrect = true
				}
			}).AnyTimes()
			observer.EXPECT().EndVerification(nil)

			if err := VerifyArchiveTrie(context.Background(), dir, config, 0, blocks, observer); err != nil {
				t.Errorf("failed to verify archive trie: %v", err)
			}

			if !blockHeightCorrect {
				t.Errorf("block height is not correct")
			}
		})
	}
}

func TestVerification_VerifyProofArchiveTrie_InvalidBlockNumber(t *testing.T) {
	ctrl := gomock.NewController(t)

	archiveTrie, err := mpt.OpenArchiveTrie(t.TempDir(), mpt.S5ArchiveConfig, mpt.NodeCacheConfig{Capacity: 1024}, mpt.ArchiveConfig{})
	if err != nil {
		t.Fatalf("failed to create empty archive, err %v", err)
	}
	defer func() {
		if err := archiveTrie.Close(); err != nil {
			t.Fatalf("failed to close archive: %v", err)
		}
	}()

	const Blocks = 3
	for i := 0; i <= Blocks; i++ {
		if err := archiveTrie.Add(uint64(i), common.Update{
			CreatedAccounts: []common.Address{{byte(i)}},
			Balances:        []common.BalanceUpdate{{Account: common.Address{byte(i)}, Balance: amount.New(12)}},
		}, nil); err != nil {
			t.Fatalf("failed to add block: %v", err)
		}
	}

	var blockAdjusted bool
	observer := mpt.NewMockVerificationObserver(ctrl)
	observer.EXPECT().Progress(gomock.Any()).Do(func(msg string) {
		if strings.Contains(msg, fmt.Sprintf("setting a maximum block height: %d", Blocks)) {
			blockAdjusted = true
		}
	}).AnyTimes()

	if err := verifyArchiveTrie(context.Background(), archiveTrie, 0, 1000, observer); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !blockAdjusted {
		t.Errorf("block height is not adjusted")
	}
}

func TestVerification_VerifyProofArchiveTrie_EmptyBlockchain(t *testing.T) {
	ctrl := gomock.NewController(t)
	archiveTrie := NewMockverifiableArchiveTrie(ctrl)
	archiveTrie.EXPECT().GetBlockHeight().Return(uint64(0), true, nil).AnyTimes()

	// no observer will be called
	observer := mpt.NewMockVerificationObserver(ctrl)

	if err := verifyArchiveTrie(context.Background(), archiveTrie, 0, 1000, observer); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestVerification_VerifyProof_Cannot_Open(t *testing.T) {
	tests := map[string]struct {
		test func(dir string, observer mpt.VerificationObserver) error
	}{
		"archive": {
			func(dir string, observer mpt.VerificationObserver) error {
				return VerifyArchiveTrie(context.Background(), dir, mpt.S5ArchiveConfig, 0, 0, observer)
			},
		},
		"live": {
			func(dir string, observer mpt.VerificationObserver) error {
				return VerifyLiveTrie(context.Background(), dir, mpt.S5LiveConfig, observer)
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// set cannot read privileges
			dir := t.TempDir()
			if err := os.Chmod(dir, 0200); err != nil {
				t.Fatalf("failed to change permissions: %v", err)
			}
			if err := test.test(dir, mpt.NilVerificationObserver{}); err == nil {
				t.Errorf("expected error, got nil")
			}
		})
	}
}

func TestVerification_VerifyProofLiveTrie(t *testing.T) {
	const accounts = 3
	const keys = 13
	for _, config := range []mpt.MptConfig{mpt.S5LiveConfig, mpt.S5ArchiveConfig} {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			live, err := mpt.OpenFileLiveTrie(dir, config, mpt.NodeCacheConfig{})
			if err != nil {
				t.Fatalf("failed to create live trie, err %v", err)
			}

			for i := 0; i < accounts; i++ {
				addr := common.Address{byte(i)}
				if err := live.SetAccountInfo(addr, mpt.AccountInfo{Nonce: common.Nonce{byte(i)}, Balance: amount.New(uint64(i))}); err != nil {
					t.Errorf("failed to add account %d; %s", i, err)
				}

				for j := 0; j < keys; j++ {
					if err := live.SetValue(addr, common.Key{byte(j), byte(i)}, common.Value{byte(i)}); err != nil {
						t.Errorf("failed to add account %d; %s", i, err)
					}
				}
			}

			if err := live.Close(); err != nil {
				t.Fatalf("failed to close live trie: %v", err)
			}

			ctrl := gomock.NewController(t)
			observer := mpt.NewMockVerificationObserver(ctrl)
			observer.EXPECT().StartVerification()
			observer.EXPECT().Progress(gomock.Any()).AnyTimes()
			observer.EXPECT().EndVerification(nil)

			if err := VerifyLiveTrie(context.Background(), dir, config, observer); err != nil {
				t.Errorf("failed to verify live trie: %v", err)
			}
		})
	}
}

func TestVerification_FailingArchiveTrie(t *testing.T) {
	ctrl := gomock.NewController(t)
	observer := mpt.NewMockVerificationObserver(ctrl)
	observer.EXPECT().Progress(gomock.Any()).AnyTimes()

	// init a real trie
	archiveTrie, err := mpt.OpenArchiveTrie(t.TempDir(), mpt.S5ArchiveConfig, mpt.NodeCacheConfig{Capacity: 1024}, mpt.ArchiveConfig{})
	if err != nil {
		t.Fatalf("failed to create empty archive, err %v", err)
	}
	defer func() {
		if err := archiveTrie.Close(); err != nil {
			t.Fatalf("failed to close archive: %v", err)
		}
	}()

	if err := archiveTrie.Add(1, common.Update{
		CreatedAccounts: []common.Address{{1}},
		Balances:        []common.BalanceUpdate{{Account: common.Address{1}, Balance: amount.New(12)}},
	}, nil); err != nil {
		t.Fatalf("failed to add block: %v", err)
	}

	injectedError := fmt.Errorf("injected error")
	var count int
	threshold := 1000_000
	errorInjectingArchiveVerifiableTrieMock := NewMockverifiableArchiveTrie(ctrl)
	errorInjectingArchiveVerifiableTrieMock.EXPECT().GetBlockHeight().DoAndReturn(func() (uint64, bool, error) {
		if count >= threshold {
			return 0, false, injectedError
		}
		count++
		return archiveTrie.GetBlockHeight()
	}).AnyTimes()
	errorInjectingArchiveVerifiableTrieMock.EXPECT().GetHash(gomock.Any()).DoAndReturn(func(block uint64) (common.Hash, error) {
		if count >= threshold {
			return common.Hash{}, injectedError
		}
		count++
		return archiveTrie.GetHash(block)
	}).AnyTimes()
	errorInjectingArchiveVerifiableTrieMock.EXPECT().VisitTrie(gomock.Any(), gomock.Any()).DoAndReturn(func(block uint64, visitor mpt.NodeVisitor) error {
		if count >= threshold {
			return injectedError
		}
		count++
		return archiveTrie.VisitTrie(block, visitor)
	}).AnyTimes()
	errorInjectingArchiveVerifiableTrieMock.EXPECT().VisitAccount(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(block uint64, address common.Address, visitor mpt.NodeVisitor) error {
		if count >= threshold {
			return injectedError
		}
		count++
		return archiveTrie.VisitAccount(block, address, visitor)
	}).AnyTimes()
	errorInjectingArchiveVerifiableTrieMock.EXPECT().GetAccountInfo(gomock.Any(), gomock.Any()).DoAndReturn(func(block uint64, addr common.Address) (mpt.AccountInfo, bool, error) {
		if count >= threshold {
			return mpt.AccountInfo{}, false, injectedError
		}
		count++
		return archiveTrie.GetAccountInfo(block, addr)
	}).AnyTimes()
	errorInjectingArchiveVerifiableTrieMock.EXPECT().GetStorage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(block uint64, addr common.Address, key common.Key) (common.Value, error) {
		if count >= threshold {
			return common.Value{}, injectedError
		}
		count++
		return archiveTrie.GetStorage(block, addr, key)
	}).AnyTimes()
	errorInjectingArchiveVerifiableTrieMock.EXPECT().CreateWitnessProof(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(block uint64, address common.Address, keys ...common.Key) (witness.Proof, error) {
		if count >= threshold {
			return nil, injectedError
		}
		count++
		return archiveTrie.CreateWitnessProof(block, address, keys...)
	}).AnyTimes()
	errorInjectingArchiveVerifiableTrieMock.EXPECT().GetBlockHeight().DoAndReturn(func() (uint64, bool, error) {
		if count >= threshold {
			return 0, false, injectedError
		}
		count++
		return archiveTrie.GetBlockHeight()
	}).AnyTimes()

	// count the number of executions first
	if err := verifyArchiveTrie(context.Background(), errorInjectingArchiveVerifiableTrieMock, 0, 10, observer); err != nil {
		t.Fatalf("failed to verify archive trie: %v", err)
	}

	// exercise the mock based on the number of executions
	loops := count
	for i := 0; i < loops; i++ {
		count = 0     // reset the counter
		threshold = i // update the threshold every loop
		if err := verifyArchiveTrie(context.Background(), errorInjectingArchiveVerifiableTrieMock, 0, 10, observer); !errors.Is(err, injectedError) {
			t.Errorf("expected error %v, got %v", injectedError, err)
		}
	}
}

func TestVerification_FailingLiveTrie(t *testing.T) {
	ctrl := gomock.NewController(t)
	observer := mpt.NewMockVerificationObserver(ctrl)
	observer.EXPECT().Progress(gomock.Any()).AnyTimes()

	// init a real trie
	trie, err := mpt.OpenFileLiveTrie(t.TempDir(), mpt.S5ArchiveConfig, mpt.NodeCacheConfig{Capacity: 1024})
	if err != nil {
		t.Fatalf("failed to create trie, err %v", err)
	}
	defer func() {
		if err := trie.Close(); err != nil {
			t.Fatalf("failed to close: %v", err)
		}
	}()

	if err := trie.SetAccountInfo(common.Address{1}, mpt.AccountInfo{Nonce: common.Nonce{1}}); err != nil {
		t.Fatalf("failed to set nonce: %v", err)
	}

	for i := 0; i < 11; i++ {
		if err := trie.SetValue(common.Address{1}, common.Key{byte(i)}, common.Value{1}); err != nil {
			t.Fatalf("failed to set storage: %v", err)
		}
	}

	injectedError := fmt.Errorf("injected error")

	var count int
	threshold := 1000_000
	errorInjectingVerifiableTrieMock := NewMockverifiableTrie(ctrl)
	errorInjectingVerifiableTrieMock.EXPECT().UpdateHashes().DoAndReturn(func() (common.Hash, *mpt.NodeHashes, error) {
		if count >= threshold {
			return common.Hash{}, nil, injectedError
		}
		count++
		return trie.UpdateHashes()
	}).AnyTimes()
	errorInjectingVerifiableTrieMock.EXPECT().VisitTrie(gomock.Any()).DoAndReturn(func(visitor mpt.NodeVisitor) error {
		if count >= threshold {
			return injectedError
		}
		count++
		return trie.VisitTrie(visitor)
	}).AnyTimes()
	errorInjectingVerifiableTrieMock.EXPECT().VisitAccount(gomock.Any(), gomock.Any()).DoAndReturn(func(address common.Address, visitor mpt.NodeVisitor) error {
		if count >= threshold {
			return injectedError
		}
		count++
		return trie.VisitAccount(address, visitor)
	}).AnyTimes()
	errorInjectingVerifiableTrieMock.EXPECT().GetAccountInfo(gomock.Any()).DoAndReturn(func(addr common.Address) (mpt.AccountInfo, bool, error) {
		if count >= threshold {
			return mpt.AccountInfo{}, false, injectedError
		}
		count++
		return trie.GetAccountInfo(addr)
	}).AnyTimes()
	errorInjectingVerifiableTrieMock.EXPECT().GetValue(gomock.Any(), gomock.Any()).DoAndReturn(func(addr common.Address, key common.Key) (common.Value, error) {
		if count >= threshold {
			return common.Value{}, injectedError
		}
		count++
		return trie.GetValue(addr, key)
	}).AnyTimes()
	errorInjectingVerifiableTrieMock.EXPECT().CreateWitnessProof(gomock.Any(), gomock.Any()).DoAndReturn(func(address common.Address, key ...common.Key) (witness.Proof, error) {
		if count >= threshold {
			return nil, injectedError
		}
		count++

		parent, err := trie.CreateWitnessProof(address, key...)

		mockWitness := witness.NewMockProof(ctrl)
		mockWitness.EXPECT().GetBalance(gomock.Any(), gomock.Any()).DoAndReturn(func(root common.Hash, addr common.Address) (amount.Amount, bool, error) {
			if count >= threshold {
				return amount.Amount{}, false, injectedError
			}
			count++
			return parent.GetBalance(root, addr)
		}).AnyTimes()
		mockWitness.EXPECT().GetNonce(gomock.Any(), gomock.Any()).DoAndReturn(func(root common.Hash, addr common.Address) (common.Nonce, bool, error) {
			if count >= threshold {
				return common.Nonce{}, false, injectedError
			}
			count++
			return parent.GetNonce(root, addr)
		}).AnyTimes()
		mockWitness.EXPECT().GetCodeHash(gomock.Any(), gomock.Any()).DoAndReturn(func(root common.Hash, addr common.Address) (common.Hash, bool, error) {
			if count >= threshold {
				return common.Hash{}, false, injectedError
			}
			count++
			return parent.GetCodeHash(root, addr)
		}).AnyTimes()
		mockWitness.EXPECT().GetState(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(root common.Hash, addr common.Address, key common.Key) (common.Value, bool, error) {
			if count >= threshold {
				return common.Value{}, false, injectedError
			}
			count++
			return parent.GetState(root, addr, key)
		}).AnyTimes()
		mockWitness.EXPECT().IsValid().Return(true).AnyTimes()

		return mockWitness, err
	}).AnyTimes()

	// count the number of executions first
	if err := verifyTrie(context.Background(), errorInjectingVerifiableTrieMock, observer); err != nil {
		t.Fatalf("failed to verify archive trie: %v", err)
	}

	// exercise the mock based on the number of executions
	loops := count
	for i := 0; i < loops; i++ {
		count = 0     // reset the counter
		threshold = i // update the threshold every loop
		if err := verifyTrie(context.Background(), errorInjectingVerifiableTrieMock, observer); !errors.Is(err, injectedError) {
			t.Errorf("expected error %v, got %v", injectedError, err)
		}
	}
}

func TestVerification_FailingInvalidProofs(t *testing.T) {
	ctrl := gomock.NewController(t)
	observer := mpt.NewMockVerificationObserver(ctrl)
	observer.EXPECT().Progress(gomock.Any()).AnyTimes()

	// init a real trie
	trie, err := mpt.OpenFileLiveTrie(t.TempDir(), mpt.S5ArchiveConfig, mpt.NodeCacheConfig{Capacity: 1024})
	if err != nil {
		t.Fatalf("failed to create trie, err %v", err)
	}
	defer func() {
		if err := trie.Close(); err != nil {
			t.Fatalf("failed to close: %v", err)
		}
	}()

	if err := trie.SetAccountInfo(common.Address{1}, mpt.AccountInfo{Nonce: common.Nonce{1}}); err != nil {
		t.Fatalf("failed to set nonce: %v", err)
	}

	for i := 0; i < 11; i++ {
		if err := trie.SetValue(common.Address{1}, common.Key{byte(i)}, common.Value{1}); err != nil {
			t.Fatalf("failed to set storage: %v", err)
		}
	}

	var counter int
	threshold := 1000_000
	errorInjectingTrieMock := NewMockverifiableTrie(ctrl)
	errorInjectingTrieMock.EXPECT().UpdateHashes().DoAndReturn(trie.UpdateHashes).AnyTimes()
	errorInjectingTrieMock.EXPECT().VisitTrie(gomock.Any()).DoAndReturn(trie.VisitTrie).AnyTimes()
	errorInjectingTrieMock.EXPECT().VisitAccount(gomock.Any(), gomock.Any()).DoAndReturn(trie.VisitAccount).AnyTimes()
	errorInjectingTrieMock.EXPECT().GetAccountInfo(gomock.Any()).DoAndReturn(trie.GetAccountInfo).AnyTimes()
	errorInjectingTrieMock.EXPECT().GetValue(gomock.Any(), gomock.Any()).DoAndReturn(trie.GetValue).AnyTimes()
	errorInjectingTrieMock.EXPECT().CreateWitnessProof(gomock.Any(), gomock.Any()).DoAndReturn(func(address common.Address, key ...common.Key) (witness.Proof, error) {
		valid := counter < threshold
		parentProof, err := trie.CreateWitnessProof(address, key...)
		if err != nil {
			t.Fatalf("failed to create proof: %v", err)
		}
		proofMock := witness.NewMockProof(ctrl)
		proofMock.EXPECT().GetBalance(gomock.Any(), gomock.Any()).DoAndReturn(parentProof.GetBalance).AnyTimes()
		proofMock.EXPECT().GetNonce(gomock.Any(), gomock.Any()).DoAndReturn(parentProof.GetNonce).AnyTimes()
		proofMock.EXPECT().GetCodeHash(gomock.Any(), gomock.Any()).DoAndReturn(parentProof.GetCodeHash).AnyTimes()
		proofMock.EXPECT().GetState(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(parentProof.GetState).AnyTimes()
		proofMock.EXPECT().IsValid().Return(valid).AnyTimes()
		counter++

		return proofMock, nil
	}).AnyTimes()

	// count the number of executions first
	if err := verifyTrie(context.Background(), errorInjectingTrieMock, observer); err != nil {
		t.Fatalf("failed to verify archive trie: %v", err)
	}

	// exercise the mock based on the number of executions
	loops := counter
	for i := 0; i < loops; i++ {
		counter = 0   // reset the counter
		threshold = i // update the threshold every loop
		if err := verifyTrie(context.Background(), errorInjectingTrieMock, observer); !errors.Is(err, ErrInvalidProof) {
			t.Errorf("expected error %v, got %v", ErrInvalidProof, err)
		}
	}
}

func TestVerification_VerifyProof_Incomplete_Or_Empty(t *testing.T) {
	tests := []struct {
		name           string
		returnComplete bool
	}{
		{"incomplete", false},
		{"empty", true},
	}

	address := common.Address{1}
	data := mpt.AccountInfo{Nonce: common.Nonce{1}, Balance: amount.New(1), CodeHash: common.Hash{1}}
	storage := map[common.Key]common.Value{{1}: {1}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			var count int
			threshold := 1000
			errorInjectingProofMock := witness.NewMockProof(ctrl)
			errorInjectingProofMock.EXPECT().GetBalance(gomock.Any(), gomock.Any()).DoAndReturn(func(root common.Hash, addr common.Address) (amount.Amount, bool, error) {
				if count >= threshold {
					return amount.Amount{}, test.returnComplete, nil
				}
				count++
				return amount.New(1), true, nil
			}).AnyTimes()
			errorInjectingProofMock.EXPECT().GetNonce(gomock.Any(), gomock.Any()).DoAndReturn(func(root common.Hash, addr common.Address) (common.Nonce, bool, error) {
				if count >= threshold {
					return common.Nonce{}, test.returnComplete, nil
				}
				count++
				return common.Nonce{1}, true, nil
			}).AnyTimes()
			errorInjectingProofMock.EXPECT().GetCodeHash(gomock.Any(), gomock.Any()).DoAndReturn(func(root common.Hash, addr common.Address) (common.Hash, bool, error) {
				if count >= threshold {
					return common.Hash{}, test.returnComplete, nil
				}
				count++
				return common.Hash{1}, true, nil
			}).AnyTimes()
			errorInjectingProofMock.EXPECT().GetState(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(root common.Hash, addr common.Address, key common.Key) (common.Value, bool, error) {
				if count >= threshold {
					return common.Value{}, test.returnComplete, nil
				}
				count++
				return common.Value{1}, true, nil
			}).AnyTimes()

			// count the number of executions first
			if err := verifyAccount(common.Hash{}, errorInjectingProofMock, address, data); err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// exercise the mock based on the number of executions
			loops := count
			for i := 0; i < loops; i++ {
				count = 0     // reset the counter
				threshold = i // update the threshold every loop
				if err := verifyAccount(common.Hash{}, errorInjectingProofMock, address, data); err == nil {
					t.Errorf("expected error, got nil")
				}
				if err := verifyStorage(common.Hash{}, errorInjectingProofMock, address, []common.Key{{1}}, storage); err == nil {
					t.Errorf("expected error, got nil")
				}
			}
		})
	}

}

func TestVerification_VerifyProof_Can_Cancel(t *testing.T) {
	ctrl := gomock.NewController(t)

	proof := witness.NewMockProof(ctrl)
	proof.EXPECT().GetBalance(gomock.Any(), gomock.Any()).Return(amount.New(), true, nil).AnyTimes()
	proof.EXPECT().GetNonce(gomock.Any(), gomock.Any()).Return(common.Nonce{}, true, nil).AnyTimes()
	proof.EXPECT().GetCodeHash(gomock.Any(), gomock.Any()).Return(common.Hash{}, true, nil).AnyTimes()
	proof.EXPECT().GetState(gomock.Any(), gomock.Any(), gomock.Any()).Return(common.Value{}, true, nil).AnyTimes()
	proof.EXPECT().IsValid().Return(true).AnyTimes()

	trie := NewMockverifiableTrie(ctrl)
	trie.EXPECT().CreateWitnessProof(gomock.Any(), gomock.Any()).Return(proof, nil).AnyTimes()
	trie.EXPECT().VisitAccount(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	trie.EXPECT().GetValue(gomock.Any(), gomock.Any()).Return(common.Value{}, nil).AnyTimes()

	tests := map[string]struct {
		create   func(ctx context.Context) mpt.NodeVisitor
		getError func(visitor mpt.NodeVisitor) error
	}{
		"account": {
			func(ctx context.Context) mpt.NodeVisitor {
				return &accountVerifyingVisitor{ctx: ctx, trie: trie, logWindow: 1000}
			},
			func(visitor mpt.NodeVisitor) error {
				return visitor.(*accountVerifyingVisitor).err
			},
		},
		"storage": {
			func(ctx context.Context) mpt.NodeVisitor {
				storage := make(map[common.Key]common.Value)
				return &storageVerifyingVisitor{ctx: ctx, trie: trie, storage: storage}
			},
			func(visitor mpt.NodeVisitor) error {
				return visitor.(*storageVerifyingVisitor).err
			},
		},
	}

	const numNodes = 100
	nodes := make([]mpt.Node, 0, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes = append(nodes, &mpt.ValueNode{})
		nodes = append(nodes, &mpt.BranchNode{})
		nodes = append(nodes, &mpt.ExtensionNode{})
		nodes = append(nodes, &mpt.AccountNode{})
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {

			ctx := newCountingWhenDoneContext(context.Background(), 10_000_000)
			visitor := test.create(ctx)
			for _, node := range nodes {
				visitor.Visit(node, mpt.NodeInfo{})
			}

			for i := 0; i < ctx.count; i++ {
				ctx := newCountingWhenDoneContext(context.Background(), i)
				visitor := test.create(ctx)
				for _, node := range nodes {
					visitor.Visit(node, mpt.NodeInfo{})
				}
				if err := test.getError(visitor); !errors.Is(err, interrupt.ErrCanceled) {
					t.Errorf("expected error %v, got %v", interrupt.ErrCanceled, err)
				}

			}
		})
	}
}

func TestVerification_Generates_ExistingAddress(t *testing.T) {
	ctrl := gomock.NewController(t)

	trie := NewMockverifiableTrie(ctrl)

	gomock.InOrder(
		trie.EXPECT().GetAccountInfo(gomock.Any()).Return(mpt.AccountInfo{}, true, nil),
		trie.EXPECT().GetAccountInfo(gomock.Any()).Return(mpt.AccountInfo{}, false, nil),
	)

	add, err := generateUnusedAddresses(trie, 1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(add) != 1 {
		t.Errorf("expected 1 address, got %d", len(add))
	}
}

func TestVerification_Generates_ExistingKey(t *testing.T) {
	ctrl := gomock.NewController(t)

	trie := NewMockverifiableTrie(ctrl)

	gomock.InOrder(
		trie.EXPECT().GetValue(gomock.Any(), gomock.Any()).Return(common.Value{1}, nil),
		trie.EXPECT().GetValue(gomock.Any(), gomock.Any()).Return(common.Value{}, nil),
	)

	keys, err := generateUnusedKeys(trie, 1, common.Address{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(keys) != 1 {
		t.Errorf("expected 1 key, got %d", len(keys))
	}
}

func TestVerification_Log_Processed_Accounts(t *testing.T) {
	ctrl := gomock.NewController(t)

	accountNode := &mpt.AccountNode{}
	const LogWindow = 100

	observer := mpt.NewMockVerificationObserver(ctrl)
	observer.EXPECT().Progress(gomock.Any()).Do(func(msg string) {
		if !strings.Contains(msg, fmt.Sprintf("  ... verified %d addresses", LogWindow)) {
			t.Errorf("expected to see a log message with the number of processed accounts = %d", LogWindow)
		}
	})

	proof := witness.NewMockProof(ctrl)
	proof.EXPECT().GetBalance(gomock.Any(), gomock.Any()).Return(amount.New(), true, nil).AnyTimes()
	proof.EXPECT().GetNonce(gomock.Any(), gomock.Any()).Return(common.Nonce{}, true, nil).AnyTimes()
	proof.EXPECT().GetCodeHash(gomock.Any(), gomock.Any()).Return(common.Hash{}, true, nil).AnyTimes()
	proof.EXPECT().GetState(gomock.Any(), gomock.Any(), gomock.Any()).Return(common.Value{}, true, nil).AnyTimes()
	proof.EXPECT().IsValid().Return(true).AnyTimes()

	trie := NewMockverifiableTrie(ctrl)
	trie.EXPECT().CreateWitnessProof(gomock.Any(), gomock.Any()).Return(proof, nil).AnyTimes()
	trie.EXPECT().VisitAccount(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	trie.EXPECT().GetValue(gomock.Any(), gomock.Any()).Return(common.Value{}, nil).AnyTimes()

	visitor := accountVerifyingVisitor{trie: trie, observer: observer, logWindow: LogWindow,
		ctx: context.Background()}

	for i := 0; i < LogWindow+1; i++ {
		visitor.Visit(accountNode, mpt.NodeInfo{})
	}
}

// countingWhenDoneContext is a context.Context that counts the number of times Done is called, and signals done only
// when the threshold is reached.
type countingWhenDoneContext struct {
	context.Context
	count     int // count the number of executions checking done
	threshold int // when this threshold is reached, signal done from this point onwards
	done      chan struct{}
}

func newCountingWhenDoneContext(ctx context.Context, threshold int) *countingWhenDoneContext {
	return &countingWhenDoneContext{
		Context:   ctx,
		threshold: threshold,
		done:      make(chan struct{}),
	}
}

func (c *countingWhenDoneContext) Done() <-chan struct{} {
	if c.count == c.threshold { // equality to close only once
		close(c.done)
	}
	c.count++
	return c.done
}

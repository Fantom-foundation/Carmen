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
	"context"
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/common/interrupt"
	"github.com/Fantom-foundation/Carmen/go/common/witness"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
	"go.uber.org/mock/gomock"
	"log"
	"os"
	"path"
	"strings"
	"testing"
)

func TestVerification_VerifyProofArchiveTrie(t *testing.T) {
	const blocks = 100
	const slots = 100
	for _, config := range []MptConfig{S5LiveConfig, S5ArchiveConfig} {
		t.Run(config.Name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{Capacity: 1024})
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
			observer := NewMockVerificationObserver(ctrl)
			observer.EXPECT().StartVerification()
			observer.EXPECT().Progress(gomock.Any()).Do(func(msg string) {
				if strings.Contains(msg, fmt.Sprintf("Verifying total block range [%d;%d]", 0, blocks)) {
					blockHeightCorrect = true
				}
			}).AnyTimes()
			observer.EXPECT().EndVerification(nil)

			if err := VerifyProofArchiveTrie(context.Background(), dir, config, 0, blocks, observer); err != nil {
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

	db := NewMockDatabase(ctrl)

	archiveTrie := ArchiveTrie{forest: db, nodeSource: db}
	archiveTrie.roots.roots = append(archiveTrie.roots.roots, Root{NodeReference{}, common.Hash{}})

	observer := NewMockVerificationObserver(ctrl)
	observer.EXPECT().Progress(gomock.Any()).Do(func(msg string) {
		// tweak the number of blocks to trigger failure
		archiveTrie.roots.roots = make([]Root, 0)
	}).AnyTimes()

	if err := verifyProofArchiveTrie(context.Background(), &archiveTrie, -1, 1000, observer); err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestVerification_VerifyProof_Cannot_Close(t *testing.T) {
	tests := map[string]struct {
		test func(dir string, observer VerificationObserver) error
	}{
		"archive": {
			func(dir string, observer VerificationObserver) error {
				return VerifyProofArchiveTrie(context.Background(), dir, S5ArchiveConfig, 0, 0, observer)
			},
		},
		"live": {
			func(dir string, observer VerificationObserver) error {
				return VerifyProofLiveTrie(context.Background(), dir, S5LiveConfig, observer)
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			ctrl := gomock.NewController(t)

			observer := NewMockVerificationObserver(ctrl)
			observer.EXPECT().StartVerification().Do(func() {
				// set read-only privileges
				file, err := os.OpenFile(path.Join(dir, "meta.json"), os.O_CREATE|os.O_RDONLY, 0400)
				if err != nil {
					log.Fatalf("Failed to create file: %v", err)
				}
				if err := file.Close(); err != nil {
					log.Fatalf("Failed to close file: %v", err)
				}
			})
			observer.EXPECT().Progress(gomock.Any()).AnyTimes()
			observer.EXPECT().EndVerification(gomock.Any())

			if err := test.test(dir, observer); err == nil {
				t.Errorf("expected error, got nil")
			}
		})
	}
}

func TestVerification_VerifyProof_Cannot_Open(t *testing.T) {
	tests := map[string]struct {
		test func(dir string, observer VerificationObserver) error
	}{
		"archive": {
			func(dir string, observer VerificationObserver) error {
				return VerifyProofArchiveTrie(context.Background(), dir, S5ArchiveConfig, 0, 0, observer)
			},
		},
		"live": {
			func(dir string, observer VerificationObserver) error {
				return VerifyProofLiveTrie(context.Background(), dir, S5LiveConfig, observer)
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
			if err := test.test(dir, NilVerificationObserver{}); err == nil {
				t.Errorf("expected error, got nil")
			}
		})
	}
}

func TestVerification_VerifyProofLiveTrie(t *testing.T) {
	const accounts = 3
	const keys = 13
	for _, config := range []MptConfig{S5LiveConfig, S5ArchiveConfig} {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			live, err := OpenFileLiveTrie(dir, config, NodeCacheConfig{})
			if err != nil {
				t.Fatalf("failed to create live trie, err %v", err)
			}

			for i := 0; i < accounts; i++ {
				addr := common.Address{byte(i)}
				if err := live.SetAccountInfo(addr, AccountInfo{Nonce: common.Nonce{byte(i)}, Balance: amount.New(uint64(i))}); err != nil {
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
			observer := NewMockVerificationObserver(ctrl)
			observer.EXPECT().StartVerification()
			observer.EXPECT().Progress(gomock.Any()).AnyTimes()
			observer.EXPECT().EndVerification(nil)

			if err := VerifyProofLiveTrie(context.Background(), dir, config, observer); err != nil {
				t.Errorf("failed to verify live trie: %v", err)
			}
		})
	}
}

func TestVerification_VerifyProofLiveTrie_CannotGetRootHash(t *testing.T) {
	ctrl := gomock.NewController(t)

	injectedError := fmt.Errorf("injected error")
	db := NewMockDatabase(ctrl)
	db.EXPECT().updateHashesFor(gomock.Any()).Return(common.Hash{}, nil, injectedError)

	mockLive := LiveTrie{db, NodeReference{}, ""}

	observer := NewMockVerificationObserver(ctrl)
	observer.EXPECT().Progress(gomock.Any()).AnyTimes()

	if err := verifyProofLiveTrie(context.Background(), &mockLive, observer); !errors.Is(err, injectedError) {
		t.Errorf("expected error %v, got %v", injectedError, err)
	}
}

func TestVerification_VerifyProof_Wrong_Proofs_Incomplete_Or_Error(t *testing.T) {
	address := common.Address{1}
	data := AccountInfo{Nonce: common.Nonce{1}, Balance: amount.New(1), CodeHash: common.Hash{1}}
	storage := map[common.Key]common.Value{{1}: {1}}

	ctrl := gomock.NewController(t)
	parent := witness.NewMockProof(ctrl)
	parent.EXPECT().GetState(gomock.Any(), gomock.Any(), gomock.Any()).Return(common.Value{1}, true, nil).AnyTimes()
	parent.EXPECT().GetBalance(gomock.Any(), gomock.Any()).Return(amount.New(1), true, nil).AnyTimes()
	parent.EXPECT().GetNonce(gomock.Any(), gomock.Any()).Return(common.Nonce{1}, true, nil).AnyTimes()
	parent.EXPECT().GetCodeHash(gomock.Any(), gomock.Any()).Return(common.Hash{1}, true, nil).AnyTimes()

	proof := errorInjectingProof{Proof: parent, returnComplete: true, returnError: nil, threshold: 1000}
	if err := verifyAccountProof(common.Hash{}, &proof, address, data); err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	t.Run("incomplete proofs", func(t *testing.T) {
		for i := 0; i < proof.count; i++ {
			proof := errorInjectingProof{Proof: parent, returnComplete: false, returnError: nil, threshold: i}
			if err := verifyAccountProof(common.Hash{}, &proof, address, data); err == nil {
				t.Errorf("expected error, got nil")
			}
			if err := verifyStorageProof(common.Hash{}, &proof, address, []common.Key{{1}}, storage); err == nil {
				t.Errorf("expected error, got nil")
			}
		}
	})

	t.Run("errors in proofs", func(t *testing.T) {
		injectedError := fmt.Errorf("injected error")
		for i := 0; i < proof.count; i++ {
			proof := errorInjectingProof{Proof: parent, returnComplete: true, returnError: injectedError, threshold: i}
			if err := verifyAccountProof(common.Hash{}, &proof, address, data); !errors.Is(err, injectedError) {
				t.Errorf("expected error %v, got %v", injectedError, err)
			}
			if err := verifyStorageProof(common.Hash{}, &proof, address, []common.Key{{1}}, storage); !errors.Is(err, injectedError) {
				t.Errorf("expected error %v, got %v", injectedError, err)
			}
		}
	})
	t.Run("empty values in proofs", func(t *testing.T) {
		for i := 0; i < proof.count; i++ {
			proof := errorInjectingProof{Proof: parent, returnComplete: true, returnError: nil, threshold: i}
			if err := verifyAccountProof(common.Hash{}, &proof, address, data); err == nil {
				t.Errorf("expected error, got nil")
			}
			if err := verifyStorageProof(common.Hash{}, &proof, address, []common.Key{{1}}, storage); err == nil {
				t.Errorf("expected error, got nil")
			}
		}
	})
}

func TestVerification_VerifyProof_FailingVisitor(t *testing.T) {
	tests := map[string]struct {
		test func(db Database, observer VerificationObserver) error
	}{
		"archive": {
			func(db Database, observer VerificationObserver) error {
				archiveTrie := ArchiveTrie{forest: db, nodeSource: db}
				archiveTrie.roots.roots = append(archiveTrie.roots.roots, Root{NodeReference{}, common.Hash{}})
				return verifyProofArchiveTrie(context.Background(), &archiveTrie, 0, 1, observer)
			},
		},
		"live": {
			func(db Database, observer VerificationObserver) error {
				mockLive := LiveTrie{db, NodeReference{}, ""}
				return verifyProofLiveTrie(context.Background(), &mockLive, observer)
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			injectedError := fmt.Errorf("injected error")

			ctrl := gomock.NewController(t)
			observer := NewMockVerificationObserver(ctrl)
			observer.EXPECT().Progress(gomock.Any()).AnyTimes()

			db := NewMockDatabase(ctrl)
			db.EXPECT().updateHashesFor(gomock.Any())
			db.EXPECT().VisitTrie(gomock.Any(), gomock.Any()).AnyTimes().Return(injectedError)
			if err := test.test(db, observer); !errors.Is(err, injectedError) {
				t.Errorf("expected error %v, got %v", injectedError, err)
			}
		})
	}
}

func TestVerification_VerifyProof_Failing_NodeSource(t *testing.T) {
	tests := map[string]struct {
		test func(db Database, root NodeReference, observer VerificationObserver) error
	}{
		"archive": {
			func(db Database, root NodeReference, observer VerificationObserver) error {
				archiveTrie := ArchiveTrie{forest: db, nodeSource: db}
				archiveTrie.roots.roots = append(archiveTrie.roots.roots, Root{root, common.Hash{}})
				return verifyProofArchiveTrie(context.Background(), &archiveTrie, 0, 0, observer)
			},
		},
		"live": {
			func(db Database, root NodeReference, observer VerificationObserver) error {
				mockLive := LiveTrie{db, root, ""}
				return verifyProofLiveTrie(context.Background(), &mockLive, observer)
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			injectedError := fmt.Errorf("injected error")
			ctrl := gomock.NewController(t)

			observer := NewMockVerificationObserver(ctrl)
			observer.EXPECT().Progress(gomock.Any()).AnyTimes()

			ctxt := newNodeContextWithConfig(t, ctrl, S5LiveConfig)

			// the simples tree that has only accounts and a storage
			valueNode := &Value{key: common.Key{1}, length: 64, value: common.Value{byte(1)}}
			_, keyNode := ctxt.Build(valueNode)
			root, accountNode := ctxt.Build(&Account{
				address:    common.Address{1},
				pathLength: 64,
				storage:    valueNode,
			})

			source := &errorInjectingNodeManager{ctxt, 1000_000, injectedError, 0}

			db := NewMockDatabase(ctrl)
			db.EXPECT().hashAddress(gomock.Any()).DoAndReturn(ctxt.hashAddress).AnyTimes()
			db.EXPECT().hashKey(gomock.Any()).DoAndReturn(ctxt.hashKey).AnyTimes()
			db.EXPECT().getConfig().Return(S5LiveConfig).AnyTimes()
			db.EXPECT().GetValue(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ref *NodeReference, addr common.Address, key common.Key) (common.Value, error) {
				h, err := db.getViewAccess(ref)
				if err == nil {
					h.Release()
				}
				return common.Value{}, err
			}).AnyTimes()
			db.EXPECT().GetAccountInfo(gomock.Any(), gomock.Any()).DoAndReturn(func(ref *NodeReference, addr common.Address) (common.Value, bool, error) {
				h, err := db.getViewAccess(ref)
				if err == nil {
					h.Release()
				}
				return common.Value{}, false, err
			}).AnyTimes()
			db.EXPECT().VisitTrie(gomock.Any(), gomock.Any()).Do(func(ref *NodeReference, visitor *proofVerifyingVisitor) {
				// return either the account or 10+1 keys
				if *ref == root {
					n := accountNode.GetViewHandle()
					defer n.Release()
					visitor.Visit(n.Get(), NodeInfo{})
				} else {
					n := keyNode.GetViewHandle() // existing key
					defer n.Release()
					visitor.Visit(n.Get(), NodeInfo{})
					for i := 0; i < 10; i++ {
						// add extra empty keys
						visitor.Visit(&ValueNode{key: common.Key{byte(0), byte(i)}, value: common.Value{}}, NodeInfo{})
					}
				}
			}).AnyTimes()
			db.EXPECT().updateHashesFor(gomock.Any()).DoAndReturn(func(ref *NodeReference) (common.Hash, *NodeHashes, error) {
				hash, err := ctxt.getHashFor(ref)
				return hash, nil, err
			}).AnyTimes()

			db.EXPECT().getViewAccess(gomock.Any()).DoAndReturn(source.getViewAccess).AnyTimes()

			_ = test.test(db, root, observer) // compute number of executions

			counter := source.counter
			for i := 0; i < counter; i++ {
				*source = errorInjectingNodeManager{ctxt, i, injectedError, 0}
				if err := test.test(db, root, observer); !errors.Is(err, injectedError) {
					t.Errorf("expected error %v, got %v", injectedError, err)
				}
			}
		})
	}
}

func TestVerification_VerifyProof_NodeSource_Returns_Incorrect_Proof(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, S5LiveConfig)

	// the simples tree that has only accounts and a storage
	root, accountHandle := ctxt.Build(&Account{
		address:    common.Address{1},
		pathLength: 64,
		storage:    &Value{key: common.Key{1}, length: 64, value: common.Value{byte(1)}},
	})
	rootHash, _ := ctxt.getHashFor(&root)

	tests := map[string]struct {
		test func(db *MockDatabase, observer VerificationObserver) error
	}{
		"account node missing - expected non-empty": {
			func(db *MockDatabase, observer VerificationObserver) error {
				db.EXPECT().getViewAccess(gomock.Any()).DoAndReturn(func(_ *NodeReference) (shared.ViewHandle[Node], error) {
					return shared.MakeShared[Node](EmptyNode{}).GetViewHandle(), nil
				}).AnyTimes()

				mockLive := LiveTrie{db, root, ""}
				return verifyProofLiveTrie(context.Background(), &mockLive, observer)
			},
		},
		"storage node missing - expected non-empty": {
			func(db *MockDatabase, observer VerificationObserver) error {
				db.EXPECT().getViewAccess(gomock.Any()).DoAndReturn(func(_ *NodeReference) (shared.ViewHandle[Node], error) {
					return accountHandle.GetViewHandle(), nil
				}).AnyTimes()

				mockLive := LiveTrie{db, root, ""}
				return verifyProofLiveTrie(context.Background(), &mockLive, observer)
			},
		},
		"account node missing - expected empty": {
			func(db *MockDatabase, observer VerificationObserver) error {
				db.EXPECT().getViewAccess(gomock.Any()).DoAndReturn(func(_ *NodeReference) (shared.ViewHandle[Node], error) {
					return shared.MakeShared[Node](EmptyNode{}).GetViewHandle(), nil
				}).AnyTimes()

				mockLive := LiveTrie{db, root, ""}
				return verifyProofEmptyAccount(&mockLive, rootHash, 1, observer)
			},
		},
		"storage node missing - expected empty": {
			func(db *MockDatabase, observer VerificationObserver) error {
				db.EXPECT().getViewAccess(gomock.Any()).DoAndReturn(func(_ *NodeReference) (shared.ViewHandle[Node], error) {
					return accountHandle.GetViewHandle(), nil
				}).AnyTimes()

				mockLive := LiveTrie{db, NodeReference{}, ""}
				return verifyProofEmptyStorage(&mockLive, rootHash, common.Address{1})
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			observer := NewMockVerificationObserver(ctrl)
			observer.EXPECT().Progress(gomock.Any()).AnyTimes()

			db := NewMockDatabase(ctrl)
			db.EXPECT().hashAddress(gomock.Any()).DoAndReturn(ctxt.hashAddress).AnyTimes()
			db.EXPECT().hashKey(gomock.Any()).DoAndReturn(ctxt.hashKey).AnyTimes()
			db.EXPECT().getConfig().Return(S5LiveConfig).AnyTimes()
			db.EXPECT().GetValue(gomock.Any(), gomock.Any(), gomock.Any()).Return(common.Value{}, nil).AnyTimes()
			db.EXPECT().GetAccountInfo(gomock.Any(), gomock.Any()).Return(AccountInfo{}, false, nil).AnyTimes()
			db.EXPECT().VisitTrie(gomock.Any(), gomock.Any()).Do(func(ref *NodeReference, visitor *proofVerifyingVisitor) {
				n, err := ctxt.getViewAccess(ref)
				if err != nil {
					t.Fatalf("failed to get view access: %v", err)
				}
				defer n.Release()
				visitor.Visit(n.Get(), NodeInfo{})
			}).AnyTimes()
			db.EXPECT().updateHashesFor(gomock.Any()).DoAndReturn(func(ref *NodeReference) (common.Hash, *NodeHashes, error) {
				hash, err := ctxt.getHashFor(ref)
				return hash, nil, err
			}).AnyTimes()

			if err := test.test(db, observer); err == nil {
				t.Errorf("expected error, got nil")
			}
		})
	}
}

func TestVerification_VerifyProof_Can_Cancel(t *testing.T) {
	tests := map[string]struct {
		test func(db Database, ctxt context.Context, observer VerificationObserver) error
	}{
		"archive": {
			func(db Database, ctxt context.Context, observer VerificationObserver) error {
				archiveTrie := ArchiveTrie{forest: db, nodeSource: db}
				archiveTrie.roots.roots = append(archiveTrie.roots.roots, Root{NodeReference{}, common.Hash{}})
				return verifyProofArchiveTrie(ctxt, &archiveTrie, 0, 0, observer)
			},
		},
		"live": {
			func(db Database, ctxt context.Context, observer VerificationObserver) error {
				mockLive := LiveTrie{db, NodeReference{}, ""}
				return verifyProofLiveTrie(ctxt, &mockLive, observer)
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			observer := NewMockVerificationObserver(ctrl)
			observer.EXPECT().StartVerification().AnyTimes()
			observer.EXPECT().Progress(gomock.Any()).AnyTimes()
			observer.EXPECT().EndVerification(gomock.Any()).AnyTimes()

			addr := common.Address{1}
			key := common.Key{1}

			db := NewMockDatabase(ctrl)
			db.EXPECT().hashAddress(gomock.Any()).Return(common.Keccak256(addr[:])).AnyTimes()
			db.EXPECT().hashKey(gomock.Any()).Return(common.Keccak256(key[:])).AnyTimes()

			const storageId = 99
			valueNode := &ValueNode{key: key, value: common.Value{1}, pathLength: 64}
			rlp, err := encodeToRlp(valueNode, db, []byte{})
			if err != nil {
				t.Fatalf("failed to encode account node: %v", err)
			}
			storageHash := common.Keccak256(rlp)

			accountNode := &AccountNode{address: addr, pathLength: 64,
				storage:     NewNodeReference(NodeId(storageId)),
				storageHash: storageHash,
				info:        AccountInfo{Nonce: common.Nonce{1}, Balance: amount.New(1)}}

			rlp, err = encodeToRlp(accountNode, db, []byte{})
			if err != nil {
				t.Fatalf("failed to encode account node: %v", err)
			}
			rootHash := common.Keccak256(rlp)

			db.EXPECT().updateHashesFor(gomock.Any()).Return(rootHash, nil, nil).AnyTimes()
			db.EXPECT().getConfig().Return(S5LiveConfig).AnyTimes()
			db.EXPECT().GetValue(gomock.Any(), gomock.Any(), gomock.Any()).Return(common.Value{}, nil).AnyTimes()
			db.EXPECT().GetAccountInfo(gomock.Any(), gomock.Any()).Return(AccountInfo{}, false, nil).AnyTimes()
			db.EXPECT().VisitTrie(gomock.Any(), gomock.Any()).Do(func(_ *NodeReference, visitor *proofVerifyingVisitor) {
				visitor.Visit(EmptyNode{}, NodeInfo{})
			}).AnyTimes()
			db.EXPECT().getViewAccess(gomock.Any()).DoAndReturn(func(ref *NodeReference) (shared.ViewHandle[Node], error) {
				var n Node = accountNode
				if ref.Id() == NodeId(storageId) {
					n = valueNode
				}
				return shared.MakeShared[Node](n).GetViewHandle(), nil
			}).AnyTimes()

			ctx := newCountingWhenDoneContext(context.Background(), 10_000_000)
			_ = test.test(db, ctx, observer)

			for i := 0; i < ctx.count; i++ {
				ctx := newCountingWhenDoneContext(context.Background(), i)
				if err := test.test(db, ctx, observer); !errors.Is(err, interrupt.ErrCanceled) {
					t.Errorf("expected error %v, got %v", interrupt.ErrCanceled, err)
				}
			}
		})
	}
}

func TestVerification_GenerateUnknownAddressesExists(t *testing.T) {

	tests := map[string]struct {
		test func(trie *LiveTrie, try int) int
	}{
		"generate unknown addresses": {
			func(trie *LiveTrie, try int) int {
				n, err := generateUnknownAddresses(trie, try)
				if err != nil {
					t.Fatalf("failed to generate unknown addresses: %v", err)
				}
				return len(n)
			},
		},
		"generate unknown keys": {
			func(trie *LiveTrie, try int) int {
				n, err := generateUnknownKeys(trie, try, common.Address{})
				if err != nil {
					t.Fatalf("failed to generate unknown keys: %v", err)
				}
				return len(n)
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			observer := NewMockVerificationObserver(ctrl)
			observer.EXPECT().Progress(gomock.Any()).AnyTimes()

			db := NewMockDatabase(ctrl)
			db.EXPECT().GetValue(gomock.Any(), gomock.Any(), gomock.Any()).Return(common.Value{1}, nil).AnyTimes()
			db.EXPECT().GetAccountInfo(gomock.Any(), gomock.Any()).Return(AccountInfo{}, true, nil).AnyTimes()

			trie := LiveTrie{db, NodeReference{}, ""}
			if n := test.test(&trie, 100); n != 0 {
				t.Errorf("expected 0 generated elements, got %d", n)
			}
		})
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

// errorInjectingProof is a witness.Proof that can be configured to return an error or wrong values after a certain number of calls.
type errorInjectingProof struct {
	witness.Proof
	returnComplete bool
	returnError    error
	count          int
	threshold      int
}

func (p *errorInjectingProof) GetCodeHash(root common.Hash, address common.Address) (common.Hash, bool, error) {
	if p.count >= p.threshold {
		return common.Hash{}, p.returnComplete, p.returnError
	}
	p.count++
	return p.Proof.GetCodeHash(root, address)
}

func (p *errorInjectingProof) GetState(root common.Hash, address common.Address, key common.Key) (common.Value, bool, error) {
	if p.count >= p.threshold {
		return common.Value{}, p.returnComplete, p.returnError
	}
	p.count++
	return p.Proof.GetState(root, address, key)
}

func (p *errorInjectingProof) GetBalance(root common.Hash, address common.Address) (amount.Amount, bool, error) {
	if p.count >= p.threshold {
		return amount.Amount{}, p.returnComplete, p.returnError
	}
	p.count++
	return p.Proof.GetBalance(root, address)
}

func (p *errorInjectingProof) GetNonce(root common.Hash, address common.Address) (common.Nonce, bool, error) {
	if p.count >= p.threshold {
		return common.Nonce{}, p.returnComplete, p.returnError
	}
	p.count++
	return p.Proof.GetNonce(root, address)
}

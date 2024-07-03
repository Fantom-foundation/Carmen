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
	for _, config := range []MptConfig{S5LiveConfig, S5ArchiveConfig} {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			archive, err := OpenArchiveTrie(dir, config, NodeCacheConfig{})
			if err != nil {
				t.Fatalf("failed to create empty archive, err %v", err)
			}

			addr := common.Address{1}
			for i := 0; i < blocks; i++ {
				update := common.Update{
					CreatedAccounts: []common.Address{addr},
					Nonces:          []common.NonceUpdate{{Account: addr, Nonce: common.Nonce{byte(i)}}},
					Balances:        []common.BalanceUpdate{{Account: addr, Balance: common.Balance{byte(i)}}},
					Slots:           []common.SlotUpdate{{Account: addr, Key: common.Key{byte(i)}, Value: common.Value{byte(i)}}},
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
				if strings.Contains(msg, fmt.Sprintf("Verifying total %d blocks", blocks)) {
					blockHeightCorrect = true
				}
			}).AnyTimes()
			observer.EXPECT().EndVerification(nil)

			if err := VerifyProofArchiveTrie(context.Background(), dir, config, -1, 2*blocks, observer); err != nil {
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

	var canChangeBlocks bool
	db := NewMockDatabase(ctrl)
	db.EXPECT().VisitTrie(gomock.Any(), gomock.Any()).Do(func(_ *NodeReference, visitor *accountCollectingVisitor) {
		canChangeBlocks = true
	})

	archiveTrie := ArchiveTrie{forest: db, nodeSource: db}
	archiveTrie.roots.roots = append(archiveTrie.roots.roots, Root{NodeReference{}, common.Hash{}})

	observer := NewMockVerificationObserver(ctrl)
	observer.EXPECT().StartVerification()
	observer.EXPECT().Progress(gomock.Any()).Do(func(msg string) {
		if canChangeBlocks {
			// tweak the number of blocks to trigger failure
			archiveTrie.roots.roots = make([]Root, 0)
		}
	}).AnyTimes()
	observer.EXPECT().EndVerification(gomock.Any())

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
			observer.EXPECT().EndVerification(gomock.Any()).AnyTimes()

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
	const accounts = 100
	for _, config := range []MptConfig{S5LiveConfig, S5ArchiveConfig} {
		t.Run(config.Name, func(t *testing.T) {
			dir := t.TempDir()
			live, err := OpenFileLiveTrie(dir, config, NodeCacheConfig{})
			if err != nil {
				t.Fatalf("failed to create live trie, err %v", err)
			}

			for i := 0; i < accounts; i++ {
				addr := common.Address{byte(i)}
				if err := live.SetAccountInfo(addr, AccountInfo{Nonce: common.Nonce{byte(i)}, Balance: common.Balance{byte(i)}}); err != nil {
					t.Errorf("failed to add account %d; %s", i, err)
				}

				if err := live.SetValue(addr, common.Key{byte(i)}, common.Value{byte(i)}); err != nil {
					t.Errorf("failed to add account %d; %s", i, err)
				}
			}

			if err := live.Close(); err != nil {
				t.Fatalf("failed to close live trie: %v", err)
			}

			ctrl := gomock.NewController(t)
			observer := NewMockVerificationObserver(ctrl)
			observer.EXPECT().StartVerification()
			observer.EXPECT().Progress(gomock.Any()).Times(4)
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
	db.EXPECT().VisitTrie(gomock.Any(), gomock.Any())
	db.EXPECT().updateHashesFor(gomock.Any()).Return(common.Hash{}, nil, injectedError)

	mockLive := LiveTrie{db, NodeReference{}, ""}

	observer := NewMockVerificationObserver(ctrl)
	observer.EXPECT().StartVerification()
	observer.EXPECT().Progress(gomock.Any()).AnyTimes()
	observer.EXPECT().EndVerification(gomock.Any())

	if err := verifyProofLiveTrie(context.Background(), &mockLive, observer); !errors.Is(err, injectedError) {
		t.Errorf("expected error %v, got %v", injectedError, err)
	}
}

func TestVerification_VerifyProof_Wrong_Proofs_Incomplete_Or_Error(t *testing.T) {
	accountsMap := accountsMap{}
	accountsMap[common.Address{1}] = accountData{
		nonce: common.Nonce{1}, balance: common.Balance{1}, code: common.Hash{1},
		storage: map[common.Key]common.Value{{1}: {1}}}

	ctrl := gomock.NewController(t)
	parent := witness.NewMockProof(ctrl)
	parent.EXPECT().GetState(gomock.Any(), gomock.Any(), gomock.Any()).Return(common.Value{1}, true, nil).AnyTimes()
	parent.EXPECT().GetBalance(gomock.Any(), gomock.Any()).Return(common.Balance{1}, true, nil).AnyTimes()
	parent.EXPECT().GetNonce(gomock.Any(), gomock.Any()).Return(common.Nonce{1}, true, nil).AnyTimes()
	parent.EXPECT().GetCodeHash(gomock.Any(), gomock.Any()).Return(common.Hash{1}, true, nil).AnyTimes()

	proof := errorInjectingProof{Proof: parent, returnComplete: true, returnError: nil, threshold: 1000}
	if err := verifyProofState(context.Background(), common.Hash{}, &proof, accountsMap); err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	t.Run("incomplete proofs", func(t *testing.T) {
		for i := 0; i < proof.count; i++ {
			proof := errorInjectingProof{Proof: parent, returnComplete: false, returnError: nil, threshold: i}
			if err := verifyProofState(context.Background(), common.Hash{}, &proof, accountsMap); err == nil {
				t.Errorf("expected error, got nil")
			}
		}
	})

	t.Run("errors in proofs", func(t *testing.T) {
		injectedError := fmt.Errorf("injected error")
		for i := 0; i < proof.count; i++ {
			proof := errorInjectingProof{Proof: parent, returnComplete: true, returnError: injectedError, threshold: i}
			if err := verifyProofState(context.Background(), common.Hash{}, &proof, accountsMap); !errors.Is(err, injectedError) {
				t.Errorf("expected error %v, got %v", injectedError, err)
			}
		}
	})
	t.Run("empty values in proofs", func(t *testing.T) {
		for i := 0; i < proof.count; i++ {
			proof := errorInjectingProof{Proof: parent, returnComplete: true, returnError: nil, threshold: i}
			if err := verifyProofState(context.Background(), common.Hash{}, &proof, accountsMap); err == nil {
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
				return verifyProofArchiveTrie(context.Background(), &archiveTrie, 0, 0, observer)
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
			ctrl := gomock.NewController(t)
			observer := NewMockVerificationObserver(ctrl)
			observer.EXPECT().StartVerification()
			observer.EXPECT().Progress(gomock.Any()).AnyTimes()
			observer.EXPECT().EndVerification(gomock.Any())

			injectedError := fmt.Errorf("injected error")
			db := NewMockDatabase(ctrl)
			db.EXPECT().VisitTrie(gomock.Any(), gomock.Any()).AnyTimes().Return(injectedError)
			if err := test.test(db, observer); !errors.Is(err, injectedError) {
				t.Errorf("expected error %v, got %v", injectedError, err)
			}
		})
	}
}

func TestVerification_VerifyProof_Failing_NodeSource(t *testing.T) {
	tests := map[string]struct {
		test func(db Database, observer VerificationObserver) error
	}{
		"archive": {
			func(db Database, observer VerificationObserver) error {
				archiveTrie := ArchiveTrie{forest: db, nodeSource: db}
				archiveTrie.roots.roots = append(archiveTrie.roots.roots, Root{NodeReference{}, common.Hash{}})
				return verifyProofArchiveTrie(context.Background(), &archiveTrie, 0, 0, observer)
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
			ctrl := gomock.NewController(t)
			observer := NewMockVerificationObserver(ctrl)
			observer.EXPECT().StartVerification()
			observer.EXPECT().Progress(gomock.Any()).AnyTimes()
			observer.EXPECT().EndVerification(gomock.Any())

			accountsMap := accountsMap{}
			accountsMap[common.Address{1}] = accountData{nonce: common.Nonce{1}, balance: common.Balance{1},
				storage: map[common.Key]common.Value{{1}: {1}}}

			injectedError := fmt.Errorf("injected error")
			db := NewMockDatabase(ctrl)
			db.EXPECT().getConfig().Return(S5LiveConfig).AnyTimes()
			db.EXPECT().VisitTrie(gomock.Any(), gomock.Any()).Do(func(_ *NodeReference, visitor *accountCollectingVisitor) {
				visitor.accounts = accountsMap
			})
			db.EXPECT().getViewAccess(gomock.Any()).Return(shared.ViewHandle[Node]{}, injectedError)
			db.EXPECT().hashAddress(gomock.Any()).AnyTimes()

			if err := test.test(db, observer); !errors.Is(err, injectedError) {
				t.Errorf("expected error %v, got %v", injectedError, err)
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

			accountsMap := accountsMap{}
			accountsMap[common.Address{1}] = accountData{nonce: common.Nonce{1}, balance: common.Balance{1},
				storage: map[common.Key]common.Value{{1}: {1}}}

			db := NewMockDatabase(ctrl)
			db.EXPECT().updateHashesFor(gomock.Any()).AnyTimes()
			db.EXPECT().getConfig().Return(S4LiveConfig).AnyTimes()
			db.EXPECT().VisitTrie(gomock.Any(), gomock.Any()).Do(func(_ *NodeReference, visitor *accountCollectingVisitor) {
				visitor.Visit(EmptyNode{}, NodeInfo{})
				visitor.accounts = accountsMap
			}).AnyTimes()
			db.EXPECT().getViewAccess(gomock.Any()).DoAndReturn(func(_ *NodeReference) (shared.ViewHandle[Node], error) {
				return shared.MakeShared[Node](EmptyNode{}).GetViewHandle(), nil
			}).AnyTimes()

			ctx := newCountingWhenDoneContext(context.Background(), 1000)
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

func (p *errorInjectingProof) GetBalance(root common.Hash, address common.Address) (common.Balance, bool, error) {
	if p.count >= p.threshold {
		return common.Balance{}, p.returnComplete, p.returnError
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

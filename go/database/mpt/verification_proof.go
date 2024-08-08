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
	"golang.org/x/exp/maps"
	"math/rand"
)

// VerifyProofArchiveTrie verifies the consistency of witness proofs for an archive trie.
// It reads the trie for each block within the input range.
// It gets account and storage slots and extracts a witness proof for each account and its storage.
// It is checked that values in the database and the proof match.
func VerifyProofArchiveTrie(ctx context.Context, dir string, config MptConfig, from, to int, observer VerificationObserver) error {
	trie, err := OpenArchiveTrie(dir, config, NodeCacheConfig{})
	if err != nil {
		return err
	}

	observer.StartVerification()
	err = errors.Join(
		verifyProofArchiveTrie(ctx, trie, from, to, observer),
		trie.Close(),
	)
	observer.EndVerification(err)
	return err
}

func verifyProofArchiveTrie(ctx context.Context, trie *ArchiveTrie, from, to int, observer VerificationObserver) error {
	blockHeight, empty, err := trie.GetBlockHeight()
	if err != nil {
		return err
	}
	if empty {
		return nil
	}

	if to > int(blockHeight) {
		to = int(blockHeight)
		observer.Progress(fmt.Sprintf("setting a maximum block height: %d", blockHeight))
	}

	observer.Progress(fmt.Sprintf("Verifying total block range [%d;%d]", from, to))
	for i := from; i <= to; i++ {
		trie, err := trie.getView(uint64(i))
		if err != nil {
			return err
		}

		observer.Progress(fmt.Sprintf("Verifying blcok: %d ", i))
		if err := verifyProofLiveTrie(ctx, trie, observer); err != nil {
			return err
		}
	}

	return nil
}

// VerifyProofLiveTrie verifies the consistency of witness proofs for a live trie.
// It reads the trie for the head block and loads accounts and storage slots.
// It extracts witness proofs for these accounts and its storage,
// and checks that values in the proof and the database match.
func VerifyProofLiveTrie(ctx context.Context, dir string, config MptConfig, observer VerificationObserver) error {
	trie, err := OpenFileLiveTrie(dir, config, NodeCacheConfig{})
	if err != nil {
		return err
	}

	observer.StartVerification()
	err = errors.Join(
		verifyProofLiveTrie(ctx, trie, observer),
		trie.Close(),
	)
	observer.EndVerification(err)
	return err
}

func verifyProofLiveTrie(ctx context.Context, trie *LiveTrie, observer VerificationObserver) error {
	rootHash, hints, err := trie.UpdateHashes()
	if hints != nil {
		hints.Release()
	}
	if err != nil {
		return err
	}

	observer.Progress("Collecting and Verifying proof... ")
	visitor := proofVerifyingVisitor{
		ctx:      ctx,
		rootHash: rootHash,
		trie:     trie,
		observer: observer,
	}
	if err := trie.VisitTrie(&visitor); err != nil || visitor.err != nil {
		return errors.Join(err, visitor.err)
	}

	const numAddresses = 1000
	return verifyProofEmptyAccount(trie, rootHash, numAddresses, observer)
}

// verifyProofEmptyAccount verifies the consistency of witness proofs for empty accounts that are not present in the trie.
func verifyProofEmptyAccount(trie *LiveTrie, rootHash common.Hash, numAddresses int, observer VerificationObserver) error {
	observer.Progress(fmt.Sprintf("Veryfing %d empty addresses...", numAddresses))
	addresses, err := generateUnknownAddresses(trie, numAddresses)
	if err != nil {
		return err
	}
	for _, addr := range addresses {
		proof, err := trie.CreateWitnessProof(addr)
		if err != nil {
			return err
		}
		// expect an empty account
		if err := verifyAccountProof(rootHash, proof, addr, AccountInfo{}); err != nil {
			return err
		}
	}

	return nil
}

// verifyProofEmptyStorage verifies the consistency of witness proofs for empty storage that are not present in the trie.
func verifyProofEmptyStorage(trie *LiveTrie, rootHash common.Hash, addr common.Address) error {
	const numKeys = 10
	keys, err := generateUnknownKeys(trie, numKeys, addr)
	if err != nil {
		return err
	}
	proof, err := trie.CreateWitnessProof(addr, keys...)
	if err != nil {
		return err
	}
	if err := verifyStorageProof(rootHash, proof, addr, keys, nil); err != nil {
		return err
	}

	return nil
}

// verifyAccountProof verifies the consistency between the input witness proofs and the account.
func verifyAccountProof(root common.Hash, proof witness.Proof, addr common.Address, info AccountInfo) error {
	balance, complete, err := proof.GetBalance(root, addr)
	if err != nil {
		return err
	}
	if !complete {
		return fmt.Errorf("proof incomplete for 0x%x", addr)
	}
	if got, want := balance, info.Balance; got != want {
		return fmt.Errorf("balance mismatch for 0x%x, got %v, want %v", addr, got, want)
	}
	nonce, complete, err := proof.GetNonce(root, addr)
	if err != nil {
		return err
	}
	if !complete {
		return fmt.Errorf("proof incomplete for 0x%x", addr)
	}
	if got, want := nonce, info.Nonce; got != want {
		return fmt.Errorf("nonce mismatch for 0x%x, got %v, want %v", addr, got, want)
	}
	code, complete, err := proof.GetCodeHash(root, addr)
	if err != nil {
		return err
	}
	if !complete {
		return fmt.Errorf("proof incomplete for 0x%x", addr)
	}
	if got, want := code, info.CodeHash; got != want {
		return fmt.Errorf("code mismatch for 0x%x, got %v, want %v", addr, got, want)
	}

	return nil
}

// verifyStorageProof verifies the consistency between the input witness proofs and the storage.
func verifyStorageProof(root common.Hash, proof witness.Proof, addr common.Address, keys []common.Key, storage map[common.Key]common.Value) error {
	for _, key := range keys {
		proofValue, complete, err := proof.GetState(root, addr, key)
		if err != nil {
			return err
		}
		if !complete {
			return fmt.Errorf("proof incomplete for address: 0x%x, key: 0x%x", addr, key)
		}
		if got, want := proofValue, storage[key]; got != want {
			return fmt.Errorf("storage mismatch for 0x%x, key 0x%x, got %v, want %v", addr, key, got, want)
		}
	}

	return nil
}

// generateUnknownAddresses generates a slice of addresses that does not appear in the input MPT.
// if in an unlikely situation a generated address is in the trie, it is not added in the result list.
// It means that in an unlikely situation, the result list may contain fewer addresses than the input number.
func generateUnknownAddresses(trie *LiveTrie, number int) ([]common.Address, error) {
	res := make([]common.Address, 0, number)
	for i := 0; i < number; i++ {
		j := rand.Int()
		addr := common.Address{byte(j), byte(j >> 8), byte(j >> 16), byte(j >> 24), 1}

		// if an unlikely situation happens and the address is not in the trie, skip it
		_, exists, err := trie.GetAccountInfo(addr)
		if err != nil {
			return nil, err
		}

		if exists {
			continue
		}

		res = append(res, addr)
	}

	return res, nil
}

// generateUnknownKeys generates a slice of keys  that does not appear in the input MPT.
// if in an unlikely situation a generated key is in the trie, it is not added in the result list.
// It means that in an unlikely situation, the result list may contain fewer keys than the input number.
func generateUnknownKeys(trie *LiveTrie, number int, address common.Address) ([]common.Key, error) {
	res := make([]common.Key, 0, number)
	for i := 0; i < number; i++ {
		j := rand.Int()
		key := common.Key{byte(j), byte(j >> 8), byte(j >> 16), byte(j >> 24), 1}

		// if an unlikely situation happens and the key is not in the trie, skip it
		val, err := trie.GetValue(address, key)
		if err != nil {
			return nil, err
		}

		if val != (common.Value{}) {
			continue
		}

		res = append(res, key)
	}

	return res, nil
}

// proofVerifyingVisitor is a visitor that verifies the consistency of witness proofs for a live trie.
// It collects account and storage slots and extracts witness proofs for each account and its storage.
// It checks that values in the database and the proof match.
// The process can be interrupted by the input context.
// Storage keys are verified in batches of 10 to save on memory and allowing for responsive cancellation.
type proofVerifyingVisitor struct {
	ctx      context.Context
	rootHash common.Hash
	trie     *LiveTrie
	observer VerificationObserver

	err error

	counter        int
	numAddresses   int
	currentAddress common.Address
	storage        map[common.Key]common.Value
}

func (v *proofVerifyingVisitor) Visit(n Node, _ NodeInfo) VisitResponse {
	if v.counter%100 == 0 && interrupt.IsCancelled(v.ctx) {
		v.err = interrupt.ErrCanceled
		return VisitResponseAbort
	}
	v.counter++

	switch n := n.(type) {
	case *AccountNode:
		proof, err := v.trie.CreateWitnessProof(n.Address())
		if err != nil {
			v.err = err
			return VisitResponseAbort
		}
		if err := verifyAccountProof(v.rootHash, proof, n.Address(), n.Info()); err != nil {
			v.err = err
			return VisitResponseAbort
		}

		storageVisitor := proofVerifyingVisitor{
			ctx:            v.ctx,
			rootHash:       v.rootHash,
			trie:           v.trie,
			observer:       v.observer,
			currentAddress: n.Address(),
			storage:        make(map[common.Key]common.Value)}

		if err := v.trie.forest.VisitTrie(&n.storage, &storageVisitor); err != nil || storageVisitor.err != nil {
			v.err = errors.Join(err, storageVisitor.err)
			return VisitResponseAbort
		}

		// verify remaining storage if not done inside the visitor
		if len(storageVisitor.storage) > 0 {
			if err := storageVisitor.verifyStorage(); err != nil {
				v.err = err
				return VisitResponseAbort
			}
		}

		// add empty storages check
		if err := verifyProofEmptyStorage(v.trie, v.rootHash, n.Address()); err != nil {
			v.err = err
			return VisitResponseAbort
		}

		v.numAddresses++
		if (v.numAddresses)%100_000 == 0 {
			v.observer.Progress(fmt.Sprintf("  ... verified %d addresses", v.numAddresses))
		}

		return VisitResponsePrune // this account resolved, do not go deeper
	case *ValueNode:
		v.storage[n.Key()] = n.Value()
	}

	// when ten keys accumulate, verify the storage
	if len(v.storage) >= 10 {
		if err := v.verifyStorage(); err != nil {
			v.err = err
			return VisitResponseAbort
		}
	}

	return VisitResponseContinue
}

func (v *proofVerifyingVisitor) verifyStorage() error {
	keys := maps.Keys(v.storage)

	proof, err := v.trie.CreateWitnessProof(v.currentAddress, keys...)
	if err != nil {
		return err
	}

	if err := verifyStorageProof(v.rootHash, proof, v.currentAddress, keys, v.storage); err != nil {
		return err
	}

	v.storage = make(map[common.Key]common.Value)
	return nil
}

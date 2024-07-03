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
	"log"
)

// VerifyProofArchiveTrie verifies the consistency of witness proofs for an archive trie.
// It reads the trie for each block of the archive and loads all accounts and storage slots.
// Then, it extracts witness proofs for the same accounts and storage slots of each block.
// Finally, the actual values from the trie are compared with the values extracted from the witness proofs.
func VerifyProofArchiveTrie(ctx context.Context, dir string, config MptConfig, from, to int, observer VerificationObserver) error {
	trie, err := OpenArchiveTrie(dir, config, NodeCacheConfig{})
	if err != nil {
		return err
	}

	return errors.Join(
		verifyProofArchiveTrie(ctx, trie, from, to, observer),
		trie.Close())
}

// verifyProofArchiveTrie verifies the consistency of witness proofs for an archive trie.
// It reads the trie for each block of the archive and loads all accounts and storage slots.
// Then, it extracts witness proofs for the same accounts and storage slots of each block.
// Finally, the actual values from the trie are compared with the values extracted from the witness proofs.
func verifyProofArchiveTrie(ctx context.Context, trie *ArchiveTrie, from, to int, observer VerificationObserver) error {
	observer.StartVerification()
	blockHeight, empty, err := trie.GetBlockHeight()
	if err != nil {
		observer.EndVerification(err)
		return err
	}
	if empty {
		return nil
	}

	if from < 0 {
		from = 0
		log.Printf("adjusting to a minimum block height: %d", from)
	}

	if to <= 0 || to > int(blockHeight) {
		to = int(blockHeight) + 1
		log.Printf("adjusting to a maximum block height: %d", to)
	}

	observer.Progress(fmt.Sprintf("Verifying total %d blocks", to-from))
	for i := uint64(from); i < uint64(to); i++ {
		observer.Progress(fmt.Sprintf("Collecting state for block... \t %d", i))
		// collect all accounts for this block
		visitor := accountCollectingVisitor{accounts: accountsMap{}, ctx: ctx}
		if err := trie.VisitTrie(i, &visitor); err != nil {
			observer.EndVerification(err)
			return err
		}
		if interrupt.IsCancelled(ctx) {
			return interrupt.ErrCanceled
		}

		observer.Progress(fmt.Sprintf("Collecting proof for block.. \t %d \t Accounts: %d", i, len(visitor.accounts)))
		// collect all proofs for this block
		rawProof := make(map[string]struct{})
		for addr, data := range visitor.accounts {
			if interrupt.IsCancelled(ctx) {
				return interrupt.ErrCanceled
			}

			accountProof, err := trie.CreateWitnessProof(i, addr, maps.Keys(data.storage)...)
			if err != nil {
				observer.EndVerification(err)
				return err
			}
			for _, element := range accountProof.GetElements() {
				rawProof[element] = struct{}{}
			}
		}

		rootHash, err := trie.GetHash(i)
		if err != nil {
			observer.EndVerification(err)
			return err
		}

		observer.Progress(fmt.Sprintf("Recovering proof for block... \t %d", i))
		// match proofs and database data
		proof := CreateWitnessProofFromNodes(maps.Keys(rawProof))
		observer.Progress(fmt.Sprintf("Verifying proof for block... \t %d", i))
		if err := verifyProofState(ctx, rootHash, proof, visitor.accounts); err != nil {
			observer.EndVerification(err)
			return err
		}
	}

	observer.EndVerification(nil)
	return nil
}

// VerifyProofLiveTrie verifies the consistency of witness proofs for an live trie.
// It reads the trie for the head block and loads all accounts and storage slots.
// Then, it extracts witness proofs for the same accounts and storage slots.
// Finally, the actual values from the trie are compared with the values extracted from the witness proofs.
func VerifyProofLiveTrie(ctx context.Context, dir string, config MptConfig, observer VerificationObserver) error {
	trie, err := OpenFileLiveTrie(dir, config, NodeCacheConfig{})
	if err != nil {
		return err
	}

	return errors.Join(
		verifyProofLiveTrie(ctx, trie, observer),
		trie.Close())
}

// verifyProofLiveTrie verifies the consistency of witness proofs for an live trie.
// It reads the trie for the head block and loads all accounts and storage slots.
// Then, it extracts witness proofs for the same accounts and storage slots.
// Finally, the actual values from the trie are compared with the values extracted from the witness proofs.
func verifyProofLiveTrie(ctx context.Context, trie *LiveTrie, observer VerificationObserver) error {
	observer.StartVerification()
	observer.Progress("Collecting states... ")
	// collect all accounts for this block
	visitor := accountCollectingVisitor{accounts: accountsMap{}, ctx: ctx}
	if err := trie.VisitTrie(&visitor); err != nil {
		observer.EndVerification(err)
		return err
	}
	if interrupt.IsCancelled(ctx) {
		return interrupt.ErrCanceled
	}

	observer.Progress(fmt.Sprintf("Collecting proof... \t Accounts: %d", len(visitor.accounts)))
	// collect all proofs for this block
	rawProof := make(map[string]struct{})
	for addr, data := range visitor.accounts {
		if interrupt.IsCancelled(ctx) {
			return interrupt.ErrCanceled
		}

		accountProof, err := trie.CreateWitnessProof(addr, maps.Keys(data.storage)...)
		if err != nil {
			observer.EndVerification(err)
			return err
		}
		for _, element := range accountProof.GetElements() {
			rawProof[element] = struct{}{}
		}
	}

	rootHash, hints, err := trie.UpdateHashes()
	if hints != nil {
		hints.Release()
	}
	if err != nil {
		observer.EndVerification(err)
		return err
	}

	observer.Progress("Recovering proof...")
	// match proofs and database data
	proof := CreateWitnessProofFromNodes(maps.Keys(rawProof))

	observer.Progress("Verifying proof...")
	if err := verifyProofState(ctx, rootHash, proof, visitor.accounts); err != nil {
		observer.EndVerification(err)
		return err
	}

	observer.EndVerification(nil)
	return nil
}

// verifyProofState verifies the consistency of witness proofs for a given state, denoted by the accountsMap.
func verifyProofState(ctx context.Context, root common.Hash, proof witness.Proof, accounts accountsMap) error {
	for addr, data := range accounts {
		if interrupt.IsCancelled(ctx) {
			return interrupt.ErrCanceled
		}

		balance, complete, err := proof.GetBalance(root, addr)
		if err != nil {
			return err
		}
		if !complete {
			return fmt.Errorf("proof incomplete for 0x%x", addr)
		}
		if got, want := balance, data.balance; got != want {
			return fmt.Errorf("balance mismatch for 0x%x, got %v, want %v", addr, got, want)
		}
		nonce, complete, err := proof.GetNonce(root, addr)
		if err != nil {
			return err
		}
		if !complete {
			return fmt.Errorf("proof incomplete for 0x%x", addr)
		}
		if got, want := nonce, data.nonce; got != want {
			return fmt.Errorf("nonce mismatch for 0x%x, got %v, want %v", addr, got, want)
		}
		code, complete, err := proof.GetCodeHash(root, addr)
		if err != nil {
			return err
		}
		if !complete {
			return fmt.Errorf("proof incomplete for 0x%x", addr)
		}
		if got, want := code, data.code; got != want {
			return fmt.Errorf("code mismatch for 0x%x, got %v, want %v", addr, got, want)
		}
		for key, value := range data.storage {
			proofValue, complete, err := proof.GetState(root, addr, key)
			if err != nil {
				return err
			}
			if !complete {
				return fmt.Errorf("proof incomplete for address: 0x%x, key: 0x%x", addr, key)
			}
			if got, want := proofValue, value; got != want {
				return fmt.Errorf("storage mismatch for 0x%x, key 0x%x, got %v, want %v", addr, key, got, want)
			}
		}
	}

	return nil
}

type accountData struct {
	balance common.Balance
	nonce   common.Nonce
	code    common.Hash
	storage map[common.Key]common.Value
}

type accountsMap map[common.Address]accountData

type accountCollectingVisitor struct {
	accounts       accountsMap
	currentAccount *accountData
	counter        int
	ctx            context.Context
}

func (v *accountCollectingVisitor) Visit(n Node, _ NodeInfo) VisitResponse {
	switch n := n.(type) {
	case *AccountNode:
		v.currentAccount = &accountData{
			balance: n.Info().Balance,
			nonce:   n.Info().Nonce,
			code:    n.Info().CodeHash,
			storage: make(map[common.Key]common.Value),
		}
		v.accounts[n.Address()] = *v.currentAccount
	case *ValueNode:
		v.currentAccount.storage[n.Key()] = n.Value()
	default:
	}

	if v.counter%100 == 0 && interrupt.IsCancelled(v.ctx) {
		return VisitResponseAbort
	}
	v.counter++

	return VisitResponseContinue
}

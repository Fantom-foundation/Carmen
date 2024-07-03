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
	"github.com/Fantom-foundation/Carmen/go/common/interrupt"
	"github.com/Fantom-foundation/Carmen/go/common/witness"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"golang.org/x/exp/maps"
	"math/rand"
)

//go:generate mockgen -source verification.go -destination verification_mocks.go -package proof

// ErrInvalidProof is an error returned when a witness proof is not invalid.
const ErrInvalidProof = common.ConstError("invalid proof")

// VerifyArchiveTrie verifies the consistency of witness proofs for an archive trie.
// It reads the trie for each block within the input range.
// It gets account and storage slots and extracts a witness proof for each account and its storage.
// It is checked that values in the database and the proof match.
func VerifyArchiveTrie(ctx context.Context, dir string, config mpt.MptConfig, from, to int, observer mpt.VerificationObserver) error {
	trie, err := mpt.OpenArchiveTrie(dir, config, mpt.NodeCacheConfig{}, mpt.ArchiveConfig{})
	if err != nil {
		return err
	}

	observer.StartVerification()
	err = errors.Join(
		verifyArchiveTrie(ctx, trie, from, to, observer),
		trie.Close(),
	)
	observer.EndVerification(err)
	return err
}

func verifyArchiveTrie(ctx context.Context, trie verifiableArchiveTrie, from, to int, observer mpt.VerificationObserver) error {
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
		trieView := &archiveTrie{trie: trie, block: uint64(i)}
		observer.Progress(fmt.Sprintf("Verifying block: %d ", i))
		if err := verifyTrie(ctx, trieView, observer); err != nil {
			return err
		}
	}

	return nil
}

// VerifyLiveTrie verifies the consistency of witness proofs for a live trie.
// It reads the trie for the head block and loads accounts and storage slots.
// It extracts witness proofs for these accounts and its storage,
// and checks that values in the proof and the database match.
func VerifyLiveTrie(ctx context.Context, dir string, config mpt.MptConfig, observer mpt.VerificationObserver) error {
	trie, err := mpt.OpenFileLiveTrie(dir, config, mpt.NodeCacheConfig{})
	if err != nil {
		return err
	}

	observer.StartVerification()
	err = errors.Join(
		verifyTrie(ctx, trie, observer),
		trie.Close(),
	)
	observer.EndVerification(err)
	return err
}

func verifyTrie(ctx context.Context, trie verifiableTrie, observer mpt.VerificationObserver) error {
	rootHash, hints, err := trie.UpdateHashes()
	if hints != nil {
		hints.Release()
	}
	if err != nil {
		return err
	}

	observer.Progress("Collecting and Verifying proofs ... ")
	visitor := accountVerifyingVisitor{
		ctx:       ctx,
		rootHash:  rootHash,
		trie:      trie,
		observer:  observer,
		logWindow: 1000_000,
	}
	if err := trie.VisitTrie(&visitor); err != nil || visitor.err != nil {
		return errors.Join(err, visitor.err)
	}

	const numAddresses = 1000
	return verifyEmptyAccount(trie, rootHash, numAddresses, observer)
}

// verifyEmptyAccount verifies the consistency of witness proofs for empty accounts that are not present in the trie.
func verifyEmptyAccount(trie verifiableTrie, rootHash common.Hash, numAddresses int, observer mpt.VerificationObserver) error {
	observer.Progress(fmt.Sprintf("Veryfing %d empty addresses...", numAddresses))
	addresses, err := generateUnusedAddresses(trie, numAddresses)
	if err != nil {
		return err
	}
	for _, addr := range addresses {
		proof, err := trie.CreateWitnessProof(addr)
		if err != nil {
			return err
		}
		if !proof.IsValid() {
			return ErrInvalidProof
		}
		// expect an empty account
		if err := verifyAccount(rootHash, proof, addr, mpt.AccountInfo{}); err != nil {
			return err
		}
	}

	return nil
}

// verifyUnusedStorageSlots verifies the consistency of witness proofs for empty storage that are not present in the trie.
func verifyUnusedStorageSlots(trie verifiableTrie, rootHash common.Hash, addr common.Address) error {
	const numKeys = 10
	keys, err := generateUnusedKeys(trie, numKeys, addr)
	if err != nil {
		return err
	}
	proof, err := trie.CreateWitnessProof(addr, keys...)
	if err != nil {
		return err
	}
	if !proof.IsValid() {
		return ErrInvalidProof
	}

	if err := verifyStorage(rootHash, proof, addr, keys, nil); err != nil {
		return err
	}

	return nil
}

// verifyAccount verifies the consistency between the input witness proofs and the account.
func verifyAccount(root common.Hash, proof witness.Proof, addr common.Address, info mpt.AccountInfo) error {
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

// verifyStorage verifies the consistency between the input witness proofs and the storage.
func verifyStorage(root common.Hash, proof witness.Proof, addr common.Address, keys []common.Key, storage map[common.Key]common.Value) error {
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

// generateUnusedAddresses generates a slice of addresses that do not appear in the input MPT.
func generateUnusedAddresses(trie verifiableTrie, number int) ([]common.Address, error) {
	res := make([]common.Address, 0, number)
	for len(res) < number {
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

// generateUnusedKeys generates a slice of keys  that do not appear in the input MPT under the given account.
func generateUnusedKeys(trie verifiableTrie, number int, address common.Address) ([]common.Key, error) {
	res := make([]common.Key, 0, number)
	for len(res) < number {
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

// accountVerifyingVisitor is a visitor that verifies the consistency of witness proofs for a live trie.
// It collects account and storage slots and extracts witness proofs for each account and its storage.
// It checks that values in the database and the proof match.
// The process can be interrupted by the input context.
type accountVerifyingVisitor struct {
	ctx      context.Context
	rootHash common.Hash
	trie     verifiableTrie
	observer mpt.VerificationObserver

	err error

	logWindow      int
	counter        int
	numAddresses   int
	currentAddress common.Address
	storage        map[common.Key]common.Value
}

func (v *accountVerifyingVisitor) Visit(n mpt.Node, _ mpt.NodeInfo) mpt.VisitResponse {
	if v.counter%100 == 0 && interrupt.IsCancelled(v.ctx) {
		v.err = interrupt.ErrCanceled
		return mpt.VisitResponseAbort
	}
	v.counter++

	switch n := n.(type) {
	case *mpt.AccountNode:
		proof, err := v.trie.CreateWitnessProof(n.Address())
		if err != nil {
			v.err = err
			return mpt.VisitResponseAbort
		}
		if !proof.IsValid() {
			v.err = ErrInvalidProof
			return mpt.VisitResponseAbort
		}

		if err := verifyAccount(v.rootHash, proof, n.Address(), n.Info()); err != nil {
			v.err = err
			return mpt.VisitResponseAbort
		}

		storageVisitor := storageVerifyingVisitor{
			ctx:            v.ctx,
			rootHash:       v.rootHash,
			trie:           v.trie,
			currentAddress: n.Address(),
			storage:        make(map[common.Key]common.Value)}

		if err := v.trie.VisitAccount(n.Address(), &storageVisitor); err != nil || storageVisitor.err != nil {
			v.err = errors.Join(err, storageVisitor.err)
			return mpt.VisitResponseAbort
		}

		// verify remaining storage if not done inside the visitor
		if len(storageVisitor.storage) > 0 {
			if err := storageVisitor.verifyStorage(); err != nil {
				v.err = err
				return mpt.VisitResponseAbort
			}
		}

		// add empty storages check
		if err := verifyUnusedStorageSlots(v.trie, v.rootHash, n.Address()); err != nil {
			v.err = err
			return mpt.VisitResponseAbort
		}

		v.numAddresses++
		if (v.numAddresses)%v.logWindow == 0 {
			v.observer.Progress(fmt.Sprintf("  ... verified %d addresses", v.numAddresses))
		}

		return mpt.VisitResponsePrune // this account resolved, do not go deeper
	}

	return mpt.VisitResponseContinue
}

// storageVerifyingVisitor is a visitor that verifies the consistency of witness proofs for storage slots.
// It collects storage slots and extracts witness proofs for each storage slot.
// It checks that values in the database and the proof match.
// The process can be interrupted by the input context.
// Storage keys are verified in batches of 10 to save on memory and allowing for responsive cancellation.
type storageVerifyingVisitor struct {
	ctx            context.Context
	rootHash       common.Hash
	trie           verifiableTrie
	counter        int
	currentAddress common.Address
	storage        map[common.Key]common.Value

	err error
}

func (v *storageVerifyingVisitor) Visit(n mpt.Node, _ mpt.NodeInfo) mpt.VisitResponse {
	if v.counter%100 == 0 && interrupt.IsCancelled(v.ctx) {
		v.err = interrupt.ErrCanceled
		return mpt.VisitResponseAbort
	}
	v.counter++

	switch n := n.(type) {
	case *mpt.ValueNode:
		v.storage[n.Key()] = n.Value()
	}

	// when ten keys accumulate, verify the storage
	if len(v.storage) >= 10 {
		if err := v.verifyStorage(); err != nil {
			v.err = err
			return mpt.VisitResponseAbort
		}
	}

	return mpt.VisitResponseContinue
}

// verifyStorage verifies the consistency of witness proofs for storage slots.
func (v *storageVerifyingVisitor) verifyStorage() error {
	keys := maps.Keys(v.storage)

	proof, err := v.trie.CreateWitnessProof(v.currentAddress, keys...)
	if err != nil {
		return err
	}
	if !proof.IsValid() {
		return ErrInvalidProof
	}

	if err := verifyStorage(v.rootHash, proof, v.currentAddress, keys, v.storage); err != nil {
		return err
	}

	v.storage = make(map[common.Key]common.Value)
	return nil
}

// verifiableTrie is an interface for a trie that can provide witness proofs
// and trie properties to validate the witness proofs against the trie.
type verifiableTrie interface {

	// GetAccountInfo returns the account info for the given address.
	GetAccountInfo(addr common.Address) (mpt.AccountInfo, bool, error)

	// GetValue returns the value for the given address and key.
	GetValue(addr common.Address, key common.Key) (common.Value, error)

	//VisitTrie visits the trie nodes with the given visitor.
	VisitTrie(visitor mpt.NodeVisitor) error

	// VisitAccount visits the account's storage nodes with the given visitor.
	VisitAccount(address common.Address, visitor mpt.NodeVisitor) error

	// UpdateHashes updates the hashes of the trie, and returns the resulting root hash.
	UpdateHashes() (common.Hash, *mpt.NodeHashes, error)

	// CreateWitnessProof creates a witness proof for the given address and keys.
	CreateWitnessProof(common.Address, ...common.Key) (witness.Proof, error)
}

// verifiableArchiveTrie is an interface for an archive trie that can provide witness proofs
// and trie properties to validate the witness proofs against the trie.
type verifiableArchiveTrie interface {

	// GetAccountInfo returns the account info for the given address at the given block.
	GetAccountInfo(block uint64, addr common.Address) (mpt.AccountInfo, bool, error)

	// GetStorage returns the value for the given address and key at the given block.
	GetStorage(block uint64, addr common.Address, key common.Key) (common.Value, error)

	// VisitTrie visits the trie nodes with the given visitor at the given block.
	VisitTrie(block uint64, visitor mpt.NodeVisitor) error

	// VisitAccount visits the account's storage nodes with the given visitor at the given block.
	VisitAccount(block uint64, address common.Address, visitor mpt.NodeVisitor) error

	// GetHash returns the root hash of the trie at the given block.
	GetHash(block uint64) (common.Hash, error)

	// CreateWitnessProof creates a witness proof for the given address and keys at the given block.
	CreateWitnessProof(block uint64, address common.Address, keys ...common.Key) (witness.Proof, error)

	// GetBlockHeight returns the block height of the trie.
	GetBlockHeight() (uint64, bool, error)
}

// archiveTrie is a wrapper for an archive trie that implements the verifiableTrie interface.
// It bounds the archive trie to a specific block.
type archiveTrie struct {
	trie  verifiableArchiveTrie
	block uint64
}

func (v *archiveTrie) GetAccountInfo(addr common.Address) (mpt.AccountInfo, bool, error) {
	return v.trie.GetAccountInfo(v.block, addr)
}

func (v *archiveTrie) GetValue(addr common.Address, key common.Key) (common.Value, error) {
	return v.trie.GetStorage(v.block, addr, key)
}

func (v *archiveTrie) VisitTrie(visitor mpt.NodeVisitor) error {
	return v.trie.VisitTrie(v.block, visitor)
}

func (v *archiveTrie) VisitAccount(address common.Address, visitor mpt.NodeVisitor) error {
	return v.trie.VisitAccount(v.block, address, visitor)
}

func (v *archiveTrie) UpdateHashes() (common.Hash, *mpt.NodeHashes, error) {
	hash, err := v.trie.GetHash(v.block)
	return hash, nil, err
}

func (v *archiveTrie) CreateWitnessProof(address common.Address, keys ...common.Key) (witness.Proof, error) {
	return v.trie.CreateWitnessProof(v.block, address, keys...)
}

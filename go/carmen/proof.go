// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package carmen

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/immutable"
	"github.com/Fantom-foundation/Carmen/go/common/witness"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
)

// Bytes is an alias for immutable.Bytes.
type Bytes = immutable.Bytes

// CreateWitnessProofFromNodes creates a witness proof from a list of strings.
// Each string is an RLP node of the witness proof.
func CreateWitnessProofFromNodes(elements ...Bytes) WitnessProof {
	proof := mpt.CreateWitnessProofFromNodes(elements)
	return witnessProof{proof}
}

// WitnessProof is an interface for witness proofs. A witness proof is a data structure that
// contains a witness for a subset of the state. The witness proof can be used to
// extract information, such as account balances, nonces, code hashes,
// and storage slots. The witness proof is self-contained and does not require access to
// the full state to extract information.
type WitnessProof interface {

	// Extract extracts a sub-proof for a given account and selected storage locations from this proof.
	// It returns a copy that contains only the data necessary for proving the given address and storage keys.
	// The resulting proof covers proofs for the intersection of the requested properties (account information and slots)
	// and the properties covered by this proof. The second return parameter indicates whether everything that
	// was requested could be covered. If so it is set to true, otherwise it is set to false.
	Extract(root Hash, address Address, keys ...Key) (WitnessProof, bool)

	// IsValid checks that this proof is self-consistent. If the result is true, the proof can be used
	// for extracting verified information. If false, the proof is corrupted and should be discarded.
	IsValid() bool

	// GetElements returns serialised elements of the witness proof.
	GetElements() []Bytes

	// GetAccountElements returns serialised elements of the witness proof for a selected account.
	GetAccountElements(root Hash, address Address) ([]Bytes, bool)

	// GetStorageElements returns serialised elements of the witness proof for a selected
	// storage location within an account.
	// The resulting elements contains only the storage part of the account.
	// For this reason, the second parameter of this method returns the storage root for this storage
	// as any proving and other operations on the resulting proof must be done related to the storage root.
	// This method returns a copy that contains only the data necessary for proving storage keys.
	// The third return parameter indicates whether everything that was requested could be covered.
	// If so, it is set to true, otherwise it is set to false.
	GetStorageElements(root Hash, address Address, key Key) ([]Bytes, Hash, bool)

	// GetBalance extracts a balance from the witness proof for the input root hash and the address.
	// If the witness proof contains the requested account for the input address for the given root hash, it returns its balance.
	// If the proof does not cover the requested account, it returns false.
	// The method may return an error if the proof is invalid.
	GetBalance(root Hash, address Address) (Amount, bool, error)

	// GetNonce extracts a nonce from the witness proof for the input root hash and the address.
	// If the witness proof contains the account for the input address, it returns its nonce.
	// If the proof does not contain the account, it returns false.
	// The method may return an error if the proof is invalid.
	GetNonce(root Hash, address Address) (uint64, bool, error)

	// GetCodeHash extracts a code hash from the witness proof for the input root hash and the address.
	// If the witness proof contains the account for the input address, it returns its code hash.
	// If the proof does not contain the account, it returns false.
	// The method may return an error if the proof is invalid.
	GetCodeHash(root Hash, address Address) (Hash, bool, error)

	// GetState extracts a storage slot from the witness proof for the input root hash, account address and the storage key.
	// If the witness proof contains the input storage slot for the input key, it returns its value.
	// If the proof does not contain the slot, it returns false.
	// The method may return an error if the proof is invalid.
	GetState(root Hash, address Address, key Key) (Value, bool, error)

	// AllStatesZero checks that all storage slots are empty for the input root hash,
	// account address and the storage key range. If the witness proof contains all empty slots
	// for the input key range, it returns true. If there is at least one non-empty slot,
	// it returns false. If the proof is not complete, it returns unknown. An incomplete proof
	// is a proof where the input address or key terminates in a node that is not a correct
	// value node, or an empty node.
	AllStatesZero(root Hash, address Address, from, to Key) (Tribool, error)

	// AllAddressesEmpty checks that all accounts are empty for the input root hash and the address range.
	// If the witness proof contains all empty accounts for the input address range, it returns true.
	// An empty account is an account that contains a zero balance, nonce, and code hash.
	// If there is at least one non-empty account, it returns false. If the proof is not complete,
	// it returns unknown. An incomplete proof is a proof where the input address terminates in a node
	// that is not a correct account node.
	AllAddressesEmpty(root Hash, from, to Address) (Tribool, error)
}

type witnessProof struct {
	proof witness.Proof
}

func (w witnessProof) Extract(root Hash, address Address, keys ...Key) (WitnessProof, bool) {
	commonKeys := make([]common.Key, len(keys))
	for i, k := range keys {
		commonKeys[i] = common.Key(k)
	}

	proof, complete := w.proof.Extract(common.Hash(root), common.Address(address), commonKeys...)
	return witnessProof{proof}, complete
}

func (w witnessProof) GetElements() []Bytes {
	return w.proof.GetElements()
}

func (w witnessProof) GetAccountElements(root Hash, address Address) ([]Bytes, bool) {
	resProof, complete := w.proof.GetAccountElements(common.Hash(root), common.Address(address))
	return resProof, complete
}

func (w witnessProof) GetStorageElements(root Hash, address Address, key Key) ([]Bytes, Hash, bool) {
	resProof, storageRoot, complete := w.proof.GetStorageElements(common.Hash(root), common.Address(address), common.Key(key))
	return resProof, Hash(storageRoot), complete
}

func (w witnessProof) IsValid() bool {
	return w.proof.IsValid()
}

func (w witnessProof) GetBalance(root Hash, address Address) (Amount, bool, error) {
	return w.proof.GetBalance(common.Hash(root), common.Address(address))
}

func (w witnessProof) GetNonce(root Hash, address Address) (uint64, bool, error) {
	nonce, complete, err := w.proof.GetNonce(common.Hash(root), common.Address(address))
	return nonce.ToUint64(), complete, err
}

func (w witnessProof) GetCodeHash(root Hash, address Address) (Hash, bool, error) {
	hash, complete, err := w.proof.GetCodeHash(common.Hash(root), common.Address(address))
	return Hash(hash), complete, err
}

func (w witnessProof) GetState(root Hash, address Address, key Key) (Value, bool, error) {
	value, complete, err := w.proof.GetState(common.Hash(root), common.Address(address), common.Key(key))
	return Value(value), complete, err
}

func (w witnessProof) AllStatesZero(root Hash, address Address, from, to Key) (Tribool, error) {
	tri, err := w.proof.AllStatesZero(common.Hash(root), common.Address(address), common.Key(from), common.Key(to))
	return Tribool(tri), err
}

func (w witnessProof) AllAddressesEmpty(root Hash, from, to Address) (Tribool, error) {
	tri, err := w.proof.AllAddressesEmpty(common.Hash(root), common.Address(from), common.Address(to))
	return Tribool(tri), err
}

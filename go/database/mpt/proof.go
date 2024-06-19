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
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/common/tribool"
)

//go:generate mockgen -source proof.go -destination proof_mocks.go -package witness

// rlpEncodedNode is an RLP encoded MPT node.
type rlpEncodedNode []byte

// proofDb is a database of RLP encoded MPT nodes and their hashes that represent witness proofs.
type proofDb map[common.Hash]rlpEncodedNode

// WitnessProof represents a witness proof.
// It contains a database of MPT nodes and their hashes.
type WitnessProof struct {
	proofDb
}

// CreateWitnessProof creates a witness proof for the input account address
// and possibly storage slots of the same account under the input storage keys.
// This method may return an error when it occurs in the underlying database.
func CreateWitnessProof(nodeSource NodeSource, root *NodeReference, address common.Address, keys ...common.Key) (WitnessProof, error) {
	panic("not implemented")
}

// Add merges the input witness proof into the current witness proof.
func (p WitnessProof) Add(other WitnessProof) {
	panic("not implemented")
}

// Extract extracts a sub-proof for a given account and selected storage locations from this proof.
// It returns a copy that contains only the data necessary for proofing the given address and storage keys.
// The resulting proof covers proofs for the intersection of the requested properties (account information and slots)
// and the properties covered by this proof. The second return parameter indicates whether everything that was requested could be covered. If so
// it is set to true, otherwise it is set to false.
func (p WitnessProof) Extract(root common.Hash, address common.Address, keys ...common.Key) (WitnessProof, bool) {
	panic("not implemented")
}

// IsValid checks that this proof is self-consistent. If the result is true, the proof can be used
// for extracting verified information. If false, the proof is corrupted and should be discarded.
func (p WitnessProof) IsValid() bool {
	panic("not implemented")
}

// GetAccountInfo extracts an account info from the witness proof for the input root hash and the address.
// If the witness proof contains an account for the input address, it returns its information.
// If the proof does not contain an account, it returns false.
// The method may return an error if the proof is invalid.
func (p WitnessProof) GetAccountInfo(root common.Hash, address common.Address) (AccountInfo, bool, error) {
	panic("not implemented")
}

// GetBalance extracts a balance from the witness proof for the input root hash and the address.
// If the witness proof contains the requested account for the input address for the given root hash, it returns its balance.
// If the proof does not cover the requested account, it returns false.
// The method may return an error if the proof is invalid.
func (p WitnessProof) GetBalance(root common.Hash, address common.Address) (amount.Amount, bool, error) {
	panic("not implemented")
}

// GetNonce extracts a nonce from the witness proof for the input root hash and the address.
// If the witness proof contains the account for the input address, it returns its nonce.
// If the proof does not contain the account, it returns false.
// The method may return an error if the proof is invalid.
func (p WitnessProof) GetNonce(root common.Hash, address common.Address) (common.Nonce, bool, error) {
	panic("not implemented")
}

// GetCodeHash extracts a code hash from the witness proof for the input root hash and the address.
// If the witness proof contains the account for the input address, it returns its code hash.
// If the proof does not contain the account, it returns false.
// The method may return an error if the proof is invalid.
func (p WitnessProof) GetCodeHash(root common.Hash, address common.Address) (common.Hash, bool, error) {
	panic("not implemented")
}

// GetState extracts a storage slot from the witness proof for the input root hash, account address and the storage key.
// If the witness proof contains the input storage slot for the input key, it returns its value.
// If the proof does not contain the slot, it returns false.
// The method may return an error if the proof is invalid.
func (p WitnessProof) GetState(root common.Hash, address common.Address, key common.Key) (common.Value, bool, error) {
	panic("not implemented")
}

// AllStatesZero checks that all storage slots are empty for the input root hash,
// account address and the storage key range. If the witness proof contains all empty slots
// for the input key range, it returns true. If there is at least one non-empty slot,
// it returns false. If the proof is not complete, it returns unknown. An incomplete proof
// is a proof where the input address or key terminates in a node that is not a correct
// value node, or an empty node.
func (p WitnessProof) AllStatesZero(root common.Hash, address common.Address, from, to common.Key) (tribool.Tribool, error) {
	panic("not implemented")
}

// AllAddressesEmpty checks that all accounts are empty for the input root hash and the address range.
// If the witness proof contains all empty accounts for the input address range, it returns true.
// An empty account is an account that contains a zero balance, nonce, and code hash.
// If there is at least one non-empty account, it returns false. If the proof is not complete,
// it returns unknown. An incomplete proof is a proof where the input address terminates in a node
// that is not a correct account node.
func (p WitnessProof) AllAddressesEmpty(root common.Hash, from, to common.Address) (tribool.Tribool, error) {
	panic("not implemented")
}

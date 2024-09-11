// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package witness

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/common/immutable"
	"github.com/Fantom-foundation/Carmen/go/common/tribool"
)

//go:generate mockgen -source proof.go -destination proof_mocks.go -package witness

// Proof is an interface for witness proofs. A witness proof is a data structure that
// contains a witness for a subset of the state. The witness proof can be used to
// extract information, such as account balances, nonces, code hashes,
// and storage slots. The witness proof is self-contained and does not require access to
// the full state to extract information.
type Proof interface {

	// Extract extracts a sub-proof for a given account and selected storage locations from this proof.
	// It returns a copy that contains only the data necessary for proving the given address and storage keys.
	// The resulting proof covers proofs for the intersection of the requested properties (account information and slots)
	// and the properties covered by this proof. The second return parameter indicates whether everything that
	// was requested could be covered. If so it is set to true, otherwise it is set to false.
	Extract(root common.Hash, address common.Address, keys ...common.Key) (Proof, bool)

	// IsValid checks that this proof is self-consistent. If the result is true, the proof can be used
	// for extracting verified information. If false, the proof is corrupted and should be discarded.
	IsValid() bool

	// GetElements returns serialised elements of the witness proof.
	GetElements() []immutable.Bytes

	// GetStorageElements returns serialised elements of the witness proof for a given account
	// and selected storage locations from this proof.
	// The resulting elements contains only the storage part of the account.
	// For this reason, the second parameter of this method returns the storage root for this storage
	// as any proving and other operations on the resulting proof must be done related to the storage root.
	// This method returns a copy that contains only the data necessary for proving storage keys.
	// The third return parameter indicates whether everything that was requested could be covered.
	// If so, it is set to true, otherwise it is set to false.
	GetStorageElements(root common.Hash, address common.Address, keys ...common.Key) ([]immutable.Bytes, common.Hash, bool)

	// GetBalance extracts a balance from the witness proof for the input root hash and the address.
	// If the witness proof contains the requested account for the input address for the given root hash, it returns its balance.
	// If the proof does not cover the requested account, it returns false.
	// The method may return an error if the proof is invalid.
	GetBalance(root common.Hash, address common.Address) (amount.Amount, bool, error)

	// GetNonce extracts a nonce from the witness proof for the input root hash and the address.
	// If the witness proof contains the account for the input address, it returns its nonce.
	// If the proof does not contain the account, it returns false.
	// The method may return an error if the proof is invalid.
	GetNonce(root common.Hash, address common.Address) (common.Nonce, bool, error)

	// GetCodeHash extracts a code hash from the witness proof for the input root hash and the address.
	// If the witness proof contains the account for the input address, it returns its code hash.
	// If the proof does not contain the account, it returns false.
	// The method may return an error if the proof is invalid.
	GetCodeHash(root common.Hash, address common.Address) (common.Hash, bool, error)

	// GetState extracts a storage slot from the witness proof for the input root hash, account address and the storage key.
	// If the witness proof contains the input storage slot for the input key, it returns its value.
	// If the proof does not contain the slot, it returns false.
	// The method may return an error if the proof is invalid.
	GetState(root common.Hash, address common.Address, key common.Key) (common.Value, bool, error)

	// AllStatesZero checks that all storage slots are empty for the input root hash,
	// account address and the storage key range. If the witness proof contains all empty slots
	// for the input key range, it returns true. If there is at least one non-empty slot,
	// it returns false. If the proof is not complete, it returns unknown. An incomplete proof
	// is a proof where the input address or key terminates in a node that is not a correct
	// value node, or an empty node.
	AllStatesZero(root common.Hash, address common.Address, from, to common.Key) (tribool.Tribool, error)

	// AllAddressesEmpty checks that all accounts are empty for the input root hash and the address range.
	// If the witness proof contains all empty accounts for the input address range, it returns true.
	// An empty account is an account that contains a zero balance, nonce, and code hash.
	// If there is at least one non-empty account, it returns false. If the proof is not complete,
	// it returns unknown. An incomplete proof is a proof where the input address terminates in a node
	// that is not a correct account node.
	AllAddressesEmpty(root common.Hash, from, to common.Address) (tribool.Tribool, error)
}

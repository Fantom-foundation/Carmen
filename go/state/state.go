// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package state

//go:generate mockgen -source state.go -destination state_mock.go -package state

import (
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/witness"
)

// NoArchiveError is an error returned by implementation of the State interface
// for archive operations if no archive is maintained by this implementation.
const NoArchiveError = common.ConstError("state does not maintain archive data")

// State interfaces provides access to accounts and smart contract values memory.
type State interface {
	// Exists obtains the current state of the provided account.
	Exists(address common.Address) (bool, error)

	// GetBalance provides balance for the input account address.
	GetBalance(address common.Address) (common.Balance, error)

	// GetNonce returns nonce of the account for the  input account address.
	GetNonce(address common.Address) (common.Nonce, error)

	// GetStorage returns the memory slot for the account address (i.e. the contract) and the memory location key.
	GetStorage(address common.Address, key common.Key) (common.Value, error)

	// GetCode returns code of the contract for the input contract address.
	GetCode(address common.Address) ([]byte, error)

	// GetCodeSize returns the length of the contract for the input contract address.
	GetCodeSize(address common.Address) (int, error)

	// GetCodeHash returns the hash of the code of the input contract address.
	GetCodeHash(address common.Address) (common.Hash, error)

	// Apply applies the provided updates to the state content.
	Apply(block uint64, update common.Update) error

	// GetHash hashes the values.
	GetHash() (common.Hash, error)

	// Flush writes all committed content to disk.
	Flush() error

	// Close flushes the store and closes it.
	Close() error

	// GetMemoryFootprint computes an approximation of the memory used by this state.
	GetMemoryFootprint() *common.MemoryFootprint

	// GetArchiveState provides a historical State view for given block.
	// An error is returned if the archive is not enabled or if it is empty.
	GetArchiveState(block uint64) (State, error)

	// GetArchiveBlockHeight provides the block height available in the archive. If
	// there is no block in the archive, the empty flag is returned.
	// An error is returned if the archive is not enabled or an IO issue occurred.
	GetArchiveBlockHeight() (height uint64, empty bool, err error)

	// Check checks the state of the DB and reports an error if issues have been
	// encountered.
	// Check should be called periodically to validate all interactions
	// with a State instance.
	// If an error is reported, all operations since the
	// last successful check need to be considered invalid.
	Check() error

	// CreateWitnessProof creates a witness proof for the given account and keys.
	// Error may be produced when it occurs in the underlying database;
	// otherwise, the proof is returned.
	CreateWitnessProof(address common.Address, keys ...common.Key) (witness.Proof, error)

	// States can be snapshotted.
	backend.Snapshotable
}

type LiveDB interface {
	Exists(address common.Address) (bool, error)
	GetBalance(address common.Address) (balance common.Balance, err error)
	GetNonce(address common.Address) (nonce common.Nonce, err error)
	GetStorage(address common.Address, key common.Key) (value common.Value, err error)
	GetCode(address common.Address) (value []byte, err error)
	GetCodeSize(address common.Address) (size int, err error)
	GetCodeHash(address common.Address) (hash common.Hash, err error)
	GetHash() (hash common.Hash, err error)
	Apply(block uint64, update common.Update) (archiveUpdateHints common.Releaser, err error)
	Flush() error
	Close() error
	common.MemoryFootprintProvider

	// getSnapshotableComponents lists all components required to back-up or restore
	// for snapshotting this schema. Returns nil if snapshotting is not supported.
	GetSnapshotableComponents() []backend.Snapshotable

	// Called after synching to a new state, requisting the schema to update cached
	// values or tables not covered by the snapshot synchronization.
	RunPostRestoreTasks() error
}

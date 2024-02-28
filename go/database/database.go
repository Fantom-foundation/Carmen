package database

//go:generate mockgen -source database.go -destination database_mock.go -package database

import (
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// NoArchiveError is an error returned by implementation of the Database interface
// for archive operations if no archive is maintained by this implementation.
const NoArchiveError = common.ConstError("database does not maintain archive data")

// State is the immutable state of a blockchain's world state for a single block.
// It provides access to all present accounts and their balances, nonces, codes, and
// storage states. Furthermore, it provides access to a cryptographic hash certifying
// the entire state's content.
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

	// GetHash gets a cryptographic hash of this state.
	GetHash() (common.Hash, error)
}

// Database interfaces provides access to accounts and smart contract values memory.
type Database interface {
	// A database provide access to the head block (aka. LiveDB state).
	State

	// Apply applies the provided updates to the database content.
	Apply(block uint64, update common.Update) error

	// Flush writes all committed content to disk.
	Flush() error

	// Close flushes the store and closes it.
	Close() error

	// GetMemoryFootprint computes an approximation of the memory used by this DB.
	GetMemoryFootprint() *common.MemoryFootprint

	// GetArchiveState provides a historical State view for given block.
	// An error is returned if the archive is not enabled or if it is empty.
	GetArchiveState(block uint64) (State, error)

	// GetArchiveBlockHeight provides the block height available in the archive. If
	// there is no block in the archive, the empty flag is returned.
	// An error is returned if the archive is not enabled or an IO issue occurred.
	GetArchiveBlockHeight() (height uint64, empty bool, err error)

	// States can be snapshotted.
	// TODO: make this optional
	backend.Snapshotable
}

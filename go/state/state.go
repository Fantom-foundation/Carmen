package state

//go:generate mockgen -source state.go -destination mock_state.go -package state

import (
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
)

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

	// States can be snapshotted.
	backend.Snapshotable
}

// directUpdateState is an extended version of the State interface adding support for
// triggering and mocking individual state updates. All its additional members are
// private and not intended to be used outside this package.
type directUpdateState interface {
	State

	// CreateAccount creates a new account with the given address.
	CreateAccount(address common.Address) error

	// DeleteAccount deletes the account with the given address.
	DeleteAccount(address common.Address) error

	// SetBalance provides balance for the input account address.
	SetBalance(address common.Address, balance common.Balance) error

	// SetNonce updates nonce of the account for the  input account address.
	SetNonce(address common.Address, nonce common.Nonce) error

	// SetStorage updates the memory slot for the account address (i.e. the contract) and the memory location key.
	SetStorage(address common.Address, key common.Key, value common.Value) error

	// SetCode updates code of the contract for the input contract address.
	SetCode(address common.Address, code []byte) error
}

// applyUpdate distributes the updates combined in an Update struct to individual update calls.
// This is intended as the default implementation for the Go, C++, and Mock state. However,
// implementations may choose to implement specialized versions.
func applyUpdate(s directUpdateState, update common.Update) error {
	for _, addr := range update.DeletedAccounts {
		if err := s.DeleteAccount(addr); err != nil {
			return err
		}
	}
	for _, addr := range update.CreatedAccounts {
		if err := s.CreateAccount(addr); err != nil {
			return err
		}
	}
	for _, change := range update.Balances {
		if err := s.SetBalance(change.Account, change.Balance); err != nil {
			return err
		}
	}
	for _, change := range update.Nonces {
		if err := s.SetNonce(change.Account, change.Nonce); err != nil {
			return err
		}
	}
	for _, change := range update.Codes {
		if err := s.SetCode(change.Account, change.Code); err != nil {
			return err
		}
	}
	for _, change := range update.Slots {
		if err := s.SetStorage(change.Account, change.Key, change.Value); err != nil {
			return err
		}
	}
	return nil
}

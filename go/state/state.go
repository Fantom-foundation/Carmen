package state

//go:generate mockgen -source state.go -destination mock_state.go -package state

import (
	"github.com/Fantom-foundation/Carmen/go/common"
)

// State interfaces provides access to accounts and smart contract values memory.
type State interface {
	// GetAccountState obtains the current state of the provided account.
	GetAccountState(address common.Address) (common.AccountState, error)

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
	Apply(block uint64, update Update) error

	// GetHash hashes the values.
	GetHash() (common.Hash, error)

	// Flush writes all committed content to disk.
	Flush() error

	// Close flushes the store and closes it.
	Close() error

	// GetMemoryFootprint computes an approximation of the memory used by this state.
	GetMemoryFootprint() *common.MemoryFootprint
}

// directUpdateState is an extended version of the State interface adding support for
// triggering and mocking indivudual state updates. All its additional members are
// private and not intended to be used outside this package.
type directUpdateState interface {
	State

	// CreateAccount creates a new account with the given address.
	createAccount(address common.Address) error

	// DeleteAccount deletes the account with the given address.
	deleteAccount(address common.Address) error

	// SetBalance provides balance for the input account address.
	setBalance(address common.Address, balance common.Balance) error

	// SetNonce updates nonce of the account for the  input account address.
	setNonce(address common.Address, nonce common.Nonce) error

	// SetStorage updates the memory slot for the account address (i.e. the contract) and the memory location key.
	setStorage(address common.Address, key common.Key, value common.Value) error

	// SetCode updates code of the contract for the input contract address.
	setCode(address common.Address, code []byte) error
}

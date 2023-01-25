package state

//go:generate mockgen -source state.go -destination mock_state.go -package state State

import (
	"github.com/Fantom-foundation/Carmen/go/common"
)

// State interfaces provides access to accounts and smart contract values memory.
type State interface {
	// CreateAccount creates a new account with the given address.
	// Deprecated: Use Apply instead.
	CreateAccount(address common.Address) error

	// GetAccountState obtains the current state of the provided account.
	GetAccountState(address common.Address) (common.AccountState, error)

	// DeleteAccount deletes the account with the given address.
	// Deprecated: Use Apply instead.
	DeleteAccount(address common.Address) error

	// GetBalance provides balance for the input account address.
	GetBalance(address common.Address) (common.Balance, error)

	// SetBalance provides balance for the input account address.
	// Deprecated: Use Apply instead.
	SetBalance(address common.Address, balance common.Balance) error

	// GetNonce returns nonce of the account for the  input account address.
	GetNonce(address common.Address) (common.Nonce, error)

	// SetNonce updates nonce of the account for the  input account address.
	// Deprecated: Use Apply instead.
	SetNonce(address common.Address, nonce common.Nonce) error

	// GetStorage returns the memory slot for the account address (i.e. the contract) and the memory location key.
	GetStorage(address common.Address, key common.Key) (common.Value, error)

	// SetStorage updates the memory slot for the account address (i.e. the contract) and the memory location key.
	// Deprecated: Use Apply instead.
	SetStorage(address common.Address, key common.Key, value common.Value) error

	// GetCode returns code of the contract for the input contract address.
	GetCode(address common.Address) ([]byte, error)

	// GetCodeSize returns the length of the contract for the input contract address.
	GetCodeSize(address common.Address) (int, error)

	// SetCode updates code of the contract for the input contract address.
	// Deprecated: Use Apply instead.
	SetCode(address common.Address, code []byte) error

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

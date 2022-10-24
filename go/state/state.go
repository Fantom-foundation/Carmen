package state

//go:generate mockgen -source state.go -destination mock_state.go -package state State

import (
	"github.com/Fantom-foundation/Carmen/go/common"
)

// State interfaces provides access to accounts and smart contract values memory
type State interface {
	// CreateAccount creates a new account with the given address.
	CreateAccount(address common.Address) error

	// GetAccountState obtains the current state of the provided account.
	GetAccountState(address common.Address) (common.AccountState, error)

	// DeleteAccount deletes the account with the given address.
	DeleteAccount(address common.Address) error

	// GetBalance provides balance for the input account address.
	GetBalance(address common.Address) (common.Balance, error)

	// SetBalance provides balance for the input account address.
	SetBalance(address common.Address, balance common.Balance) error

	// GetNonce returns nonce of the account for the  input account address.
	GetNonce(address common.Address) (common.Nonce, error)

	// SetNonce updates nonce of the account for the  input account address.
	SetNonce(address common.Address, nonce common.Nonce) error

	// GetStorage returns the memory slot for the account address (i.e. the contract) and the memory location key.
	GetStorage(address common.Address, key common.Key) (common.Value, error)

	// SetStorage updates the memory slot for the account address (i.e. the contract) and the memory location key.
	SetStorage(address common.Address, key common.Key, value common.Value) error

	// GetCode returns code of the contract for the input contract address.
	GetCode(address common.Address) ([]byte, error)

	// SetCode updates code of the contract for the input contract address.
	SetCode(address common.Address, code []byte) error

	// GetHash hashes the values.
	GetHash() (common.Hash, error)
}

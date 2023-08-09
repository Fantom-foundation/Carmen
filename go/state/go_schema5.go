package state

import (
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/s4"
)

// goSchema5 implements a state utilizes an MPT based data structure that
// produces the same hashes as Ethereum's MPT implementation.
type goSchema5 struct {
	*s4.S4State
}

func newS5State(params Parameters, state *s4.S4State) (State, error) {
	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, err
	}
	return NewGoState(&goSchema5{
		S4State: state,
	}, arch, []func(){archiveCleanup}), nil
}

func NewGoMemoryS5State(params Parameters) (State, error) {
	state, err := s4.OpenGoMemoryState(params.Directory, s4.S5Config)
	if err != nil {
		return nil, err
	}
	return newS5State(params, state)
}

func NewGoFileS5State(params Parameters) (State, error) {
	state, err := s4.OpenGoFileState(params.Directory, s4.S5Config)
	if err != nil {
		return nil, err
	}
	return newS5State(params, state)
}

func (s *goSchema5) createAccount(address common.Address) error {
	return s.CreateAccount(address)
}

func (s *goSchema5) deleteAccount(address common.Address) error {
	return s.DeleteAccount(address)
}

func (s *goSchema5) setBalance(address common.Address, balance common.Balance) error {
	return s.SetBalance(address, balance)
}

func (s *goSchema5) setNonce(address common.Address, nonce common.Nonce) error {
	return s.SetNonce(address, nonce)
}

func (s *goSchema5) setStorage(address common.Address, key common.Key, value common.Value) error {
	return s.SetStorage(address, key, value)
}

func (s *goSchema5) setCode(address common.Address, code []byte) error {
	return s.SetCode(address, code)
}

func (s *goSchema5) getSnapshotableComponents() []backend.Snapshotable {
	return s.GetSnapshotableComponents()
}

func (s *goSchema5) runPostRestoreTasks() error {
	return s.RunPostRestoreTasks()
}

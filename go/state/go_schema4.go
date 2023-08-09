package state

import (
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/s4"
)

// goSchema4 implements a state utilizes an MPT based data structure. However, it is
// not binary compatible with the Ethereum variant of an MPT.
type goSchema4 struct {
	*s4.S4State
}

func newS4State(params Parameters, state *s4.S4State) (State, error) {
	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, err
	}
	return NewGoState(&goSchema4{
		S4State: state,
	}, arch, []func(){archiveCleanup}), nil
}

func NewGoMemoryS4State(params Parameters) (State, error) {
	state, err := s4.OpenGoMemoryState(params.Directory, s4.S4Config)
	if err != nil {
		return nil, err
	}
	return newS4State(params, state)
}

func NewGoFileS4State(params Parameters) (State, error) {
	state, err := s4.OpenGoFileState(params.Directory, s4.S4Config)
	if err != nil {
		return nil, err
	}
	return newS4State(params, state)
}

func (s *goSchema4) createAccount(address common.Address) error {
	return s.CreateAccount(address)
}

func (s *goSchema4) deleteAccount(address common.Address) error {
	return s.DeleteAccount(address)
}

func (s *goSchema4) setBalance(address common.Address, balance common.Balance) error {
	return s.SetBalance(address, balance)
}

func (s *goSchema4) setNonce(address common.Address, nonce common.Nonce) error {
	return s.SetNonce(address, nonce)
}

func (s *goSchema4) setStorage(address common.Address, key common.Key, value common.Value) error {
	return s.SetStorage(address, key, value)
}

func (s *goSchema4) setCode(address common.Address, code []byte) error {
	return s.SetCode(address, code)
}

func (s *goSchema4) getSnapshotableComponents() []backend.Snapshotable {
	return s.GetSnapshotableComponents()
}

func (s *goSchema4) runPostRestoreTasks() error {
	return s.RunPostRestoreTasks()
}

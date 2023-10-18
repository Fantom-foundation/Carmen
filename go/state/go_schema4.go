package state

import (
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt"
)

// goSchema4 implements a state utilizes an MPT based data structure. However, it is
// not binary compatible with the Ethereum variant of an MPT.
type goSchema4 struct {
	*mpt.MptState
}

func newS4State(params Parameters, state *mpt.MptState) (State, error) {
	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, err
	}
	return newGoState(&goSchema4{
		MptState: state,
	}, arch, []func(){archiveCleanup}), nil
}

func newGoMemoryS4State(params Parameters) (State, error) {
	state, err := mpt.OpenGoMemoryState(params.Directory, mpt.S4LiveConfig)
	if err != nil {
		return nil, err
	}
	return newS4State(params, state)
}

func newGoFileS4State(params Parameters) (State, error) {
	state, err := mpt.OpenGoFileState(params.Directory, mpt.S4LiveConfig)
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

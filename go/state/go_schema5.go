package state

import (
	"errors"
	"fmt"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt"
)

// goSchema5 implements a state utilizes an MPT based data structure that
// produces the same hashes as Ethereum's MPT implementation.
type goSchema5 struct {
	*mpt.MptState
}

func newS5State(params Parameters, state *mpt.MptState) (State, error) {
	if params.Archive == S4Archive {
		return nil, errors.Join(
			fmt.Errorf("%w: cannot use archive %v with schema 5", UnsupportedConfiguration, params.Archive),
			state.Close(),
		)
	}
	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, errors.Join(err, state.Close())
	}
	return newGoState(&goSchema5{
		MptState: state,
	}, arch, []func(){archiveCleanup}), nil
}

func newGoMemoryS5State(params Parameters) (State, error) {
	state, err := mpt.OpenGoMemoryState(params.Directory, mpt.S5LiveConfig)
	if err != nil {
		return nil, err
	}
	return newS5State(params, state)
}

func newGoFileS5State(params Parameters) (State, error) {
	state, err := mpt.OpenGoFileState(params.Directory, mpt.S5LiveConfig)
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

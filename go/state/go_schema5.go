package state

import (
	"errors"
	"fmt"

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

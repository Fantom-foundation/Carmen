package state

import (
	"errors"
	"fmt"
	"path/filepath"

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

func mptStateCapacity(params Parameters) int {
	if params.CacheCapacity == 0 {
		return mpt.DefaultMptStateCapacity
	}
	capacity := int(params.CacheCapacity / 512) // TODO use more accurate coefficient
	if capacity == 0 {
		capacity = mpt.MinMptStateCapacity
	}
	return capacity
}

func newGoMemoryS5State(params Parameters) (State, error) {
	state, err := mpt.OpenGoMemoryState(filepath.Join(params.Directory, "live"), mpt.S5LiveConfig, mptStateCapacity(params))
	if err != nil {
		return nil, err
	}
	return newS5State(params, state)
}

func newGoFileS5State(params Parameters) (State, error) {
	state, err := mpt.OpenGoFileState(filepath.Join(params.Directory, "live"), mpt.S5LiveConfig, mptStateCapacity(params))
	if err != nil {
		return nil, err
	}
	return newS5State(params, state)
}

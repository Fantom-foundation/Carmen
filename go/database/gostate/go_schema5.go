package gostate

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/Fantom-foundation/Carmen/go/database"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
)

// goSchema5 implements a state utilizes an MPT based data structure that
// produces the same hashes as Ethereum's MPT implementation.
type goSchema5 struct {
	*mpt.MptState
}

func newS5State(params database.Parameters, mptState *mpt.MptState) (database.Database, error) {
	if params.Archive == database.S4Archive {
		return nil, errors.Join(
			fmt.Errorf("%w: cannot use archive %v with schema 5", database.UnsupportedConfiguration, params.Archive),
			mptState.Close(),
		)
	}
	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, errors.Join(err, mptState.Close())
	}
	return newGoState(&goSchema5{
		MptState: mptState,
	}, arch, []func(){archiveCleanup}), nil
}

func mptStateCapacity(param int64) int {
	if param <= 0 {
		return mpt.DefaultMptStateCapacity
	}
	capacity := int(param / 512) // TODO use more accurate coefficient
	if capacity < mpt.MinMptStateCapacity {
		capacity = mpt.MinMptStateCapacity
	}
	return capacity
}

func newGoMemoryS5State(params database.Parameters) (database.Database, error) {
	state, err := mpt.OpenGoMemoryState(filepath.Join(params.Directory, "live"), mpt.S5LiveConfig, mptStateCapacity(params.LiveCache))
	if err != nil {
		return nil, err
	}
	return newS5State(params, state)
}

func newGoFileS5State(params database.Parameters) (database.Database, error) {
	state, err := mpt.OpenGoFileState(filepath.Join(params.Directory, "live"), mpt.S5LiveConfig, mptStateCapacity(params.LiveCache))
	if err != nil {
		return nil, err
	}
	return newS5State(params, state)
}

package gostate

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/Fantom-foundation/Carmen/go/database"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
)

// goSchema4 implements a state utilizes an MPT based data structure. However, it is
// not binary compatible with the Ethereum variant of an MPT.
type goSchema4 struct {
	*mpt.MptState
}

func newS4State(params database.Parameters, mptState *mpt.MptState) (database.Database, error) {
	if params.Archive == database.S5Archive {
		return nil, errors.Join(
			fmt.Errorf("%w: cannot use archive %v with schema 4", database.UnsupportedConfiguration, params.Archive),
			mptState.Close(),
		)
	}
	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, errors.Join(err, mptState.Close())
	}
	return newGoState(&goSchema4{
		MptState: mptState,
	}, arch, []func(){archiveCleanup}), nil
}

func newGoMemoryS4State(params database.Parameters) (database.Database, error) {
	state, err := mpt.OpenGoMemoryState(filepath.Join(params.Directory, "live"), mpt.S4LiveConfig, mpt.DefaultMptStateCapacity)
	if err != nil {
		return nil, err
	}
	return newS4State(params, state)
}

func newGoFileS4State(params database.Parameters) (database.Database, error) {
	state, err := mpt.OpenGoFileState(filepath.Join(params.Directory, "live"), mpt.S4LiveConfig, mpt.DefaultMptStateCapacity)
	if err != nil {
		return nil, err
	}
	return newS4State(params, state)
}

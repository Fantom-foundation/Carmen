package gostate

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/Fantom-foundation/Carmen/go/state"
	"github.com/Fantom-foundation/Carmen/go/state/mpt"
)

// goSchema5 implements a state utilizes an MPT based data structure that
// produces the same hashes as Ethereum's MPT implementation.
type goSchema5 struct {
	*mpt.MptState
}

func newS5State(params state.Parameters, mptState *mpt.MptState) (state.State, error) {
	if params.Archive == state.S4Archive {
		return nil, errors.Join(
			fmt.Errorf("%w: cannot use archive %v with schema 5", state.UnsupportedConfiguration, params.Archive),
			mptState.Close(),
		)
	}
	arch, archiveCleanup, err := openArchive(params)
	if err != nil {
		return nil, errors.Join(err, mptState.Close())
	}

	if params.Archive == state.S5Archive {
		// TODO: we can ignore archiveCleanup because it is not used for S5Archive,
		// It is used for leveldb only, but it should be avoided at all: calling Close() on archive should close
		// all resources instead
		archiveBlockHeight, empty, err := arch.GetBlockHeight()
		if err != nil {
			return nil, errors.Join(err, arch.Close(), mptState.Close())
		}
		if !empty {
			archiveHash, err := arch.GetHash(archiveBlockHeight)
			if err != nil {
				return nil, errors.Join(err, arch.Close(), mptState.Close())
			}

			liveHash, err := mptState.GetHash()
			if err != nil {
				return nil, errors.Join(err, arch.Close(), mptState.Close())
			}

			if archiveHash != liveHash {
				return nil, errors.Join(
					fmt.Errorf("archive and live state hashes do not match: archive: 0x%x != live: 0x%x", archiveHash, liveHash),
					arch.Close(),
					mptState.Close())
			}
		}
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

func newGoMemoryS5State(params state.Parameters) (state.State, error) {
	state, err := mpt.OpenGoMemoryState(filepath.Join(params.Directory, "live"), mpt.S5LiveConfig, mptStateCapacity(params.LiveCache))
	if err != nil {
		return nil, err
	}
	return newS5State(params, state)
}

func newGoFileS5State(params state.Parameters) (state.State, error) {
	state, err := mpt.OpenGoFileState(filepath.Join(params.Directory, "live"), mpt.S5LiveConfig, mptStateCapacity(params.LiveCache))
	if err != nil {
		return nil, err
	}
	return newS5State(params, state)
}

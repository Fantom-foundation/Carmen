// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package gostate

import (
	"errors"
	"fmt"
	"time"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"

	"path/filepath"

	"github.com/Fantom-foundation/Carmen/go/state"
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
		// We can ignore archiveCleanup because it is not used for S5Archive,
		// it is used for leveldb only
		archiveBlockHeight, empty, err := arch.GetBlockHeight()
		if err != nil {
			return nil, errors.Join(err, arch.Close(), mptState.Close())
		}

		liveHash, err := mptState.GetHash()
		if err != nil {
			return nil, errors.Join(err, arch.Close(), mptState.Close())
		}

		var archiveHash common.Hash
		if !empty {
			archiveHash, err = arch.GetHash(archiveBlockHeight)
			if err != nil {
				return nil, errors.Join(err, arch.Close(), mptState.Close())
			}
		} else {
			archiveHash = mpt.EmptyNodeEthereumHash
		}

		if archiveHash != liveHash {
			return nil, errors.Join(
				fmt.Errorf("archive and live state hashes do not match: archive: 0x%x != live: 0x%x", archiveHash, liveHash),
				arch.Close(),
				mptState.Close())
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
	capacity := int(param / int64(mpt.EstimatePerNodeMemoryUsage()))
	if capacity < mpt.MinMptStateCapacity {
		capacity = mpt.MinMptStateCapacity
	}
	return capacity
}

func newGoMemoryS5State(params state.Parameters) (state.State, error) {
	state, err := mpt.OpenGoMemoryState(filepath.Join(params.Directory, "live"), mpt.S5LiveConfig, getTrieConfig(params))
	if err != nil {
		return nil, err
	}
	return newS5State(params, state)
}

func newGoFileS5State(params state.Parameters) (state.State, error) {
	state, err := mpt.OpenGoFileState(filepath.Join(params.Directory, "live"), mpt.S5LiveConfig, getTrieConfig(params))
	if err != nil {
		return nil, err
	}
	return newS5State(params, state)
}

func getTrieConfig(params state.Parameters) mpt.TrieConfig {
	return mpt.TrieConfig{
		CacheCapacity:         mptStateCapacity(params.LiveCache),
		BackgroundFlushPeriod: time.Duration(params.BackgroundFlushPeriod) * time.Millisecond,
	}
}

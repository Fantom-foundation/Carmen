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

func getNodeCacheConfig(cacheSize int64) mpt.NodeCacheConfig {
	capacity := 0
	if cacheSize > 0 {
		capacity = int(cacheSize / int64(mpt.EstimatePerNodeMemoryUsage()))

		// If a cache size is given, the resulting capacity should be at least 1.
		// A capacity of 0 would signal the MPT implementation to use the default
		// cache size. However, if a cache size is given that is small enough to
		// not even cover a single node, the minimum MPT cache size should be used.
		if capacity == 0 {
			capacity = 1
		}
	}
	return mpt.NodeCacheConfig{
		Capacity: capacity,
	}
}

func newGoMemoryS5State(params state.Parameters) (state.State, error) {
	state, err := mpt.OpenGoMemoryState(filepath.Join(params.Directory, "live"), mpt.S5LiveConfig, getNodeCacheConfig(params.LiveCache))
	if err != nil {
		return nil, err
	}
	return newS5State(params, state)
}

func newGoFileS5State(params state.Parameters) (state.State, error) {
	state, err := mpt.OpenGoFileState(filepath.Join(params.Directory, "live"), mpt.S5LiveConfig, getNodeCacheConfig(params.LiveCache))
	if err != nil {
		return nil, err
	}
	return newS5State(params, state)
}

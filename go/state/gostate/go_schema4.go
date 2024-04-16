//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package gostate

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"github.com/Fantom-foundation/Carmen/go/state"
)

// goSchema4 implements a state utilizes an MPT based data structure. However, it is
// not binary compatible with the Ethereum variant of an MPT.
type goSchema4 struct {
	*mpt.MptState
}

func newS4State(params state.Parameters, mptState *mpt.MptState) (state.State, error) {
	if params.Archive == state.S5Archive {
		return nil, errors.Join(
			fmt.Errorf("%w: cannot use archive %v with schema 4", state.UnsupportedConfiguration, params.Archive),
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

func newGoMemoryS4State(params state.Parameters) (state.State, error) {
	state, err := mpt.OpenGoMemoryState(filepath.Join(params.Directory, "live"), mpt.S4LiveConfig, mpt.DefaultMptStateCapacity)
	if err != nil {
		return nil, err
	}
	return newS4State(params, state)
}

func newGoFileS4State(params state.Parameters) (state.State, error) {
	state, err := mpt.OpenGoFileState(filepath.Join(params.Directory, "live"), mpt.S4LiveConfig, mpt.DefaultMptStateCapacity)
	if err != nil {
		return nil, err
	}
	return newS4State(params, state)
}

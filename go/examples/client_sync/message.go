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

package demo

import (
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// Message is interface of any message to be exchanged between demo clients.
type Message interface{}

type ErrorMessage struct {
	Issue error
}

// ----------------------------------------------------------------------------
//                          Current State Information
// ----------------------------------------------------------------------------

// - Request block height -

type GetBlockHeightRequest struct {
}

type GetBlockHeightResponse struct {
	BlockHeight uint64
}

// - Request state proof -

type GetStateProofRequest struct {
}

type GetStateProofResponse struct {
	Proof backend.Proof
}

// ----------------------------------------------------------------------------
//                                Block Processing
// ----------------------------------------------------------------------------

// - Broadcast for state updates -

type BlockUpdateBroadcast struct {
	block  uint64
	update common.Update
}

// ----------------------------------------------------------------------------
//                                   Syncing
// ----------------------------------------------------------------------------

// - Broadcast for end of epoch -

// This should trigger the creation of a new snapshot for synching.
type EndOfEpochBroadcast struct {
}

// - Get Snapshot Information -

type GetSnapshotRootProofRequest struct{}

type GetSnapshotRootProofResponse struct {
	Data []byte
}

type GetSnapshotMetaDataRequest struct{}

type GetSnapshotMetaDataResponse struct {
	Data []byte
}

type GetSnapshotProofRequest struct {
	PartNumber int
}

type GetSnapshotProofResponse struct {
	Data []byte
}

type GetSnapshotPartRequest struct {
	PartNumber int
}

type GetSnapshotPartResponse struct {
	Data []byte
}

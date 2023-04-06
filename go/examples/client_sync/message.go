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

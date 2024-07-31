// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package archive

//go:generate mockgen -source archive.go -destination archive_mock.go -package archive

import (
	"io"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/common/witness"
)

// ErrWitnessProofNotSupported is returned when the archive does not support witness proofs.
const (
	ErrWitnessProofNotSupported  = common.ConstError("witness proof not supported")
	ErrGenesisExportNotSupported = common.ConstError("genesis export not supported")
)

// An Archive retains a history of state mutations in a blockchain on a
// block-level granularity. The history is recorded by adding per-block updates.
// All updates are append-only. History written once can no longer be altered.
//
// Archive Add(..) and GetXXX(..) operations are thread safe and may thus be run
// in parallel.
type Archive interface {

	// Add adds the changes of the given block to this archive.
	Add(block uint64, update common.Update, hints any) error

	// GetBlockHeight gets the maximum block height inserted so far. If there
	// is no block in the archive, the empty flag is set instead.
	GetBlockHeight() (block uint64, empty bool, err error)

	// Exists allows to fetch a historic existence status of a given account.
	Exists(block uint64, account common.Address) (exists bool, err error)

	// GetBalance allows to fetch a historic balance values for a given account.
	GetBalance(block uint64, account common.Address) (balance amount.Amount, err error)

	// GetCode allows to fetch a historic code values for a given account.
	GetCode(block uint64, account common.Address) (code []byte, err error)

	// GetNonce allows to fetch a historic nonce values for a given account.
	GetNonce(block uint64, account common.Address) (nonce common.Nonce, err error)

	// GetStorage allows to fetch a historic value for a given slot.
	GetStorage(block uint64, account common.Address, slot common.Key) (value common.Value, err error)

	// GetAccountHash provides a hash of the account state at the given block height.
	GetAccountHash(block uint64, account common.Address) (common.Hash, error)

	// GetHash provides a hash of the state at the given block height.
	GetHash(block uint64) (hash common.Hash, err error)

	// CreateWitnessProof creates a witness proof for the given account and keys.
	CreateWitnessProof(block uint64, address common.Address, keys ...common.Key) (witness.Proof, error)

	// CreateLiveDBGenesis writes a LiveDB genesis for given block into out.
	CreateLiveDBGenesis(block uint64, out io.Writer) (common.Hash, error)

	// MemoryFootprintProvider provides the size of the store in memory in bytes.
	common.MemoryFootprintProvider

	common.FlushAndCloser
}

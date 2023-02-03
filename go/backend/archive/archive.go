package archive

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
)

// An Archive retains a history of state mutations in a blockchain on a
// block-level granularity. The history is recorded by adding per-block updates.
// All updates are append-only. History written once can no longer be altered.
//
// Archive Add(..) and GetXXX(..) operations are thread safe and may thus be run
// in parallel.
type Archive interface {

	// Add adds the changes of the given block to this archive.
	Add(block uint64, update common.Update) error

	// GetLastBlockHeight gets the maximum block height inserted so far, returns 0 if there is none.
	GetLastBlockHeight() (block uint64, err error)

	// Exists allows to fetch a historic existence status of a given account.
	Exists(block uint64, account common.Address) (exists bool, err error)

	// GetBalance allows to fetch a historic balance values for a given account.
	GetBalance(block uint64, account common.Address) (balance common.Balance, err error)

	// GetCode allows to fetch a historic code values for a given account.
	GetCode(block uint64, account common.Address) (code []byte, err error)

	// GetNonce allows to fetch a historic nonce values for a given account.
	GetNonce(block uint64, account common.Address) (nonce common.Nonce, err error)

	// GetStorage allows to fetch a historic value for a given slot.
	GetStorage(block uint64, account common.Address, slot common.Key) (value common.Value, err error)

	io.Closer
}

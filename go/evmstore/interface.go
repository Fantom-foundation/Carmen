package evmstore

import "github.com/Fantom-foundation/Carmen/go/common"

type EvmStore interface {
	// SetTxPosition stores transaction block and position.
	SetTxPosition(txHash common.Hash, position TxPosition) error

	// GetTxPosition returns stored transaction block and position.
	// Returns empty position with no err if the tx is not present.
	GetTxPosition(txHash common.Hash) (TxPosition, error)

	// SetTx stores non-event transaction.
	SetTx(txHash common.Hash, tx []byte) error

	// GetTx returns stored non-event transaction.
	// Returns nil,nil if the tx is not present.
	GetTx(txHash common.Hash) ([]byte, error)

	// SetRawReceipts stores raw transaction receipts for one block.
	SetRawReceipts(block uint64, receipts []byte) error

	// GetRawReceipts loads raw transaction receipts.
	// Returns nil,nil if receipts for given block aren't stored.
	GetRawReceipts(block uint64) ([]byte, error)

	// Flush writes all committed content to disk.
	Flush() error

	// Close flushes the store and closes it.
	Close() error
}

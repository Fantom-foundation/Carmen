//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

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

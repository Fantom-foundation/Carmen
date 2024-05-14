// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package common

// Log summarizes a log message recorded during the execution of a contract.
// This should be approximating ethereum's definition: t.ly/dVL7
type Log struct {
	// -- payload --
	// Address of the contract that generated the event.
	Address Address
	// List of topics the log message should be tagged by.
	Topics []Hash
	// The actual log message.
	Data []byte

	// -- metadata --
	// Index of the log in the block.
	Index uint
}

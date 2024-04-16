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

package evmstore

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/common"
)

type TxPosition struct {
	Block       uint64      // zero for skipped event txs
	Event       common.Hash // zeros indicates no-event tx (genesis/epoch sealing)
	EventOffset uint32
	BlockOffset uint32
}

const txPositionSize = 8 + 32 + 4 + 4

// TxPositionSerializer is a Serializer of the TxPosition type
type TxPositionSerializer struct{}

func (a TxPositionSerializer) ToBytes(value TxPosition) []byte {
	out := make([]byte, txPositionSize)
	a.CopyBytes(value, out)
	return out
}
func (a TxPositionSerializer) CopyBytes(value TxPosition, out []byte) {
	binary.BigEndian.PutUint64(out[:], value.Block)
	copy(out[8:], value.Event[:])
	binary.BigEndian.PutUint32(out[8+32:], value.EventOffset)
	binary.BigEndian.PutUint32(out[8+32+4:], value.BlockOffset)
}
func (a TxPositionSerializer) FromBytes(bytes []byte) (out TxPosition) {
	out.Block = binary.BigEndian.Uint64(bytes[:])
	copy(out.Event[:], bytes[8:])
	out.EventOffset = binary.BigEndian.Uint32(bytes[8+32:])
	out.BlockOffset = binary.BigEndian.Uint32(bytes[8+32+4:])
	return out
}
func (a TxPositionSerializer) Size() int {
	return txPositionSize
}

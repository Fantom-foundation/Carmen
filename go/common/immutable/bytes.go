// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package immutable

import "fmt"

// Bytes is an immutable slice of bytes that can be trivially cloned.
type Bytes struct {
	data string
}

// NewBytes creates a new Bytes from a slice of bytes.
func NewBytes(data []byte) Bytes {
	return Bytes{data: string(data)}
}

func (b Bytes) ToBytes() []byte {
	return []byte(b.data)
}

func (b Bytes) String() string {
	return fmt.Sprintf("0x%x", b.data)
}

// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package ldb_test

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/multimap/ldb"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestDbKey(t *testing.T) {
	var dbKey ldb.DbKey[uint32, uint64]
	dbKey.SetTableKey(backend.AddressSlotMultiMapKey, 0x12345678, common.Identifier32Serializer{})
	dbKey.SetValue(0x987654321, common.Identifier64Serializer{})

	if !bytes.Equal(dbKey[:], []byte{0x4d, 0x12, 0x34, 0x56, 0x78, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x87, 0x65, 0x43, 0x21}) {
		t.Errorf("unexpected dbKey: %x", dbKey)
	}

	dbKey.SetMaxValue()

	if !bytes.Equal(dbKey[:], []byte{0x4d, 0x12, 0x34, 0x56, 0x78, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}) {
		t.Errorf("unexpected dbKey: %x", dbKey)
	}
}

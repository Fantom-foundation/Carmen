//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

package ldb

import (
	"bytes"
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestBlockKey(t *testing.T) {
	var block0, block1, block2 blockKey
	block0.set(0)
	block1.set(1)
	block2.set(2)
	blockRange := getBlockKeyRangeFrom(2)

	if !bytes.Equal(blockRange.Start, block2[:]) {
		t.Errorf("the range does not start at block 2; %x == %x", blockRange.Start, block2[:])
	}
	if !(bytes.Compare(blockRange.Limit, block1[:]) > 0) {
		t.Errorf("the range does not include block 1; %x > %x", blockRange.Limit, block1[:])
	}
	if !(bytes.Compare(blockRange.Limit, block0[:]) > 0) {
		t.Errorf("the range does not include block 0; %x > %x", blockRange.Limit, block0[:])
	}
}

func TestAccountBlockKey(t *testing.T) {
	var addr = common.Address{0x01}
	var block0, block1, block2 accountBlockKey
	block0.set(backend.NonceArchiveKey, addr, 0)
	block1.set(backend.NonceArchiveKey, addr, 1)
	block2.set(backend.NonceArchiveKey, addr, 2)
	blockRange := block2.getRange()

	if !bytes.Equal(blockRange.Start, block2[:]) {
		t.Errorf("the range does not start at block 2; %x == %x", blockRange.Start, block2[:])
	}
	if !(bytes.Compare(blockRange.Limit, block1[:]) > 0) {
		t.Errorf("the range does not include block 1; %x > %x", blockRange.Limit, block1[:])
	}
	if !(bytes.Compare(blockRange.Limit, block0[:]) > 0) {
		t.Errorf("the range does not include block 0; %x > %x", blockRange.Limit, block0[:])
	}
}

func TestAccountKeyBlockKey(t *testing.T) {
	var addr = common.Address{0x01}
	var block0, block1, block2 accountKeyBlockKey
	block0.set(backend.StorageArchiveKey, addr, 0, common.Key{0x01}, 0)
	block1.set(backend.StorageArchiveKey, addr, 0, common.Key{0x01}, 1)
	block2.set(backend.StorageArchiveKey, addr, 0, common.Key{0x01}, 2)
	blockRange := block2.getRange()

	if !bytes.Equal(blockRange.Start, block2[:]) {
		t.Errorf("the range does not start at block 2; %x == %x", blockRange.Start, block2[:])
	}
	if !(bytes.Compare(blockRange.Limit, block1[:]) > 0) {
		t.Errorf("the range does not include block 1; %x > %x", blockRange.Limit, block1[:])
	}
	if !(bytes.Compare(blockRange.Limit, block0[:]) > 0) {
		t.Errorf("the range does not include block 0; %x > %x", blockRange.Limit, block0[:])
	}
}

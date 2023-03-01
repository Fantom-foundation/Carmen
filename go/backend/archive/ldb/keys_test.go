package ldb

import (
	"bytes"
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
	block0.set(common.NonceArchiveKey, addr, 0)
	block1.set(common.NonceArchiveKey, addr, 1)
	block2.set(common.NonceArchiveKey, addr, 2)
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
	block0.set(common.StorageArchiveKey, addr, 0, common.Key{0x01}, 0)
	block1.set(common.StorageArchiveKey, addr, 0, common.Key{0x01}, 1)
	block2.set(common.StorageArchiveKey, addr, 0, common.Key{0x01}, 2)
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

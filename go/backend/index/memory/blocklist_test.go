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

package memory

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestBlockListIsMap(t *testing.T) {
	var instance BlockList[common.Address, uint32]
	var _ common.Map[common.Address, uint32] = &instance
}

func TestBlockList_NewListIsEmpty(t *testing.T) {

	b := NewBlockList[common.Address, uint32](10, common.AddressComparator{})

	if _, exists := b.Get(B); exists {
		t.Errorf("Value should not exist")
	}

	if b.Size() != 0 {
		t.Errorf("The size is incorrect")
	}
}

func TestBlockListGetSet(t *testing.T) {
	b := NewBlockList[common.Address, uint32](10, common.AddressComparator{})

	b.Put(B, 10)
	if val, exists := b.Get(B); !exists || val != 10 {
		t.Errorf("Value should exist, %d", val)
	}

	// override
	b.Put(B, 20)
	if val, exists := b.Get(B); !exists || val != 20 {
		t.Errorf("Value should exist, %d", val)
	}

	if b.Size() != 1 {
		t.Errorf("The size is incorrect")
	}
}

func TestBlockListBulkInsert(t *testing.T) {
	max := uint32(2*10 + 3) // three pages
	data := make([]common.MapEntry[common.Address, uint32], max)
	for i := uint32(0); i < max; i++ {
		address := common.Address{byte(i + 1)}
		data[i] = common.MapEntry[common.Address, uint32]{Key: address, Val: i + 1}
	}

	b := InitBlockList[common.Address, uint32](10, data, common.AddressComparator{})

	if size := b.Size(); size != int(max) {
		t.Errorf("Size does not match: %d != %d", size, max)
	}

	if size := len(b.list); size != 3 {
		t.Errorf("Number of pages does not match: %d != %d", size, 3)
	}

	// inserted data must much returned data
	entries := b.GetEntries()
	for i, entry := range entries {
		if entry.Key != data[i].Key || entry.Val != data[i].Val {
			t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, data[i].Key, data[i].Val)
		}
	}

	if size := len(entries); size != int(max) {
		t.Errorf("Size does not match: %d != %d", size, max)
	}

}

func TestBlockListOverflows(t *testing.T) {
	b := NewBlockList[common.Address, uint32](10, common.AddressComparator{})

	for i := 0; i < 10; i++ {
		b.Put(common.Address{byte(i)}, uint32(i))
	}

	// tail is the head
	if len(b.list) != 1 {
		t.Errorf("There should be one block")
	}

	b.Put(common.Address{120}, 120)
	if len(b.list) != 2 {
		t.Errorf("There must be one more block ")
	}

	if b.Size() != 11 {
		t.Errorf("The size is incorrect")
	}

	// tree blocks
	for i := 0; i < 10; i++ {
		b.Put(common.Address{byte(i + 10)}, uint32(i))
	}

	if len(b.list) != 3 {
		t.Errorf("There must be one more block ")
	}

	if val, exists := b.Get(common.Address{120}); !exists || val != 120 {
		t.Errorf("Value dos not match for key: %v, %d != %d", common.Address{120}, val, 120)
	}
	// replace a key in an overflow (tail) block
	b.Put(common.Address{120}, 125)
	if val, exists := b.Get(common.Address{120}); !exists || val != 125 {
		t.Errorf("Value dos not match for key: %v, %d != %d", common.Address{120}, val, 125)
	}
}

func TestBlockListBlockSizes(t *testing.T) {
	b := NewBlockList[common.Address, uint32](9, common.AddressComparator{})

	n := 10000
	for i := 0; i < n; i++ {
		b.Put(common.AddressFromNumber(i), uint32(i))
	}

	for i, item := range b.list {
		if size := item.Size(); i < len(b.list)-1 && size != 9 {
			t.Errorf("Wrong block size!")
		}
	}

	// check tail
	tailSize := n % 9
	tail := b.list[len(b.list)-1]
	if size := tail.Size(); size != tailSize {
		t.Errorf("Wrong block size!")
	}

}

func TestBlockListIterate(t *testing.T) {
	b := NewBlockList[common.Address, uint32](10, common.AddressComparator{})

	var data = make(map[common.Address]uint32, 123)
	for i := 0; i < 123; i++ {
		b.Put(common.Address{byte(i)}, uint32(i))
		data[common.Address{byte(i)}] = uint32(i)
	}

	if b.Size() != 123 {
		t.Errorf("The size is incorrect")
	}

	actualData := make(map[common.Address]uint32, 123)
	b.ForEach(func(k common.Address, v uint32) {
		actualData[k] = v
		if expected, exists := data[k]; !exists || v != expected {
			t.Errorf("Values differ for key: %v, %v != %v", k, expected, v)
		}
	})
	if len(actualData) != len(data) {
		t.Errorf("Wrong number of items received from for-each")
	}
}

func TestBlockListRemove(t *testing.T) {
	b := NewBlockList[common.Address, uint32](10, common.AddressComparator{})

	// remove non-existing should not fail
	if exists := b.Remove(C); exists {
		t.Errorf("remove failed")
	}

	// remove one item
	b.Put(A, 99)
	if exists := b.Remove(A); !exists {
		t.Errorf("remove failed")
	}

	if _, exists := b.Get(A); exists {
		t.Errorf("Not removed")
	}

	for i := 0; i < 10; i++ {
		b.Put(common.Address{byte(i)}, uint32(i))
	}
	b.Put(A, 190)

	if size := len(b.list); size != 2 {
		t.Errorf("Wrong number of inner blocks")
	}

	// remove last block
	if exists := b.Remove(A); !exists {
		t.Errorf("remove failed")
	}

	if _, exists := b.Get(A); exists {
		t.Errorf("Not removed")
	}

	// tail must be removed
	if size := len(b.list); size != 1 {
		t.Errorf("Wrong number of inner blocks")
	}

	// one item ouf of 11 removed
	if size := b.Size(); size != 10 {
		t.Errorf("Wrong size: %d != %d", size, 10)
	}
}

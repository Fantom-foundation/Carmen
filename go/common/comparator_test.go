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

import "testing"

var (
	addressA = Address{0xA}
	addressB = Address{0xB}
	addressC = Address{0xC}

	keyA = &Key{0xA}
	keyB = &Key{0xB}

	slotA = &SlotIdx[uint32]{uint32(10), uint32(20)}
	slotB = &SlotIdx[uint32]{uint32(30), uint32(40)}
	slotC = &SlotIdx[uint32]{uint32(10), uint32(40)}
)

func TestAddressComparator(t *testing.T) {

	if addressA.Compare(&addressA) != 0 {
		t.Errorf("Wrong comparator error")
	}
	if addressA.Compare(&addressB) > 0 {
		t.Errorf("Wrong comparator error")
	}
	if addressB.Compare(&addressA) < 0 {
		t.Errorf("Wrong comparator error")
	}
}
func TestKeyComparator(t *testing.T) {
	if keyA.Compare(keyA) != 0 {
		t.Errorf("Wrong comparator error")
	}
	if keyA.Compare(keyB) > 0 {
		t.Errorf("Wrong comparator error")
	}
	if keyB.Compare(keyA) < 0 {
		t.Errorf("Wrong comparator error")
	}
}

func TestSlotAddressDifferComparator(t *testing.T) {
	if slotA.Compare(slotA) != 0 {
		t.Errorf("Wrong comparator error")
	}
	if slotA.Compare(slotB) >= 0 {
		t.Errorf("Wrong comparator error")
	}
	if slotB.Compare(slotA) <= 0 {
		t.Errorf("Wrong comparator error")
	}
}

func TestSlotAddressSameComparator(t *testing.T) {
	if slotC.Compare(slotC) != 0 {
		t.Errorf("Wrong comparator error")
	}
	if slotA.Compare(slotC) >= 0 {
		t.Errorf("Wrong comparator error")
	}
	if slotC.Compare(slotA) <= 0 {
		t.Errorf("Wrong comparator error")
	}
}

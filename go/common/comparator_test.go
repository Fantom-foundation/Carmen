package common

import "testing"

var (
	addressA = &Address{byte(0xA)}
	addressB = &Address{byte(0xB)}

	keyA = &Key{byte(0xA)}
	keyB = &Key{byte(0xB)}

	slotA = &SlotIdx1[uint32]{uint32(10), uint32(20)}
	slotB = &SlotIdx1[uint32]{uint32(30), uint32(40)}
	slotC = &SlotIdx1[uint32]{uint32(10), uint32(40)}
)

func TestAddressComparator(t *testing.T) {

	if addressA.Compare(addressA) != 0 {
		t.Errorf("Wrong comparator error")
	}
	if addressA.Compare(addressB) > 0 {
		t.Errorf("Wrong comparator error")
	}
	if addressB.Compare(addressA) < 0 {
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

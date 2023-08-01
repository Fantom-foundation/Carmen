package common

import "testing"

var (
	addressA = &Address{byte(0xA)}
	addressB = &Address{byte(0xB)}

	keyA = &Key{byte(0xA)}
	keyB = &Key{byte(0xB)}
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

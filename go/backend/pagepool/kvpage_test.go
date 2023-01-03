package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

var (
	A = common.Address{0xAA}
	B = common.Address{0xBB}
	C = common.Address{0xCC}
	D = common.Address{0xDD}
)

func TestKVPageIsPage(t *testing.T) {
	var instance KVPage[common.Address, uint32]
	var _ Page = &instance
}

func TestPageInsertFindValues(t *testing.T) {
	p := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})

	if _, exists := p.get(A); exists {
		t.Errorf("Value should not exist")
	}
	if index, exists := p.findItem(A); exists || index != 0 {
		t.Errorf("Wrong index %d", index)
	}

	p.put(A, 10)
	if _, exists := p.get(A); !exists {
		t.Errorf("Value should exist")
	}
	if index, exists := p.findItem(A); !exists || index != 0 {
		t.Errorf("Wrong index %d", index)
	}
	if index, exists := p.findValue(A, 10); !exists || index != 0 {
		t.Errorf("Wrong index %d", index)
	}
	// should not exist and point after A
	if index, exists := p.findItem(B); exists || index != 1 {
		t.Errorf("Wrong index %d", index)
	}

	p.put(B, 20)
	if _, exists := p.get(B); !exists {
		t.Errorf("Value should exist")
	}
	if index, exists := p.findItem(B); !exists || index != 1 {
		t.Errorf("Wrong index %d", index)
	}
	if index, exists := p.findValue(B, 20); !exists || index != 1 {
		t.Errorf("Wrong index %d", index)
	}
	// should not exist and point after A
	if index, exists := p.findItem(C); exists || index != 2 {
		t.Errorf("Wrong index %d", index)
	}

	p.put(D, 40)
	if _, exists := p.get(D); !exists {
		t.Errorf("Value should exist")
	}
	if index, exists := p.findItem(D); !exists || index != 2 {
		t.Errorf("Wrong index %d", index)
	}
	if index, exists := p.findValue(D, 40); !exists || index != 2 {
		t.Errorf("Wrong index %d", index)
	}
	// should not exist and point after B, before D
	if index, exists := p.findItem(C); exists || index != 2 {
		t.Errorf("Wrong index %d", index)
	}

	p.put(C, 30)
	if _, exists := p.get(C); !exists {
		t.Errorf("Value should exist")
	}
	if index, exists := p.findItem(C); !exists || index != 2 {
		t.Errorf("Wrong index %d", index)
	}
	if index, exists := p.findValue(C, 40); exists || index != 2 {
		t.Errorf("Wrong index %d", index)
	}

	if size := p.size(); size != 4 {
		t.Errorf("Wrong size: %d", size)
	}

	testPageSorted[common.Address, uint32](t, p, common.AddressComparator{})
}

func TestPageSorted(t *testing.T) {
	p := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})

	p.put(A, 10)
	p.put(B, 20)
	p.put(C, 30)

	testPageSorted[common.Address, uint32](t, p, common.AddressComparator{})
}

func TestPageSortedReverseInsert(t *testing.T) {
	p := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})

	p.put(C, 30)
	p.put(B, 20)
	p.put(A, 10)

	testPageSorted[common.Address, uint32](t, p, common.AddressComparator{})
}

func TestPageSortedMultiValue(t *testing.T) {
	p := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})

	p.add(C, 30)

	p.add(B, 20)
	p.add(B, 30)
	p.add(B, 40)

	p.add(A, 10)
	p.add(A, 20)

	p.add(B, 50)
	p.add(B, 60)
	p.add(B, 70)

	testPageSorted[common.Address, uint32](t, p, common.AddressComparator{})
}

func TestPageInsertMultiValues(t *testing.T) {
	p := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})

	p.add(A, 10)
	p.add(A, 20)

	p.add(B, 10)
	p.add(B, 20)
	p.add(B, 30)

	p.add(C, 40)

	if start, end, exists := p.findRange(A); !exists || start != 0 || end != 2 {
		t.Errorf("Wrong indexes: %d, %d ", start, end)
	}

	if start, end, exists := p.findRange(B); !exists || start != 2 || end != 5 {
		t.Errorf("Wrong indexes: %d, %d ", start, end)
	}

	if start, end, exists := p.findRange(C); !exists || start != 5 || end != 6 {
		t.Errorf("Wrong indexes: %d, %d ", start, end)
	}

	// non-existing values
	if index, exists := p.findValue(C, 30); exists {
		t.Errorf("Value not found at index %d", index)
	}
}

func TestPageRemoveValues(t *testing.T) {
	p := NewKVPage[common.Address, uint32](pageSize, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})

	p.add(A, 10)
	p.add(A, 20)

	p.add(B, 10)
	p.add(B, 20)
	p.add(B, 30)

	p.add(C, 40)

	if index, exists := p.findValue(A, 10); !exists {
		t.Errorf("Value does not exist at index %d", index)
	}

	// remove single value
	p.removeVal(A, 10)
	if _, exists := p.findValue(A, 10); exists {
		t.Errorf("Value should not exist")
	}

	// remove the whole key
	p.removeAll(B)
	if _, _, exists := p.findRange(B); exists {
		t.Errorf("Value should not exist")
	}

	p.Clear()
	if size := p.size(); size != 0 {
		t.Errorf("Wrong size: %d", size)
	}
}

func testPageSorted[K comparable, V comparable](t *testing.T, page *KVPage[K, V], comparator common.Comparator[K]) {
	keys := make([]K, 0, page.size())
	page.forEach(func(k K, v V) {
		keys = append(keys, k)
	})

	common.AssertArraySorted(t, keys, comparator)
}

func pageFactory(pageItems int) func() *KVPage[common.Address, uint32] {
	return KVPageFactoryNumItems[common.Address, uint32](pageItems, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
}

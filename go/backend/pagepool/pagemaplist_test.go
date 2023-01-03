package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"math/rand"
	"testing"
)

func TestPageMapListIsMap(t *testing.T) {
	var instance PageMapList[common.Address, uint32]
	var _ common.ErrMap[common.Address, uint32] = &instance
}

func TestMapGetPut(t *testing.T) {
	h := newPageMapList(5)

	if _, exists := h.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	h.Put(A, 10)
	h.Put(B, 20)
	h.Put(C, 30)

	if val, exists := h.Get(A); !exists || val != 10 {
		t.Errorf("Value is not correct")
	}
	if val, exists := h.Get(B); !exists || val != 20 {
		t.Errorf("Value is not correct")
	}
	if val, exists := h.Get(C); !exists || val != 30 {
		t.Errorf("Value is not correct")
	}

	// replace
	h.Put(A, 33)
	if val, exists := h.Get(A); !exists || val != 33 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.Put(B, 44)
	if val, exists := h.Get(B); !exists || val != 44 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.Put(C, 55)
	if val, exists := h.Get(C); !exists || val != 55 {
		t.Errorf("Value is not correct")
	}

	if size := h.Size(); size != 3 {
		t.Errorf("Size does not fit: %d", size)
	}

	actualData := make(map[common.Address]uint32, 123)
	h.ForEach(func(k common.Address, v uint32) {
		actualData[k] = v

		if k != A && k != B && k != C {
			t.Errorf("Unexpected key: %v", k)
		}
		if k == A && v != 33 {
			t.Errorf("Unexpected value: %v -> %d", k, v)
		}
		if k == B && v != 44 {
			t.Errorf("Unexpected value: %v -> %d", k, v)
		}
		if k == C && v != 55 {
			t.Errorf("Unexpected value: %v -> %d", k, v)
		}
	})

	if len(actualData) != h.Size() {
		t.Errorf("Wrong number of items received from for-each")
	}
}

func TestMapInverseGetPut(t *testing.T) {
	h := newPageMapList(5)
	if _, exists := h.Get(A); exists {
		t.Errorf("Value is not correct")
	}

	h.Put(C, 30)
	h.Put(B, 20)
	h.Put(A, 10)

	if val, _ := h.Get(A); val != 10 {
		t.Errorf("Value is not correct")
	}
	if val, _ := h.Get(B); val != 20 {
		t.Errorf("Value is not correct")
	}
	if val, _ := h.Get(C); val != 30 {
		t.Errorf("Value is not correct")
	}

	// replace
	h.Put(A, 33)
	if val, _ := h.Get(A); val != 33 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.Put(B, 44)
	if val, _ := h.Get(B); val != 44 {
		t.Errorf("Value is not correct")
	}
	// replace
	h.Put(C, 55)
	if val, _ := h.Get(C); val != 55 {
		t.Errorf("Value is not correct")
	}

	if size := h.Size(); size != 3 {
		t.Errorf("Size does not fit: %d", size)
	}
}

func TestMapSize(t *testing.T) {
	h := newPageMapList(10000)

	n := rand.Intn(9999)
	for i := 0; i < n; i++ {
		h.Put(common.AddressFromNumber(i), uint32(i))
	}

	if size := h.Size(); size != n {
		t.Errorf("Size is not correct: %d != %d", size, n)
	}
}

func TestMapRemove(t *testing.T) {
	h := newPageMapList(5)

	if exists := h.Remove(C); exists {
		t.Errorf("remove from empty map failed")
	}

	h.Put(C, 99)
	if exists := h.Remove(C); !exists {
		t.Errorf("remove failed:  %v", C)
	}
	if actual, exists := h.Get(C); exists || actual == 99 {
		t.Errorf("remove failed:  %v -> %v", C, actual)
	}

	h.Put(A, 1)
	h.Put(B, 2)
	h.Put(C, 3)

	if size := h.Size(); size != 3 {
		t.Errorf("Size is not correct: %d ", size)
	}

	// remove from middle
	if exists := h.Remove(B); !exists {
		t.Errorf("remove failed:  %v", B)
	}

	if size := h.Size(); size != 2 {
		t.Errorf("Size is not correct: %d ", size)
	}

	// remove from last
	if exists := h.Remove(C); !exists {
		t.Errorf("remove failed:  %v", B)
	}

	if size := h.Size(); size != 1 {
		t.Errorf("Size is not correct: %d ", size)
	}

	if exists := h.Remove(A); !exists {
		t.Errorf("remove failed:  %v", B)
	}

	h.ForEach(func(k common.Address, v uint32) {
		t.Errorf("There should be no item to iterata: %v -> %d", k, v)
	})
}

func TestMapClear(t *testing.T) {
	h := newPageMapList(5)

	h.Put(A, 10)
	h.Put(B, 20)
	h.Put(C, 30)

	if size := h.Size(); size != 3 {
		t.Errorf("Size is not correct: %d", size)
	}

	h.Clear()

	if size := h.Size(); size != 0 {
		t.Errorf("Size is not correct: %d", size)
	}

	h.ForEach(func(k common.Address, v uint32) {
		t.Errorf("There should be no item to iterata: %v -> %d", k, v)
	})
}

func newPageMapList(capacity int) common.Map[common.Address, uint32] {
	// two pages in the pool, two items each
	pageCapacity := capacity / 2
	pagePool := NewPagePool[common.Address, uint32](pagePoolSize, pageCapacity, nil, NewMemoryPageStore[common.Address, uint32](), common.AddressComparator{})
	return &noErrMapWrapper[common.Address, uint32]{NewPageMapList[common.Address, uint32](33, pageCapacity, pagePool)}
}

// noErrMapWrapper converts the input map to non-err map
type noErrMapWrapper[K comparable, V any] struct {
	m common.ErrMap[K, V]
}

func (c *noErrMapWrapper[K, V]) ForEach(callback func(K, V)) {
	_ = c.m.ForEach(callback)
}

func (c *noErrMapWrapper[K, V]) Get(key K) (val V, exists bool) {
	val, exists, _ = c.m.Get(key)
	return
}

func (c *noErrMapWrapper[K, V]) Put(key K, val V) {
	_ = c.m.Put(key, val)
}

func (c *noErrMapWrapper[K, V]) Remove(key K) (exists bool) {
	exists, _ = c.m.Remove(key)
	return
}

func (c *noErrMapWrapper[K, V]) Clear() {
	_ = c.m.Clear()
}

func (c *noErrMapWrapper[K, V]) Size() int {
	return c.m.Size()
}

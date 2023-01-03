package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"math/rand"
	"testing"
)

func TestPageMultiMapListIsMap(t *testing.T) {
	var instance PageMultiMapList[common.Address, uint32]
	var _ common.ErrMultiMap[common.Address, uint32] = &instance
}

func TestPageMultiMapListAddGetRemove(t *testing.T) {
	h := newPageMultiMapList(6)
	// store a few values under the same key
	h.Add(A, 10)
	h.Add(A, 10) // the same value does not change the map
	h.Add(A, 20)
	h.Add(A, 30)
	h.Add(A, 30) // the same value does not change the map

	h.Add(B, 35)
	h.Add(B, 35) // the same value does not change the map
	h.Add(B, 25)
	h.Add(B, 15)
	h.Add(B, 15) // the same value does not change the map

	if size := h.Size(); size != 6 {
		t.Errorf("SizeBytes does not match: %d", size)
	}

	common.AssertEqualArrays(t, h.GetAll(A), []uint32{10, 20, 30})
	common.AssertEqualArrays(t, h.GetAll(B), []uint32{15, 25, 35})

	// pickup values in order
	keys := make([]common.Address, 0, h.Size())
	h.ForEach(func(k common.Address, v uint32) {
		keys = append(keys, k)
	})
	if len(keys) != 6 {
		t.Errorf("Not all items have been iterated")
	}

	// remove
	h.RemoveAll(A)
	if values := h.GetAll(A); len(values) != 0 {
		t.Errorf("unexpected values returned: %v", values)
	}

	if size := h.Size(); size != 3 {
		t.Errorf("SizeBytes does not match: %d", size)
	}

	h.RemoveAll(B)
	if values := h.GetAll(B); len(values) != 0 {
		t.Errorf("unexpected values returned: %v", values)
	}

	if size := h.Size(); size != 0 {
		t.Errorf("SizeBytes does not match: %d", size)
	}
}

func TestPageMultiMapListRemoveSingleValues(t *testing.T) {
	h := newPageMultiMapList(6)
	// store a few values under the same key
	h.Add(A, 10)
	h.Add(A, 10) // the same value does not change the map
	h.Add(A, 20)
	h.Add(A, 30)
	h.Add(A, 30) // the same value does not change the map

	h.Add(B, 35)
	h.Add(B, 35) // the same value does not change the map
	h.Add(B, 25)
	h.Add(B, 15)
	h.Add(B, 15) // the same value does not change the map

	if size := h.Size(); size != 6 {
		t.Errorf("SizeBytes does not match: %d", size)
	}

	// remove
	h.Remove(A, 10)
	h.Remove(B, 35)
	h.Remove(B, 25)

	common.AssertEqualArrays(t, h.GetAll(A), []uint32{20, 30})
	common.AssertEqualArrays(t, h.GetAll(B), []uint32{15})

	if size := h.Size(); size != 3 {
		t.Errorf("SizeBytes does not match: %d", size)
	}

	// remove non existing values
	h.Remove(A, 100)
	h.Remove(B, 350)

	// the properties of the array should not change
	common.AssertEqualArrays(t, h.GetAll(A), []uint32{20, 30})
	common.AssertEqualArrays(t, h.GetAll(B), []uint32{15})

	if size := h.Size(); size != 3 {
		t.Errorf("SizeBytes does not match: %d", size)
	}
}

func TestPageMultiMapListSize(t *testing.T) {
	h := newPageMultiMapList(10000)

	n := rand.Intn(9999)
	for i := 0; i < n; i++ {
		h.Add(common.AddressFromNumber(i), uint32(i))
	}

	if size := h.Size(); size != n {
		t.Errorf("SizeBytes is not correct: %d != %d", size, n)
	}
}

func TestPageMultiMapListClear(t *testing.T) {
	h := newPageMultiMapList(5)

	h.Add(A, 10)
	h.Add(B, 20)
	h.Add(C, 30)

	if size := h.Size(); size != 3 {
		t.Errorf("SizeBytes is not correct: %d", size)
	}

	h.Clear()

	if size := h.Size(); size != 0 {
		t.Errorf("SizeBytes is not correct: %d", size)
	}

	h.ForEach(func(k common.Address, v uint32) {
		t.Errorf("There should be no item to iterata: %v -> %d", k, v)
	})
}

func newPageMultiMapList(capacity int) common.MultiMap[common.Address, uint32] {
	pageCapacity := capacity / 2
	pageFactory := pageFactory(pageCapacity)
	pagePool := NewPagePool[*KVPage[common.Address, uint32]](pagePoolSize, nil, NewMemoryPageStore(), pageFactory)
	return &noErrMultiMapMapWrapper[common.Address, uint32]{NewPageMultiMapList[common.Address, uint32](33, pageCapacity, pagePool)}
}

// noErrMapWrapper converts the input map to non-err map
type noErrMultiMapMapWrapper[K comparable, V any] struct {
	m common.ErrMultiMap[K, V]
}

func (c *noErrMultiMapMapWrapper[K, V]) ForEach(callback func(K, V)) {
	_ = c.m.ForEach(callback)
}

func (c *noErrMultiMapMapWrapper[K, V]) Add(key K, val V) {
	_ = c.m.Add(key, val)
}

func (c *noErrMultiMapMapWrapper[K, V]) GetAll(key K) (val []V) {
	val, _ = c.m.GetAll(key)
	return
}

func (c *noErrMultiMapMapWrapper[K, V]) RemoveAll(key K) {
	_ = c.m.RemoveAll(key)
}

func (c *noErrMultiMapMapWrapper[K, V]) Remove(key K, val V) bool {
	exists, _ := c.m.Remove(key, val)
	return exists
}

func (c *noErrMultiMapMapWrapper[K, V]) Clear() {
	_ = c.m.Clear()
}

func (c *noErrMultiMapMapWrapper[K, V]) Size() int {
	return c.m.Size()
}

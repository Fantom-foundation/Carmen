package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"math/rand"
	"testing"
)

func TestMapGetPut(t *testing.T) {

	for name, mFactory := range initMaps() {
		t.Run(name, func(t *testing.T) {
			h := mFactory(5)

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

			verifyMapSorted(t, name, h)

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

			verifyMapSorted(t, name, h)

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
		})
	}
}

func TestMapInverseGetPut(t *testing.T) {
	for name, mFactory := range initMaps() {
		t.Run(name, func(t *testing.T) {
			h := mFactory(5)
			if _, exists := h.Get(A); exists {
				t.Errorf("Value is not correct")
			}

			h.Put(C, 30)
			h.Put(B, 20)
			h.Put(A, 10)

			verifyMapSorted(t, name, h)

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

			verifyMapSorted(t, name, h)

			if size := h.Size(); size != 3 {
				t.Errorf("Size does not fit: %d", size)
			}
		})
	}
}

func TestMapBulk(t *testing.T) {
	for name, mFactory := range initMaps() {
		t.Run(name, func(t *testing.T) {
			h := mFactory(5)
			if _, exists := h.Get(A); exists {
				t.Errorf("Value is not correct")
			}

			max := uint32(maxItems)
			data := make([]common.MapEntry[common.Address, uint32], max)
			for i := uint32(0); i < max; i++ {
				address := common.Address{byte(i + 1)}
				data[i] = common.MapEntry[common.Address, uint32]{address, i + 1}
			}

			h.BulkInsert(data)

			if size := h.Size(); size != int(max) {
				t.Errorf("Size does not match: %d != %d", size, max)
			}

			// inserted data must much returned data
			for i, entry := range h.GetEntries() {
				if entry.Key != data[i].Key || entry.Val != data[i].Val {
					t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, data[i].Key, data[i].Val)
				}
			}

			if size := len(h.GetEntries()); size != int(max) {
				t.Errorf("Size does not match: %d != %d", size, max)
			}

		})
	}
}

func TestMapBulkMultipleTimes(t *testing.T) {
	for name, mFactory := range initMaps() {
		t.Run(name, func(t *testing.T) {
			maxItems := 1000
			h := mFactory(maxItems)

			if _, exists := h.Get(A); exists {
				t.Errorf("Value is not correct")
			}

			max := uint32(maxItems / 2)
			data := make([]common.MapEntry[common.Address, uint32], 0, max)
			for i := uint32(0); i < max; i++ {
				address := common.Address{byte(i + 1)}
				data = append(data, common.MapEntry[common.Address, uint32]{address, i + 1})
			}

			h.BulkInsert(data)

			nextMax := uint32(maxItems/2 - 1)
			nextData := make([]common.MapEntry[common.Address, uint32], 0, nextMax)
			for i := max; i < nextMax+max; i++ {
				address := common.Address{byte(i + 1)}
				nextData = append(nextData, common.MapEntry[common.Address, uint32]{address, i + 1})
			}

			h.BulkInsert(nextData)

			allData := append(data, nextData...)
			// inserted data must much returned data
			for i, entry := range h.GetEntries() {
				if entry.Key != allData[i].Key || entry.Val != allData[i].Val {
					t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, allData[i].Key, allData[i].Val)
				}
			}

			if size := len(h.GetEntries()); size != int(max+nextMax) {
				t.Errorf("Size does not match: %d != %d", size, max+nextMax)
			}

			// pickup values in order
			arr := make([]common.Address, 0, max)
			h.ForEach(func(k common.Address, v uint32) {
				arr = append(arr, k)
			})
		})
	}
}

func TestMapSize(t *testing.T) {
	for name, mFactory := range initMaps() {
		t.Run(name, func(t *testing.T) {
			h := mFactory(10000)

			n := rand.Intn(9999)
			for i := 0; i < n; i++ {
				h.Put(common.AddressFromNumber(i), uint32(i))
			}

			if size := h.Size(); size != n {
				t.Errorf("Size is not correct: %d != %d", size, n)
			}
		})
	}
}

func TestMapRemove(t *testing.T) {
	for name, mFactory := range initMaps() {
		t.Run(name, func(t *testing.T) {
			h := mFactory(5)

			if exists := h.Remove(C); exists {
				t.Errorf("Remove from empty map failed")
			}

			h.Put(C, 99)
			if exists := h.Remove(C); !exists {
				t.Errorf("Remove failed:  %v", C)
			}
			if actual, exists := h.Get(C); exists || actual == 99 {
				t.Errorf("Remove failed:  %v -> %v", C, actual)
			}

			h.Put(A, 1)
			h.Put(B, 2)
			h.Put(C, 3)

			if size := h.Size(); size != 3 {
				t.Errorf("Size is not correct: %d ", size)
			}

			// remove from middle
			if exists := h.Remove(B); !exists {
				t.Errorf("Remove failed:  %v", B)
			}

			if size := h.Size(); size != 2 {
				t.Errorf("Size is not correct: %d ", size)
			}

			// remove from last
			if exists := h.Remove(C); !exists {
				t.Errorf("Remove failed:  %v", B)
			}

			if size := h.Size(); size != 1 {
				t.Errorf("Size is not correct: %d ", size)
			}

			if exists := h.Remove(A); !exists {
				t.Errorf("Remove failed:  %v", B)
			}

			h.ForEach(func(k common.Address, v uint32) {
				t.Errorf("There should be no item to iterata: %v -> %d", k, v)
			})
		})
	}
}

func TestMapClear(t *testing.T) {
	for name, mFactory := range initMaps() {
		t.Run(name, func(t *testing.T) {
			h := mFactory(5)

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
		})
	}
}

func verifyMapSorted(t *testing.T, name string, h noErrBulkInsertMap[common.Address, uint32]) {
	if name != "page" {
		return
	}

	keys := make([]common.Address, 0, h.Size())
	for _, entry := range h.GetEntries() {
		keys = append(keys, entry.Key)
	}
	common.AssertArraySorted[common.Address](t, keys, common.AddressComparator{})

	keys = make([]common.Address, 0, h.Size())
	h.ForEach(func(k common.Address, v uint32) {
		keys = append(keys, k)
	})

	common.AssertArraySorted[common.Address](t, keys, common.AddressComparator{})
}

func initMaps() map[string]func(capacity int) noErrBulkInsertMap[common.Address, uint32] {
	return map[string]func(capacity int) noErrBulkInsertMap[common.Address, uint32]{
		"pageList": newPageList,
		"page":     newPage,
	}
}

func newPageList(capacity int) noErrBulkInsertMap[common.Address, uint32] {
	// two pages in the pool, two items each
	pageCapacity := capacity / 2
	pagePool := NewPagePool[common.Address, uint32](pagePoolSize, pageCapacity, nil, NewMemoryPageStore[common.Address, uint32](), common.AddressComparator{})
	m := &noErrMapWrapper[common.Address, uint32]{NewPageList[common.Address, uint32](33, pageCapacity, pagePool)}
	return &noErrBulkInsertWrapper[common.Address, uint32]{m}
}

func newPage(capacity int) noErrBulkInsertMap[common.Address, uint32] {
	page := NewPage[common.Address, uint32](capacity, common.AddressComparator{})
	return &noErrBulkInsertWrapper[common.Address, uint32]{page}
}

// noErrMapWrapper converts the input map to non-err map
type noErrMapWrapper[K comparable, V any] struct {
	m common.BulkInsertMap[K, V]
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

func (c *noErrMapWrapper[K, V]) BulkInsert(data []common.MapEntry[K, V]) {
	_ = c.m.BulkInsert(data)
}

func (c *noErrMapWrapper[K, V]) GetEntries() []common.MapEntry[K, V] {
	entries, _ := c.m.GetEntries()
	return entries
}

// noErrBulkInsertMap converts methods BulkInsertMap to a variant that returns errors
type noErrBulkInsertMap[K comparable, V any] interface {
	common.Map[K, V]

	BulkInsert(data []common.MapEntry[K, V])
	GetEntries() []common.MapEntry[K, V]
}

type noErrBulkInsertWrapper[K comparable, V any] struct {
	m noErrBulkInsertMap[K, V]
}

func (c *noErrBulkInsertWrapper[K, V]) BulkInsert(data []common.MapEntry[K, V]) {
	c.m.BulkInsert(data)
}

func (c *noErrBulkInsertWrapper[K, V]) GetEntries() []common.MapEntry[K, V] {
	return c.m.GetEntries()
}

func (c *noErrBulkInsertWrapper[K, V]) ForEach(callback func(K, V)) {
	c.m.ForEach(callback)
}

func (c *noErrBulkInsertWrapper[K, V]) Get(key K) (val V, exists bool) {
	return c.m.Get(key)
}

func (c *noErrBulkInsertWrapper[K, V]) Put(key K, val V) {
	c.m.Put(key, val)
}

func (c *noErrBulkInsertWrapper[K, V]) Remove(key K) (exists bool) {
	return c.m.Remove(key)
}
func (c *noErrBulkInsertWrapper[K, V]) Clear() {
	c.m.Clear()
}

func (c *noErrBulkInsertWrapper[K, V]) Size() int {
	return c.m.Size()
}

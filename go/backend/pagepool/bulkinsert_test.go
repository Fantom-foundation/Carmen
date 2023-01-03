package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestMapBulk(t *testing.T) {
	for name, mFactory := range initBulkInserts() {
		t.Run(name, func(t *testing.T) {
			h := mFactory(5)

			h.forEach(func(k common.Address, v uint32) {
				t.Errorf("should be empty")
			})

			max := uint32(maxItems)
			data := make([]common.MapEntry[common.Address, uint32], max)
			for i := uint32(0); i < max; i++ {
				address := common.Address{byte(i + 1)}
				data[i] = common.MapEntry[common.Address, uint32]{address, i + 1}
			}

			h.bulkInsert(data)

			if size := h.size(); size != int(max) {
				t.Errorf("SizeBytes does not match: %d != %d", size, max)
			}

			// inserted data must much returned data
			for i, entry := range h.getEntries() {
				if entry.Key != data[i].Key || entry.Val != data[i].Val {
					t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, data[i].Key, data[i].Val)
				}
			}

			if size := len(h.getEntries()); size != int(max) {
				t.Errorf("SizeBytes does not match: %d != %d", size, max)
			}

		})
	}
}

func TestMapBulkMultipleTimes(t *testing.T) {
	for name, mFactory := range initBulkInserts() {
		t.Run(name, func(t *testing.T) {
			maxItems := 1000
			h := mFactory(maxItems)

			h.forEach(func(k common.Address, v uint32) {
				t.Errorf("should be empty")
			})

			max := uint32(maxItems / 2)
			data := make([]common.MapEntry[common.Address, uint32], 0, max)
			for i := uint32(0); i < max; i++ {
				address := common.Address{byte(i + 1)}
				data = append(data, common.MapEntry[common.Address, uint32]{address, i + 1})
			}

			h.bulkInsert(data)

			nextMax := uint32(maxItems/2 - 1)
			nextData := make([]common.MapEntry[common.Address, uint32], 0, nextMax)
			for i := max; i < nextMax+max; i++ {
				address := common.Address{byte(i + 1)}
				nextData = append(nextData, common.MapEntry[common.Address, uint32]{address, i + 1})
			}

			h.bulkInsert(nextData)

			allData := append(data, nextData...)
			// inserted data must much returned data
			for i, entry := range h.getEntries() {
				if entry.Key != allData[i].Key || entry.Val != allData[i].Val {
					t.Errorf("Entries do not match: %v, %d != %v, %d", entry.Key, entry.Val, allData[i].Key, allData[i].Val)
				}
			}

			if size := len(h.getEntries()); size != int(max+nextMax) {
				t.Errorf("SizeBytes does not match: %d != %d", size, max+nextMax)
			}

			// pickup values in order
			arr := make([]common.Address, 0, max)
			h.forEach(func(k common.Address, v uint32) {
				arr = append(arr, k)
			})
		})
	}
}

func initBulkInserts() map[string]func(capacity int) noErrBulkInsert[common.Address, uint32] {
	return map[string]func(capacity int) noErrBulkInsert[common.Address, uint32]{
		"pageList": newPageList,
		"page":     newPage,
	}
}

func newPageList(capacity int) noErrBulkInsert[common.Address, uint32] {
	// two pages in the pool, two items each
	pageCapacity := capacity / 2
	pageFactory := pageFactory(pageCapacity)
	pagePool := NewPagePool[*KVPage[common.Address, uint32]](pagePoolSize, nil, NewMemoryPageStore(), pageFactory)
	return &noErrBulkInsertWrapper[common.Address, uint32]{NewPageList[common.Address, uint32](33, pageCapacity, pagePool)}
}

func newPage(capacity int) noErrBulkInsert[common.Address, uint32] {
	return pageFactory(capacity)()
}

type noErrBulkInsert[K comparable, V any] interface {
	bulkInsert(data []common.MapEntry[K, V])
	getEntries() []common.MapEntry[K, V]
	forEach(callback func(K, V))
	size() int
}

type errBulkInsert[K comparable, V any] interface {
	bulkInsert(data []common.MapEntry[K, V]) error
	getEntries() ([]common.MapEntry[K, V], error)
	forEach(callback func(K, V)) error
	size() int
}

// noErrBulkInsertWrapper converts the input  to non-err map
type noErrBulkInsertWrapper[K comparable, V any] struct {
	m errBulkInsert[K, V]
}

func (w *noErrBulkInsertWrapper[K, V]) bulkInsert(data []common.MapEntry[K, V]) {
	_ = w.m.bulkInsert(data)
}

func (w *noErrBulkInsertWrapper[K, V]) getEntries() []common.MapEntry[K, V] {
	data, _ := w.m.getEntries()
	return data
}

func (w *noErrBulkInsertWrapper[K, V]) forEach(callback func(K, V)) {
	_ = w.m.forEach(callback)
}

func (w *noErrBulkInsertWrapper[K, V]) size() int {
	return w.m.size()
}

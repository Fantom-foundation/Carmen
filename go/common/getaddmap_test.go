package common_test

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestMapsGetOrAdd(t *testing.T) {
	// run the test for various map implementations
	for _, max := range inputSizes {
		for name, factory := range initGetOrAddMapFactories(t) {
			t.Run(fmt.Sprintf("%s %d", name, max), func(t *testing.T) {
				m := factory()

				// insert data
				data := make([]common.MapEntry[common.Address, uint32], 0, max)
				dataMap := make(map[common.Address]uint32)
				for i := 0; i < max; i++ {
					key := convert[common.Address](i+1, common.AddressSerializer{})
					value := convert[uint32](i+1, common.Identifier32Serializer{})
					data = append(data, common.MapEntry[common.Address, uint32]{key, value})
					dataMap[key] = value

					if val, exists := m.GetOrAdd(key, value); exists || val != value {
						t.Errorf("Cannot GetOrAdd: val: %v, exists: %v", val, exists)
					}
				}

				// add the same keys trying to replace values with the value += 1
				for i := 0; i < max; i++ {
					key := convert[common.Address](i+1, common.AddressSerializer{})
					value := convert[uint32](i+2, common.Identifier32Serializer{})

					// original value must be returned, no replace happens
					if val, exists := m.GetOrAdd(key, value); !exists || val != value-1 {
						t.Errorf("Cannot GetOrAdd: val: %v, exists: %v", val, exists)
					}
				}

				// verify data present by iterator
				visited := make(map[common.Address]bool)
				m.ForEach(func(actKey common.Address, actVal uint32) {
					expVal, exists := dataMap[actKey]
					if !exists || expVal != actVal {
						t.Errorf("Values does not match for key: %v, %d != %d", actKey, actVal, expVal)
					}
					if _, exists := visited[actKey]; exists {
						t.Errorf("the key has been already visited: %v", actKey)
					}
					visited[actKey] = true
				})

				// verify the size of visited elements
				if size := m.Size(); size != len(visited) {
					t.Errorf("Sizes does not match: %d != %d", size, len(visited))
				}
			})
		}
	}
}

// initMapFactories creates tested map factories
func initGetOrAddMapFactories(t *testing.T) map[string]func() getOrAdd[common.Address, uint32] {
	pageItems := 5
	numBuckets := 3

	pageSize := 1 << 8
	pagePoolSize := 3

	blockListFactory := func() getOrAdd[common.Address, uint32] {
		return common.NewBlockList[common.Address, uint32](pageItems, common.AddressComparator{})
	}
	linearHashFactory := func() getOrAdd[common.Address, uint32] {
		return common.NewLinearHashMap[common.Address, uint32](pageItems, numBuckets, common.AddressHasher{}, common.AddressComparator{})
	}

	singlePageListFactory := func() getOrAdd[common.Address, uint32] {
		eachPageStore := pagepool.NewMemoryPageStore[common.Address, uint32]()
		eachPagePool := pagepool.NewPagePool[common.Address, uint32](pagePoolSize, pageItems, nil, eachPageStore, common.AddressComparator{})
		return &getOrAddWrapper[common.Address, uint32]{pagepool.NewPageMapList[common.Address, uint32](123, pageItems, eachPagePool)}
	}

	sharedPageStore := pagepool.NewMemoryPageStore[common.Address, uint32]()
	sharedPagePool := pagepool.NewPagePool[common.Address, uint32](pagePoolSize, pageItems, nil, sharedPageStore, common.AddressComparator{})
	linearHashPagePoolFactory := func() getOrAdd[common.Address, uint32] {
		return &getOrAddWrapper[common.Address, uint32]{pagepool.NewLinearHashMap[common.Address, uint32](pageItems, numBuckets, sharedPagePool, common.AddressHasher{}, common.AddressComparator{})}
	}

	persistedLinearHashPagePoolFactory := func() getOrAdd[common.Address, uint32] {
		persistedSharedPageStore, _ := pagepool.NewFilePageStorage[common.Address, uint32](t.TempDir(), pageSize, pageItems, 0, 0, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
		persistedSharedPagePool := pagepool.NewPagePool[common.Address, uint32](pagePoolSize, pageItems, nil, persistedSharedPageStore, common.AddressComparator{})
		return &getOrAddWrapper[common.Address, uint32]{pagepool.NewLinearHashMap[common.Address, uint32](pageItems, numBuckets, persistedSharedPagePool, common.AddressHasher{}, common.AddressComparator{})}
	}

	factories := map[string]func() getOrAdd[common.Address, uint32]{
		"blockList":                    blockListFactory,                   // in-memory block list for linear hash
		"linearHash":                   linearHashFactory,                  // in-memory linear hash
		"pageList":                     singlePageListFactory,              // paged list for paged linear hash
		"linearHashPagePool":           linearHashPagePoolFactory,          // paged linear hash with in-memory page store
		"persistentLinearHashPagePool": persistedLinearHashPagePoolFactory, // paged linear hash with persisted pages
	}

	return factories
}

type errGetOrAdd[K comparable, V any] interface {
	GetOrAdd(key K, val V) (V, bool, error)
	ForEach(callback func(K, V)) error
	Size() int
}

type getOrAdd[K comparable, V any] interface {
	GetOrAdd(key K, val V) (V, bool)
	ForEach(callback func(K, V))
	Size() int
}

type getOrAddWrapper[K comparable, V any] struct {
	m errGetOrAdd[K, V]
}

func (w *getOrAddWrapper[K, V]) GetOrAdd(key K, val V) (V, bool) {
	v, exists, _ := w.m.GetOrAdd(key, val)
	return v, exists
}

func (c *getOrAddWrapper[K, V]) ForEach(callback func(K, V)) {
	_ = c.m.ForEach(callback)
}

func (c *getOrAddWrapper[K, V]) Size() int {
	return c.m.Size()
}

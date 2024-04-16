//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package common_test

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/index/file"
	"github.com/Fantom-foundation/Carmen/go/backend/index/memory"
	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
)

var inputSizes = []int{1, 10, 20, 1300, 12345}

// TestMapsFetchDataFromInitMap tests basic operations on various maps we provide
func TestMapsFetchDataFromInitMap(t *testing.T) {
	// run the test for various map implementations
	for _, max := range inputSizes {
		for name, factory := range initMapFactories(t) {
			t.Run(fmt.Sprintf("%s %d", name, max), func(t *testing.T) {
				m := factory()
				testData := initTestData(t, m, max)

				// verify size
				if size := m.Size(); size != len(testData) {
					t.Errorf("Sizes does not match: %d != %d", size, len(testData))
				}

				// verify values stored
				for expKey, expVal := range testData {
					if actVal, exists := m.Get(expKey); !exists || expVal != actVal {
						t.Errorf("Values does not match for key: %v, %d != %d ", expKey, actVal, expVal)
					}
				}

				// verify iterator
				visited := make(map[common.Address]bool)
				m.ForEach(func(actKey common.Address, actVal uint32) {
					expVal, exists := testData[actKey]
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

func TestMapsRemoveItemsFromInitMap(t *testing.T) {
	// run the test for various map implementations
	for _, max := range inputSizes {
		for name, factory := range initMapFactories(t) {
			t.Run(fmt.Sprintf("%s %d", name, max), func(t *testing.T) {
				m := factory()
				testData := initTestData(t, m, max)

				// remove keys and check if it does not exist
				for key, val := range testData {
					exists := m.Remove(key)
					if !exists {
						t.Errorf("Key does not exist: %v -> %v ", key, val)
					}
					if actual, exists := m.Get(key); exists {
						t.Errorf("Value should be deleted: key %v ->  %v", key, actual)
					}
				}

				// for-each does not see any data
				m.ForEach(func(k common.Address, v uint32) {
					t.Errorf("No iteration should happen")
				})

				// size should be zero
				if size := m.Size(); size != 0 {
					t.Errorf("Map should be empty")
				}
			})
		}
	}
}

func TestMapsClearInitMap(t *testing.T) {
	// run the test for various map implementations
	for _, max := range inputSizes {
		for name, factory := range initMapFactories(t) {
			t.Run(fmt.Sprintf("%s %d", name, max), func(t *testing.T) {
				m := factory()
				testData := initTestData(t, m, max)

				m.Clear()
				if size := m.Size(); size != 0 {
					t.Errorf("Map is not empty")
				}

				// verify values not stored
				for key := range testData {
					if _, exists := m.Get(key); exists {
						t.Errorf("Map is not empty, key %v found ", key)
					}
				}

				// verify iterator
				m.ForEach(func(actKey common.Address, actVal uint32) {
					t.Errorf("Map is not empty")
				})

				// size should be zero
				if size := m.Size(); size != 0 {
					t.Errorf("Map should be empty")
				}
			})
		}
	}
}

// initTestData generates test data and prefills the map
func initTestData(t *testing.T, initMap common.Map[common.Address, uint32], max int) map[common.Address]uint32 {
	rand := rand.New(rand.NewSource(123456))
	n := rand.Intn(max)

	keySerializer := common.AddressSerializer{}
	indexSerializer := common.Identifier32Serializer{}

	testData := make(map[common.Address]uint32, n)
	// insert random data
	for i := 1; i < n; i++ {
		key := convert[common.Address](rand.Intn(n), keySerializer)
		value := convert[uint32](i, indexSerializer)
		initMap.Put(key, value)
		testData[key] = value
	}

	return testData
}

// initMapFactories creates tested map factories
func initMapFactories(t *testing.T) map[string]func() common.Map[common.Address, uint32] {
	pageItems := 5
	numBuckets := 3

	pageSize := 1 << 8
	pagePoolSize := 3

	pageMetaSize := 2 + 4
	sizeBytes := pageMetaSize + pageItems*(common.AddressSerializer{}.Size()+common.Identifier32Serializer{}.Size())
	pageFactory := file.PageFactory[common.Address, uint32](sizeBytes, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})

	sortedMapFactory := func() common.Map[common.Address, uint32] {
		return common.NewSortedMap[common.Address, uint32](pageItems, common.AddressComparator{})
	}

	blockListFactory := func() common.Map[common.Address, uint32] {
		return memory.NewBlockList[common.Address, uint32](pageItems, common.AddressComparator{})
	}
	linearHashFactory := func() common.Map[common.Address, uint32] {
		return memory.NewLinearHashMap[common.Address, uint32](pageItems, numBuckets, common.AddressHasher{}, common.AddressComparator{})
	}

	singlePageListFactory := func() common.Map[common.Address, uint32] {
		eachPageStore := pagepool.NewMemoryPageStore[file.PageId](file.NextPageIdGenerator())
		eachPagePool := pagepool.NewPagePool[file.PageId, *file.IndexPage[common.Address, uint32]](pagePoolSize, eachPageStore, pageFactory)
		pageList := file.NewPageList[common.Address, uint32](123, pageItems, eachPagePool)
		return &noErrMapWrapper[common.Address, uint32]{&pageList}
	}

	sharedPageStore := pagepool.NewMemoryPageStore[file.PageId](file.NextPageIdGenerator())
	sharedPagePool := pagepool.NewPagePool[file.PageId, *file.IndexPage[common.Address, uint32]](pagePoolSize, sharedPageStore, pageFactory)
	linearHashPagePoolFactory := func() common.Map[common.Address, uint32] {
		return &noErrMapWrapper[common.Address, uint32]{file.NewLinearHashMap[common.Address, uint32](pageItems, numBuckets, 0, sharedPagePool, common.AddressHasher{}, common.AddressComparator{})}
	}

	persistedLinearHashPagePoolFactory := func() common.Map[common.Address, uint32] {
		persistedSharedPageStore, _ := file.NewTwoFilesPageStorage(t.TempDir(), pageSize)
		persistedSharedPagePool := pagepool.NewPagePool[file.PageId, *file.IndexPage[common.Address, uint32]](pagePoolSize, persistedSharedPageStore, pageFactory)
		return &noErrMapWrapper[common.Address, uint32]{file.NewLinearHashMap[common.Address, uint32](pageItems, numBuckets, 0, persistedSharedPagePool, common.AddressHasher{}, common.AddressComparator{})}
	}

	factories := map[string]func() common.Map[common.Address, uint32]{
		"sortedMap":                    sortedMapFactory,                   // in-memory map
		"blockList":                    blockListFactory,                   // in-memory block list for linear hash
		"linearHash":                   linearHashFactory,                  // in-memory linear hash
		"pageList":                     singlePageListFactory,              // paged list for paged linear hash
		"linearHashPagePool":           linearHashPagePoolFactory,          // paged linear hash with in-memory page store
		"persistentLinearHashPagePool": persistedLinearHashPagePoolFactory, // paged linear hash with persisted pages
	}

	return factories
}

func convert[V any](val int, serializer common.Serializer[V]) V {
	keyBytes := make([]byte, 32)
	binary.BigEndian.PutUint32(keyBytes, uint32(val))
	return serializer.FromBytes(keyBytes)
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

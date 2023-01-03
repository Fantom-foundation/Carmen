package common_test

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
	"math/rand"
	"testing"
)

func TestMultiMapAddFetchRemove(t *testing.T) {
	// run the test for various map implementations
	for _, max := range inputSizes {
		for name, factory := range initMultiMapMapFactories(t) {
			t.Run(fmt.Sprintf("%s %d", name, max), func(t *testing.T) {
				m := factory()

				// divide input size among a few keys
				numKeys := max/5 + 1
				keys := make([]common.Address, 0, numKeys)
				insertedData := make(map[common.Address][]uint32)
				for i := 0; i < numKeys; i++ {
					key := convert[common.Address](i+1, common.AddressSerializer{})
					keys = append(keys, key)
					insertedData[key] = make([]uint32, 0)
				}

				// insert data for the keys
				for i := 0; i < max; i++ {
					key := keys[i%numKeys]
					value := convert[uint32](i+1, common.Identifier32Serializer{})
					m.Add(key, value)
					insertedData[key] = append(insertedData[key], value)
				}

				if size := m.Size(); size != max {
					t.Errorf("Invalied size: %d != %d", size, max)
				}

				// test fetch values by the keys
				for _, key := range keys {
					expected := insertedData[key]
					actual := m.GetAll(key)
					common.AssertEqualArrays(t, expected, actual)
				}

				// test all data can be obtained by iteration
				visitedValues := make(map[uint32]bool)
				m.ForEach(func(actKey common.Address, actVal uint32) {
					if _, exists := visitedValues[actVal]; exists {
						t.Errorf("the value has been already visited: %v", actVal)
					}
					visitedValues[actVal] = true
					// check value is for expected key
					expectedVals := insertedData[actKey]
					var found bool
					for _, val := range expectedVals {
						if val == actVal {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("Value %d does not belong to key: %v", actVal, actKey)
					}
				})

				// verify the size of visited elements
				if size := m.Size(); size != len(visitedValues) {
					t.Errorf("Sizes does not match: %d != %d", size, len(visitedValues))
				}

				// remove values, test sizes match, and the data are no more available
				expectedSize := m.Size()
				for _, key := range keys {
					toRemoveSize := len(insertedData[key])
					m.RemoveAll(key)
					if actual := m.GetAll(key); len(actual) != 0 {
						t.Errorf("Removed key should return empty list: size: %d", len(actual))
					}

					expectedSize = expectedSize - toRemoveSize
					// check sizes
					if size := m.Size(); size != expectedSize {
						t.Errorf("Invalied size: %d != %d", size, expectedSize)
					}
				}
			})
		}
	}
}

func TestMapMultiMapRemoveSingleValues(t *testing.T) {
	// run the test for various map implementations
	for _, max := range inputSizes {
		for name, factory := range initMultiMapMapFactories(t) {
			t.Run(fmt.Sprintf("%s %d", name, max), func(t *testing.T) {
				m := factory()

				// divide input size among a few keys
				numKeys := max/5 + 1
				keys := make([]common.Address, 0, numKeys)
				insertedData := make(map[common.Address][]uint32)
				for i := 0; i < numKeys; i++ {
					key := convert[common.Address](i+1, common.AddressSerializer{})
					keys = append(keys, key)
					insertedData[key] = make([]uint32, 0)
				}

				// insert data for the keys
				for i := 0; i < max; i++ {
					key := keys[i%numKeys]
					value := convert[uint32](i+1, common.Identifier32Serializer{})
					m.Add(key, value)
					insertedData[key] = append(insertedData[key], value)
				}

				if size := m.Size(); size != max {
					t.Errorf("Invalied size: %d != %d", size, max)
				}

				// remove three values for every key
				expectedSize := m.Size()
				for i, key := range keys {
					start := 0                        // one value from the beginning
					inner := i                        // one value within the keys range
					end := len(insertedData[key]) - 1 // one value from the end

					for _, index := range []int{end, inner, start} {
						if index < len(insertedData[key]) {
							val := insertedData[key][index]
							insertedData[key] = remove(insertedData[key], index)
							if exists := m.Remove(key, val); !exists {
								t.Errorf("Error to remove one val: %v -> %d", key, val)
							}
							expectedSize -= 1
						}
					}
				}

				// test fetch values by the keys
				for _, key := range keys {
					expected := insertedData[key]
					actual := m.GetAll(key)
					common.AssertEqualArrays(t, expected, actual)
				}

				if size := m.Size(); size != expectedSize {
					t.Errorf("Sizes does not match: %d != %d", size, expectedSize)
				}
			})
		}
	}
}

func TestMapsClearInitMultiMap(t *testing.T) {
	// run the test for various map implementations
	for _, max := range inputSizes {
		for name, factory := range initMultiMapMapFactories(t) {
			t.Run(fmt.Sprintf("%s %d", name, max), func(t *testing.T) {
				m := factory()
				testData := initTestMultiMapData(t, m, max)

				m.Clear()
				if size := m.Size(); size != 0 {
					t.Errorf("Map is not empty")
				}

				// verify values not stored
				for key := range testData {
					if vals := m.GetAll(key); len(vals) > 0 {
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
func initTestMultiMapData(t *testing.T, initMap common.MultiMap[common.Address, uint32], max int) map[common.Address]uint32 {
	rand.Seed(123456)
	//rand.Seed(time.Now().UnixNano())
	n := rand.Intn(max)

	keySerializer := common.AddressSerializer{}
	indexSerializer := common.Identifier32Serializer{}

	testData := make(map[common.Address]uint32, n)
	// insert random data
	for i := 1; i < n; i++ {
		key := convert[common.Address](rand.Intn(n), keySerializer)
		value := convert[uint32](i, indexSerializer)
		initMap.Add(key, value)
		testData[key] = value
	}

	return testData
}

// initMultiMapMapFactories creates tested map factories
func initMultiMapMapFactories(t *testing.T) map[string]func() common.MultiMap[common.Address, uint32] {
	pageItems := 5
	numBuckets := 3
	pagePoolSize := 3

	pageSize := pagepool.ByteSizePage[common.Address, uint32](pageItems, common.AddressSerializer{}, common.Identifier32Serializer{})
	pageFactory := pageFactory(pageItems)

	singlePageListFactory := func() common.MultiMap[common.Address, uint32] {
		eachPageStore := pagepool.NewMemoryPageStore()
		eachPagePool := pagepool.NewPagePool[*pagepool.KVPage[common.Address, uint32]](pagePoolSize, nil, eachPageStore, pageFactory)
		return &noErrMultiMapMapWrapper[common.Address, uint32]{pagepool.NewPageMultiMapList[common.Address, uint32](123, pageItems, eachPagePool)}
	}

	sharedPageStore := pagepool.NewMemoryPageStore()
	sharedPagePool := pagepool.NewPagePool[*pagepool.KVPage[common.Address, uint32]](pagePoolSize, nil, sharedPageStore, pageFactory)
	linearHashPagePoolFactory := func() common.MultiMap[common.Address, uint32] {
		return &noErrMultiMapMapWrapper[common.Address, uint32]{pagepool.NewLinearHashMultiMap[common.Address, uint32](pageItems, numBuckets, sharedPagePool, common.AddressHasher{}, common.AddressComparator{})}
	}

	persistedLinearHashPagePoolFactory := func() common.MultiMap[common.Address, uint32] {
		persistedSharedPageStore, _ := pagepool.NewTwoFilesPageStorage(t.TempDir(), pageSize, 0, 0)
		persistedSharedPagePool := pagepool.NewPagePool[*pagepool.KVPage[common.Address, uint32]](pagePoolSize, nil, persistedSharedPageStore, pageFactory)
		return &noErrMultiMapMapWrapper[common.Address, uint32]{pagepool.NewLinearHashMultiMap[common.Address, uint32](pageItems, numBuckets, persistedSharedPagePool, common.AddressHasher{}, common.AddressComparator{})}
	}

	factories := map[string]func() common.MultiMap[common.Address, uint32]{
		"pageMultiMapList":                     singlePageListFactory,
		"linearHashMultiMapPagePool":           linearHashPagePoolFactory,
		"persistentLinearHashMultiMapPagePool": persistedLinearHashPagePoolFactory,
	}

	return factories
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

// Remove deletes the key from the map and returns whether an element was removed.
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

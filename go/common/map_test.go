package common_test

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/index/file"
	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
	"math/rand"
	"testing"
)

var inputSizes = []int{1, 10, 20, 1300, 12345}

// TestMapsFetchDataFromInitMap tests basic operations on various maps we provide
func TestMapsFetchDataFromInitMap(t *testing.T) {
	// run the test for various map implementations
	for _, max := range inputSizes {
		for name, factory := range initMapFactories(t) {
			t.Run(fmt.Sprintf("%s %d", name, max), func(t *testing.T) {
				capacity := 10
				m := factory(0, capacity)
				testData := initTestData(t, m, max)

				// verify size
				if size := m.Size(); size != len(testData) {
					t.Errorf("Sizes does not match: %d != %d", size, len(testData))
				}

				// verify values stored
				for expKey, expVal := range testData {
					if actVal, exists, err := m.Get(expKey); err != nil || !exists || expVal != actVal {
						t.Errorf("Values does not match for key: %v, %d != %d (err: %s)", expKey, actVal, expVal, err)
					}
				}

				// verify iterator
				visited := make(map[common.Address]bool)
				err := m.ForEach(func(actKey common.Address, actVal uint32) {
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

				if err != nil {
					t.Errorf("error: %s", err)
				}
			})
		}
	}
}

func TestMapMultiMap(t *testing.T) {
	// run the test for various map implementations
	for _, max := range inputSizes {
		for name, factory := range initMapFactories(t) {
			t.Run(fmt.Sprintf("%s %d", name, max), func(t *testing.T) {

				if name == "blockList" || name == "sortedMap" || name == "linearHash" {
					t.Skipf("no implemented yet")
				}

				capacity := 10
				m := factory(0, capacity)

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
					if err := m.Add(key, value); err != nil {
						t.Errorf("Error to add value: %v", err)
					}
					insertedData[key] = append(insertedData[key], value)
				}

				if size := m.Size(); size != max {
					t.Errorf("Invalied size: %d != %d", size, max)
				}

				// test fetch values by the keys
				for i := 0; i < numKeys; i++ {
					key := keys[i]
					expected := insertedData[key]
					actual, err := m.GetAll(key)
					if err != nil {
						t.Errorf("Error to get value: %v", err)
					}

					common.AssertEqualArrays(t, expected, actual)
				}

				// test all data can be obtained by iteration
				visitedValues := make(map[uint32]bool)
				err := m.ForEach(func(actKey common.Address, actVal uint32) {
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
				if err != nil {
					t.Errorf("error: %s", err)
				}

				// verify the size of visited elements
				if size := m.Size(); size != len(visitedValues) {
					t.Errorf("Sizes does not match: %d != %d", size, len(visitedValues))
				}

				// remove values, test sizes match, and the data are no more available
				expectedSize := m.Size()
				for _, key := range keys {
					toRemoveSize := len(insertedData[key])
					if err := m.RemoveAll(key); err != nil {
						t.Errorf("Error to get value: %v", err)
					}
					if actual, err := m.GetAll(key); err != nil || len(actual) != 0 {
						t.Errorf("Removed key should return empty list: size: %d, err: %v", len(actual), err)
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

// TestMapBulkInsert tests bulk operations on various maps we provide
func TestMapBulkInsert(t *testing.T) {
	// run the test for various map implementations
	for _, max := range inputSizes {
		for name, factory := range initBulkMapFactories() {
			t.Run(fmt.Sprintf("%s %d", name, max), func(t *testing.T) {
				capacity := 10
				m := factory(capacity)

				// generate dataset to bulk insert
				data := make([]common.MapEntry[common.Address, uint32], 0, max)
				dataMap := make(map[common.Address]uint32)
				for i := 0; i < max; i++ {
					key := convert[common.Address](i+1, common.AddressSerializer{})
					value := convert[uint32](i+1, common.Identifier32Serializer{})
					data = append(data, common.MapEntry[common.Address, uint32]{key, value})
					dataMap[key] = value
				}

				// bulk insert
				if err := m.BulkInsert(data); err != nil {
					t.Errorf("error: %s", err)
				}

				entries, err := m.GetEntries()
				if err != nil {
					t.Errorf("error: %s", err)
				}
				if len(entries) != len(data) {
					t.Errorf("Size of provided data does not much")
				}

				for _, entry := range entries {
					if expected, exists := dataMap[entry.Key]; !exists || expected != entry.Val {
						t.Errorf("Values does not match for key: %v, %d != %d", entry.Key, entry.Val, expected)
					}
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
				capacity := 10
				m := factory(0, capacity)
				testData := initTestData(t, m, max)

				// remove keys and check if it does not exist
				for key, val := range testData {
					exists, err := m.Remove(key)
					if err != nil {
						t.Errorf("error: %s", err)
					}
					if !exists {
						t.Errorf("Key does not exist: %v -> %v ", key, val)
					}
					if actual, exists, err := m.Get(key); err != nil || exists {
						t.Errorf("Value should be deleted: key %v ->  %v (err: %s)", key, actual, err)
					}
				}

				// for-each does not see any data
				err := m.ForEach(func(k common.Address, v uint32) {
					t.Errorf("No iteration should happen")
				})
				if err != nil {
					t.Errorf("error: %s", err)
				}

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
				capacity := 10
				m := factory(0, capacity)
				testData := initTestData(t, m, max)

				err := m.Clear()
				if err != nil {
					t.Errorf("error: %s", err)
				}

				if size := m.Size(); size != 0 {
					t.Errorf("Map is not empty")
				}

				// verify values not stored
				for key := range testData {
					if _, exists, err := m.Get(key); err != nil || exists {
						t.Errorf("Map is not empty, key %v found (err: %s", key, err)
					}
				}

				// verify iterator
				err = m.ForEach(func(actKey common.Address, actVal uint32) {
					t.Errorf("Map is not empty")
				})
				if err != nil {
					t.Errorf("error: %s", err)
				}

				// size should be zero
				if size := m.Size(); size != 0 {
					t.Errorf("Map should be empty")
				}
			})
		}
	}
}

// initTestData generates test data and prefills the map
func initTestData(t *testing.T, initMap common.ErrMap[common.Address, uint32], max int) map[common.Address]uint32 {
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
		if err := initMap.Put(key, value); err != nil {
			t.Fatalf("Cannot insert test data: %s", err)
		}
		testData[key] = value
	}

	return testData
}

// initBulkMapFactories creates tested map factories
func initBulkMapFactories() map[string]func(parameters ...int) common.BulkInsert[common.Address, uint32] {
	pageItems := 5

	sortedMapFactory := func(parameters ...int) common.BulkInsert[common.Address, uint32] {
		return &noErrBulkInsertWrapper[common.Address, uint32]{common.NewSortedMap[common.Address, uint32](pageItems, common.AddressComparator{})}
	}

	pageFactory := func(parameters ...int) common.BulkInsert[common.Address, uint32] {
		// page capacity set to never overflow
		return &noErrBulkInsertWrapper[common.Address, uint32]{pagepool.NewPage[common.Address, uint32](1000000, common.AddressComparator{})}
	}

	blockListFactory := func(parameters ...int) common.BulkInsert[common.Address, uint32] {
		return common.NewBlockList[common.Address, uint32](parameters[0], common.AddressComparator{})
	}

	pagePoolSize := 3
	singlePageListFactory := func(parameters ...int) common.BulkInsert[common.Address, uint32] {
		eachPageStore := pagepool.NewMemoryPageStore[common.Address, uint32]()
		eachPagePool := pagepool.NewPagePool[common.Address, uint32](pagePoolSize, pageItems, nil, eachPageStore, common.AddressComparator{})
		return pagepool.NewPageList[common.Address, uint32](123, pageItems, eachPagePool)
	}

	factories := map[string]func(parameters ...int) common.BulkInsert[common.Address, uint32]{
		"page":      pageFactory,
		"sortedMap": sortedMapFactory,
		"blockList": blockListFactory,
		"pageList":  singlePageListFactory,
	}

	return factories
}

// initMapFactories creates tested map factories
func initMapFactories(t *testing.T) map[string]func(bucket, capacity int) common.ErrMultiMap[common.Address, uint32] {
	pageItems := 5
	numBuckets := 3

	sortedMapFactory := func(bucket, capacity int) common.ErrMultiMap[common.Address, uint32] {
		return &noErrMapWrapper[common.Address, uint32]{common.NewSortedMap[common.Address, uint32](pageItems, common.AddressComparator{})}
	}

	pageFactory := func(bucket, capacity int) common.ErrMultiMap[common.Address, uint32] {
		// page capacity set to never overflow
		return &noErrMapWrapper[common.Address, uint32]{pagepool.NewPage[common.Address, uint32](1000000, common.AddressComparator{})}
	}

	blockListFactory := func(bucket, capacity int) common.ErrMultiMap[common.Address, uint32] {
		return common.NewBlockList[common.Address, uint32](capacity, common.AddressComparator{})
	}
	linearHashFactory := func(bucket, capacity int) common.ErrMultiMap[common.Address, uint32] {
		blockListFactory := func(bucket, capacity int) common.BulkInsertMap[common.Address, uint32] {
			return common.NewBlockList[common.Address, uint32](capacity, common.AddressComparator{})
		}
		return common.NewLinearHashMap[common.Address, uint32](numBuckets, capacity, common.AddressHasher{}, common.AddressComparator{}, blockListFactory)
	}

	sharedPageStore := pagepool.NewMemoryPageStore[common.Address, uint32]()
	sharedPagePool := pagepool.NewPagePool[common.Address, uint32](numBuckets, pageItems, nil, sharedPageStore, common.AddressComparator{})
	linearHashPagePoolFactory := func(bucket, capacity int) common.ErrMultiMap[common.Address, uint32] {
		pageListFactory := func(bucket, capacity int) common.BulkInsertMap[common.Address, uint32] {
			return pagepool.NewPageList[common.Address, uint32](bucket, capacity, sharedPagePool)
		}
		return common.NewLinearHashMap[common.Address, uint32](pageItems, capacity, common.AddressHasher{}, common.AddressComparator{}, pageListFactory)
	}

	pageSize := 1 << 8
	pagePoolSize := 3
	persistedPageListFactory := func(bucket, capacity int) common.BulkInsertMap[common.Address, uint32] {
		persistedSharedPageStore, _ := file.NewStorage[common.Address, uint32](t.TempDir(), pageSize, pageItems, 0, 0, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressComparator{})
		persistedSharedPagePool := pagepool.NewPagePool[common.Address, uint32](pagePoolSize, pageItems, nil, persistedSharedPageStore, common.AddressComparator{})
		return pagepool.NewPageList[common.Address, uint32](bucket, capacity, persistedSharedPagePool)
	}
	persistedLinearHashPagePoolFactory := func(bucket, capacity int) common.ErrMultiMap[common.Address, uint32] {
		return common.NewLinearHashMap[common.Address, uint32](pageItems, capacity, common.AddressHasher{}, common.AddressComparator{}, persistedPageListFactory)
	}
	singlePageListFactory := func(bucket, capacity int) common.ErrMultiMap[common.Address, uint32] {
		eachPageStore := pagepool.NewMemoryPageStore[common.Address, uint32]()
		eachPagePool := pagepool.NewPagePool[common.Address, uint32](pagePoolSize, pageItems, nil, eachPageStore, common.AddressComparator{})
		return pagepool.NewPageList[common.Address, uint32](123, pageItems, eachPagePool)
	}

	factories := map[string]func(bucket, capacity int) common.ErrMultiMap[common.Address, uint32]{
		"sortedMap":                    sortedMapFactory,
		"page":                         pageFactory,
		"blockList":                    blockListFactory,
		"linearHash":                   linearHashFactory,
		"pageList":                     singlePageListFactory,
		"linearHashPagePool":           linearHashPagePoolFactory,
		"persistentLinearHashPagePool": persistedLinearHashPagePoolFactory,
	}

	return factories
}

func convert[V any](val int, serializer common.Serializer[V]) V {
	keyBytes := make([]byte, 32)
	binary.BigEndian.PutUint32(keyBytes, uint32(val))
	return serializer.FromBytes(keyBytes)
}

// noErrMapWrapper converts the input map to ErrMap
type noErrMapWrapper[K comparable, V any] struct {
	m common.MultiMap[K, V]
}

func (c *noErrMapWrapper[K, V]) ForEach(callback func(K, V)) error {
	c.m.ForEach(callback)
	return nil
}

func (c *noErrMapWrapper[K, V]) Get(key K) (val V, exists bool, err error) {
	val, exists = c.m.Get(key)
	return
}

func (c *noErrMapWrapper[K, V]) GetAll(key K) (val []V, err error) {
	val = c.m.GetAll(key)
	return
}

func (c *noErrMapWrapper[K, V]) Put(key K, val V) error {
	c.m.Put(key, val)
	return nil
}

func (c *noErrMapWrapper[K, V]) Add(key K, val V) error {
	c.m.Add(key, val)
	return nil
}

// Remove deletes the key from the map and returns whether an element was removed.
func (c *noErrMapWrapper[K, V]) RemoveAll(key K) (err error) {
	c.m.RemoveAll(key)
	return
}

func (c *noErrMapWrapper[K, V]) Remove(key K) (exists bool, err error) {
	exists = c.m.Remove(key)
	return
}
func (c *noErrMapWrapper[K, V]) Clear() error {
	c.m.Clear()
	return nil
}

func (c *noErrMapWrapper[K, V]) Size() int {
	return c.m.Size()
}

// noErrBulkInsertMap converts methods BulkInsertMap to a variant that returns errors
type noErrBulkInsertMap[K comparable, V any] interface {
	BulkInsert(data []common.MapEntry[K, V])
	GetEntries() []common.MapEntry[K, V]
}

type noErrBulkInsertWrapper[K comparable, V any] struct {
	m noErrBulkInsertMap[K, V]
}

func (c *noErrBulkInsertWrapper[K, V]) BulkInsert(data []common.MapEntry[K, V]) error {
	c.m.BulkInsert(data)
	return nil
}

func (c *noErrBulkInsertWrapper[K, V]) GetEntries() ([]common.MapEntry[K, V], error) {
	entries := c.m.GetEntries()
	return entries, nil
}

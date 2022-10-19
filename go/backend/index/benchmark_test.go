package index_test

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/index/cache"
	"github.com/Fantom-foundation/Carmen/go/backend/index/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/index/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"testing"
)

// indexWrapper wraps an instance of the index to have serializers and the index available at hand
type indexWrapper[K comparable, I common.Identifier] struct {
	keySerializer   common.Serializer[K]
	indexSerializer common.Serializer[I]
	idx             index.Index[K, I]
}

// testConfig parametrise each benchmark
type testConfig[K comparable, I common.Identifier] struct {
	name         string
	initialSizes []uint32                  // initial number of keys inserted into the Index before the benchmark
	updateSizes  []uint32                  // numbers of extra elements to update.
	getIndex     func() indexWrapper[K, I] // create index implementation under test
}

// BenchmarkInsert benchmark inserts N keys into index implementations and measures addition of a sample .
func BenchmarkInsert(b *testing.B) {
	config := createConfiguration[common.Key, uint32](b, common.KeySerializer{}, common.Identifier32Serializer{})
	for _, c := range config {
		for _, initialSize := range c.initialSizes {
			idx := c.getIndex() // create the Index instance
			// insert 0...N-1 elements before benchmark starts
			idx.insertKeys(b, initialSize)
			keyShift := initialSize // use keyShift to make sure the keys are inserted after already inserted keys during the benchmark
			// Execute benchmark!
			b.Run(fmt.Sprintf("Index %s initialSize %d", c.name, initialSize), func(b *testing.B) {
				idx.benchmarkInsert(b, keyShift)
				keyShift += uint32(b.N)
			})
		}
	}
}

// BenchmarkRead benchmark inserts N keys into index implementations and measures read of a sample .
func BenchmarkRead(b *testing.B) {
	config := createConfiguration[common.Key, uint32](b, common.KeySerializer{}, common.Identifier32Serializer{})
	for _, c := range config {
		for _, initialSize := range c.initialSizes {
			idx := c.getIndex() // create the Index instance
			// insert 0...N-1 keys before benchmark starts
			idx.insertKeys(b, initialSize)
			// Execute benchmark for each distribution
			for _, dist := range common.GetDistributions(int(initialSize)) {
				b.Run(fmt.Sprintf("Index %s initialSize %d dist %s", c.name, initialSize, dist.Label), func(b *testing.B) {
					idx.benchmarkRead(b, dist)
				})
			}
		}
	}
}

// BenchmarkHash benchmark inserts N keys into index implementations and measures hashing of addition sample.
func BenchmarkHash(b *testing.B) {
	config := createConfiguration[common.Key, uint32](b, common.KeySerializer{}, common.Identifier32Serializer{})
	for _, c := range config {
		for _, initialSize := range c.initialSizes {
			idx := c.getIndex() // create the Index instance
			// insert 0...N-1 elements before benchmark starts
			idx.insertKeys(b, initialSize)
			_, _ = idx.idx.GetStateHash() // flush hash for initial keys
			keyShift := initialSize       // use keyShift to make sure the keys are inserted after already inserted keys during the benchmark
			// Execute benchmark!
			for _, updateSize := range c.updateSizes {
				b.Run(fmt.Sprintf("Index %s initialSize %d updateSize %d", c.name, initialSize, updateSize), func(b *testing.B) {
					idx.benchmarkHash(b, updateSize, keyShift)
					keyShift += uint32(b.N) * updateSize // increase by the number of iterations and the number of extra inserted elements
				})
			}
		}
	}
}

// benchmarkInsert inserts sample keys in the index , it starts from the input key
func (iw *indexWrapper[K, I]) benchmarkInsert(b *testing.B, keyShift uint32) {
	for i := uint32(0); i < uint32(b.N); i++ {
		// keyShift index to write after already written keys
		idx, err := iw.idx.GetOrAdd(iw.toKey(i + keyShift))
		if err != nil {
			b.Fatalf("failed to add item into Index; %s", err)
		}
		sink = idx // prevent compiler to optimize it out
	}
}

// benchmarkRead reads sample keys in the index
func (iw *indexWrapper[K, I]) benchmarkRead(b *testing.B, dist common.Distribution) {
	for i := 0; i < b.N; i++ {
		idx, err := iw.idx.GetOrAdd(iw.toKey(dist.GetNext()))
		if err != nil {
			b.Fatalf("failed to add item into Index; %s", err)
		}
		sink = idx // prevent compiler to optimize it out
	}
}

// benchmarkHash insert sample keys and measure time to compute hash.
// The inserted keys are shifted by the input offset.
func (iw *indexWrapper[K, I]) benchmarkHash(b *testing.B, updateSize, keyShift uint32) {
	for i := 0; i < b.N; i++ {

		b.StopTimer()
		for n := uint32(0); n < updateSize; n++ {
			if _, err := iw.idx.GetOrAdd(iw.toKey(n + keyShift)); err != nil {
				b.Fatalf("failed to add item %d, %s", n, err)
			}
		}
		keyShift += updateSize
		b.StartTimer()

		// this we measure
		idx, err := iw.idx.GetStateHash()
		if err != nil {
			b.Fatalf("failed to hash; %s", err)
		}
		sink = idx
	}
}

// insertKeys insert N elements into the Index
func (iw *indexWrapper[K, I]) insertKeys(b *testing.B, N uint32) {
	for n := uint32(0); n < N; n++ {
		if _, err := iw.idx.GetOrAdd(iw.toKey(n)); err != nil {
			b.Fatalf("Error to insert eleent %d, %s", n, err)
		}
	}
}

// createMemoryIndex create instance of memory index
func createMemoryIndex[K comparable, I common.Identifier](keySerializer common.Serializer[K], indexSerializer common.Serializer[I]) indexWrapper[K, I] {
	return indexWrapper[K, I]{keySerializer, indexSerializer, memory.NewIndex[K, I](keySerializer)}
}

// createLevelDbIndex create instance of LevelDB index
func createLevelDbIndex[K comparable, I common.Identifier](b *testing.B, keySerializer common.Serializer[K], indexSerializer common.Serializer[I]) indexWrapper[K, I] {
	db, err := leveldb.OpenFile(b.TempDir(), nil)
	if err != nil {
		b.Errorf("failed to init leveldb; %s", err)
	}

	b.Cleanup(func() {
		_ = db.Close()
	})

	idx, err := ldb.NewIndex[K, I](db, common.SlotKey, keySerializer, indexSerializer)
	if err != nil {
		b.Fatalf("failed to init leveldb index; %s", err)
	}

	return indexWrapper[K, I]{keySerializer, indexSerializer, idx}
}

// createEachMultiLevelDbIndex creates many instances of the index with one shared LevelDB instance
func createSharedMultiLevelDbIndex[K comparable, I common.Identifier](b *testing.B, keySerializer common.Serializer[K], indexSerializer common.Serializer[I]) indexWrapper[K, I] {

	// database instance shared by all index instances
	db, err := leveldb.OpenFile(b.TempDir(), nil)
	if err != nil {
		b.Errorf("failed to init leveldb; %s", err)
	}

	b.Cleanup(func() {
		_ = db.Close()
	})

	tableSpaces := []common.TableSpace{common.BalanceKey, common.NonceKey, common.SlotKey, common.ValueKey}
	indexArray := index.NewIndexArray[K, I]()
	for _, tableSpace := range tableSpaces {
		if idx, err := ldb.NewIndex[K, I](db, tableSpace, keySerializer, indexSerializer); err != nil {
			b.Fatalf("failed to init leveldb index; %s", err)
		} else {
			indexArray.Add(idx)
		}
	}

	return indexWrapper[K, I]{keySerializer, indexSerializer, indexArray}
}

// createEachMultiLevelDbIndex creates many instances of the index with each having its LevelDB instance
func createEachMultiLevelDbIndex[K comparable, I common.Identifier](b *testing.B, keySerializer common.Serializer[K], indexSerializer common.Serializer[I]) indexWrapper[K, I] {
	tableSpaces := []common.TableSpace{common.BalanceKey, common.NonceKey, common.SlotKey, common.ValueKey}
	indexArray := index.NewIndexArray[K, I]()
	for _, tableSpace := range tableSpaces {

		// new database instance for every new index
		db, err := leveldb.OpenFile(b.TempDir(), nil)
		if err != nil {
			b.Errorf("failed to init leveldb; %s", err)
		}

		b.Cleanup(func() {
			_ = db.Close()
		})

		if idx, err := ldb.NewIndex[K, I](db, tableSpace, keySerializer, indexSerializer); err != nil {
			b.Fatalf("failed to init leveldb index; %s", err)
		} else {
			indexArray.Add(idx)
		}
	}

	return indexWrapper[K, I]{keySerializer, indexSerializer, indexArray}
}

// createCachedLevelDbIndex create instance of LevelDB index with the cache
func createTransactLevelDbIndex[K comparable, I common.Identifier](b *testing.B, writeBufferSize int, keySerializer common.Serializer[K], indexSerializer common.Serializer[I]) indexWrapper[K, I] {
	opts := opt.Options{WriteBuffer: writeBufferSize}
	db, err := leveldb.OpenFile(b.TempDir(), &opts)
	if err != nil {
		b.Errorf("failed to init leveldb; %s", err)
	}

	tr, err := db.OpenTransaction()
	if err != nil {
		b.Errorf("failed to init leveldb transaction; %s", err)
	}

	b.Cleanup(func() {
		_ = tr.Commit()
	})

	b.Cleanup(func() {
		_ = db.Close()
	})

	idx, err := ldb.NewTransactIndex[K, I](tr, common.SlotKey, keySerializer, indexSerializer)
	if err != nil {
		b.Fatalf("failed to init leveldb index; %s", err)
	}

	return indexWrapper[K, I]{keySerializer, indexSerializer, idx}
}

// createCachedLevelDbIndex create instance of LevelDB index with the cache
func createCachedLevelDbIndex[K comparable, I common.Identifier](b *testing.B, cacheCapacity int, keySerializer common.Serializer[K], indexSerializer common.Serializer[I]) indexWrapper[K, I] {
	db, err := leveldb.OpenFile(b.TempDir(), nil)
	if err != nil {
		b.Errorf("failed to init leveldb; %s", err)
	}

	b.Cleanup(func() {
		_ = db.Close()
	})

	idx, err := ldb.NewIndex[K, I](db, common.SlotKey, keySerializer, indexSerializer)
	if err != nil {
		b.Fatalf("failed to init leveldb index; %s", err)
	}

	cached := cache.NewIndex[K, I](idx, cacheCapacity)
	return indexWrapper[K, I]{keySerializer, indexSerializer, cached}
}

var sink interface{}

// toKey converts the key from an input uint32 to the generic Key
func (iw *indexWrapper[K, I]) toKey(key uint32) K {
	keyBytes := binary.BigEndian.AppendUint32(make([]byte, 0, 32), key)
	return iw.keySerializer.FromBytes(keyBytes)
}

func createConfiguration[K comparable, I common.Identifier](b *testing.B, keySerializer common.Serializer[K], indexSerializer common.Serializer[I]) []testConfig[K, I] {

	// the size of buffer for transaction execution
	transactWriteBuffer := 512 * opt.MiB

	cacheCapacity := 2 << 15 // number of items: 2 ^ 15 * 32B = 1MB

	memoryIndexFunc := func() indexWrapper[K, I] { return createMemoryIndex[K, I](keySerializer, indexSerializer) }
	levelDbIndexFunc := func() indexWrapper[K, I] { return createLevelDbIndex[K, I](b, keySerializer, indexSerializer) }
	sharedLevelDbIndexFunc := func() indexWrapper[K, I] {
		return createSharedMultiLevelDbIndex[K, I](b, keySerializer, indexSerializer)
	}
	eachLevelDbIndexFunc := func() indexWrapper[K, I] { return createEachMultiLevelDbIndex[K, I](b, keySerializer, indexSerializer) }
	transactLevelDbIndexFunc := func() indexWrapper[K, I] { return createTransactLevelDbIndex[K, I](b, transactWriteBuffer, keySerializer, indexSerializer) }
	cachedLevelDbIndexFunc := func() indexWrapper[K, I] {
		return createCachedLevelDbIndex[K, I](b, cacheCapacity, keySerializer, indexSerializer)
	}

	initialSizes := []uint32{1 << 20, 1 << 24, 1 << 30}
	updateSizes := []uint32{100}

	//initialSizes := []uint32{1 << 5, 1 << 10} // debug Ns
	//updateSizes := []uint32{1, 2}             // debug Ms

	return []testConfig[K, I]{
		{"Memory", initialSizes, updateSizes, memoryIndexFunc},
		{"LevelDb", initialSizes, updateSizes, levelDbIndexFunc},
		{"SharedLevelDb", initialSizes, updateSizes, sharedLevelDbIndexFunc},
		{"EachLevelDb", initialSizes, updateSizes, eachLevelDbIndexFunc},
		{"TransactLevelDb", initialSizes, updateSizes, transactLevelDbIndexFunc},
		{"CachedLevelDb", initialSizes, updateSizes, cachedLevelDbIndexFunc},
	}
}

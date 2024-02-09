package index_test

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/index/cache"
	"github.com/Fantom-foundation/Carmen/go/backend/index/file"
	"github.com/Fantom-foundation/Carmen/go/backend/index/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/index/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"io"
	"testing"
)

// indexWrapper wraps an instance of the index to have serializers and the index available at hand
type indexWrapper[K comparable, I common.Identifier] struct {
	io.Closer
	keySerializer common.Serializer[K]
	idx           index.Index[K, I]
	cleanups      []func() error
}

// newIndexWrapper creates a new newIndexWrapper
func newIndexWrapper[K comparable, I common.Identifier](
	keySerializer common.Serializer[K],
	idx index.Index[K, I]) indexWrapper[K, I] {

	return indexWrapper[K, I]{
		keySerializer: keySerializer,
		idx:           idx}
}

// cleanUp registers a clean-up callback
func (iw *indexWrapper[K, I]) cleanUp(f func() error) {
	iw.cleanups = append(iw.cleanups, f)
}

// Close executes clean-up
func (iw *indexWrapper[K, I]) Close() error {
	for _, f := range iw.cleanups {
		_ = f()
	}
	return iw.idx.Close()
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
	config := createConfiguration[common.Key, uint32](b, common.KeySerializer{}, common.Identifier32Serializer{}, common.KeyHasher{}, common.KeyComparator{})
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
			_ = idx.Close()
		}
	}
}

// BenchmarkRead benchmark inserts N keys into index implementations and measures read of a sample .
func BenchmarkRead(b *testing.B) {
	config := createConfiguration[common.Key, uint32](b, common.KeySerializer{}, common.Identifier32Serializer{}, common.KeyHasher{}, common.KeyComparator{})
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
			_ = idx.Close()
		}
	}
}

// BenchmarkHash benchmark inserts N keys into index implementations and measures hashing of addition sample.
func BenchmarkHash(b *testing.B) {
	config := createConfiguration[common.Key, uint32](b, common.KeySerializer{}, common.Identifier32Serializer{}, common.KeyHasher{}, common.KeyComparator{})
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
			_ = idx.Close()
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
		sinkInt = uint32(idx) // prevent compiler to optimize it out
	}
}

// benchmarkRead reads sample keys in the index
func (iw *indexWrapper[K, I]) benchmarkRead(b *testing.B, dist common.Distribution) {
	for i := 0; i < b.N; i++ {
		idx, err := iw.idx.GetOrAdd(iw.toKey(dist.GetNext()))
		if err != nil {
			b.Fatalf("failed to add item into Index; %s", err)
		}
		sinkInt = uint32(idx) // prevent compiler to optimize it out
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
		hash, err := iw.idx.GetStateHash()
		if err != nil {
			b.Fatalf("failed to hash; %s", err)
		}
		sinkHash = hash
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
func createMemoryIndex[K comparable, I common.Identifier](keySerializer common.Serializer[K]) indexWrapper[K, I] {
	return newIndexWrapper[K, I](keySerializer, memory.NewIndex[K, I](keySerializer))
}

// createMemoryIndex creates instance of memory index
func createLinearHashMemoryIndex[K comparable, I common.Identifier](keySerializer common.Serializer[K], indexSerializer common.Serializer[I], hasher common.Hasher[K], comparator common.Comparator[K]) indexWrapper[K, I] {
	return newIndexWrapper[K, I](keySerializer, memory.NewLinearHashIndex[K, I](keySerializer, indexSerializer, hasher, comparator))
}

// createFileIndex creates instance of file index
func createFileIndex[K comparable, I common.Identifier](b *testing.B, keySerializer common.Serializer[K], indexSerializer common.Serializer[I], hasher common.Hasher[K], comparator common.Comparator[K]) indexWrapper[K, I] {
	idx, err := file.NewIndex[K, I](b.TempDir(), keySerializer, indexSerializer, hasher, comparator)
	if err != nil {
		b.Fatalf("failed to init file index; %s", err)
	}
	wrapper := newIndexWrapper[K, I](keySerializer, idx)
	wrapper.cleanUp(idx.Close)
	return wrapper
}

// createFileIndex creates instance of file index
func createCachedFileIndex[K comparable, I common.Identifier](b *testing.B, cacheSize int, keySerializer common.Serializer[K], indexSerializer common.Serializer[I], hasher common.Hasher[K], comparator common.Comparator[K]) indexWrapper[K, I] {
	idx := createFileIndex[K, I](b, keySerializer, indexSerializer, hasher, comparator)
	cached := cache.NewIndex[K, I](idx.idx, cacheSize)
	wrapper := newIndexWrapper[K, I](keySerializer, cached)
	wrapper.cleanUp(cached.Close)
	return wrapper
}

// createLevelDbIndex create instance of LevelDB index
func createLevelDbIndex[K comparable, I common.Identifier](b *testing.B, keySerializer common.Serializer[K], indexSerializer common.Serializer[I]) indexWrapper[K, I] {
	db, err := backend.OpenLevelDb(b.TempDir(), nil)
	if err != nil {
		b.Fatalf("failed to init leveldb; %s", err)
	}

	idx, err := ldb.NewIndex[K, I](db, common.KeyIndexKey, keySerializer, indexSerializer)
	if err != nil {
		b.Fatalf("failed to init leveldb index; %s", err)
	}

	wrapper := newIndexWrapper[K, I](keySerializer, idx)
	wrapper.cleanUp(db.Close)
	return wrapper
}

// createEachMultiLevelDbIndex creates many instances of the index with one shared LevelDB instance
func createSharedMultiLevelDbIndex[K comparable, I common.Identifier](b *testing.B, keySerializer common.Serializer[K], indexSerializer common.Serializer[I]) indexWrapper[K, I] {

	// database instance shared by all index instances
	db, err := backend.OpenLevelDb(b.TempDir(), nil)
	if err != nil {
		b.Fatalf("failed to init leveldb; %s", err)
	}

	tableSpaces := []common.TableSpace{common.BalanceStoreKey, common.NonceStoreKey, common.KeyIndexKey, common.ValueStoreKey}
	indexArray := index.NewIndexArray[K, I]()
	for _, tableSpace := range tableSpaces {
		if idx, err := ldb.NewIndex[K, I](db, tableSpace, keySerializer, indexSerializer); err != nil {
			b.Fatalf("failed to init leveldb index; %s", err)
		} else {
			indexArray.Add(idx)
		}
	}

	wrapper := newIndexWrapper[K, I](keySerializer, indexArray)
	wrapper.cleanUp(db.Close)
	return wrapper
}

// createEachMultiLevelDbIndex creates many instances of the index with each having its LevelDB instance
func createEachMultiLevelDbIndex[K comparable, I common.Identifier](b *testing.B, keySerializer common.Serializer[K], indexSerializer common.Serializer[I]) indexWrapper[K, I] {
	tableSpaces := []common.TableSpace{common.BalanceStoreKey, common.NonceStoreKey, common.KeyIndexKey, common.ValueStoreKey}
	indexArray := index.NewIndexArray[K, I]()
	wrapper := newIndexWrapper[K, I](keySerializer, indexArray)
	for _, tableSpace := range tableSpaces {

		// new database instance for every new index
		db, err := backend.OpenLevelDb(b.TempDir(), nil)
		if err != nil {
			b.Errorf("failed to init leveldb; %s", err)
		}
		wrapper.cleanUp(db.Close)

		if idx, err := ldb.NewIndex[K, I](db, tableSpace, keySerializer, indexSerializer); err != nil {
			b.Fatalf("failed to init leveldb index; %s", err)
		} else {
			indexArray.Add(idx)
		}
	}

	return wrapper
}

// createCachedLevelDbIndex create instance of LevelDB index with the cache
func createTransactLevelDbIndex[K comparable, I common.Identifier](b *testing.B, writeBufferSize int, keySerializer common.Serializer[K], indexSerializer common.Serializer[I]) indexWrapper[K, I] {
	opts := opt.Options{WriteBuffer: writeBufferSize}
	db, err := backend.OpenLevelDb(b.TempDir(), &opts)
	if err != nil {
		b.Errorf("failed to init leveldb; %s", err)
	}

	tr, err := db.OpenTransaction()
	if err != nil {
		b.Errorf("failed to init leveldb transaction; %s", err)
	}

	idx, err := ldb.NewIndex[K, I](tr, common.KeyIndexKey, keySerializer, indexSerializer)
	if err != nil {
		b.Fatalf("failed to init leveldb index; %s", err)
	}

	wrapper := newIndexWrapper[K, I](keySerializer, idx)
	wrapper.cleanUp(tr.Commit)
	wrapper.cleanUp(db.Close)
	return wrapper
}

// createCachedTransactLevelDbIndex create instance of LevelDB index witch is transactional and cached
func createCachedTransactLevelDbIndex[K comparable, I common.Identifier](b *testing.B, writeBufferSize int, cacheCapacity int, keySerializer common.Serializer[K], indexSerializer common.Serializer[I]) indexWrapper[K, I] {
	opts := opt.Options{WriteBuffer: writeBufferSize}
	db, err := backend.OpenLevelDb(b.TempDir(), &opts)
	if err != nil {
		b.Errorf("failed to init leveldb; %s", err)
	}

	tr, err := db.OpenTransaction()
	if err != nil {
		b.Errorf("failed to init leveldb transaction; %s", err)
	}

	idx, err := ldb.NewIndex[K, I](tr, common.KeyIndexKey, keySerializer, indexSerializer)
	if err != nil {
		b.Fatalf("failed to init leveldb index; %s", err)
	}
	cached := cache.NewIndex[K, I](idx, cacheCapacity)

	wrapper := newIndexWrapper[K, I](keySerializer, cached)
	wrapper.cleanUp(tr.Commit)
	wrapper.cleanUp(db.Close)
	return wrapper
}

// createCachedLevelDbIndex create instance of LevelDB index with the cache
func createCachedLevelDbIndex[K comparable, I common.Identifier](b *testing.B, cacheCapacity int, keySerializer common.Serializer[K], indexSerializer common.Serializer[I]) indexWrapper[K, I] {
	db, err := backend.OpenLevelDb(b.TempDir(), nil)
	if err != nil {
		b.Errorf("failed to init leveldb; %s", err)
	}

	idx, err := ldb.NewIndex[K, I](db, common.KeyIndexKey, keySerializer, indexSerializer)
	if err != nil {
		b.Fatalf("failed to init leveldb index; %s", err)
	}

	cached := cache.NewIndex[K, I](idx, cacheCapacity)
	wrapper := newIndexWrapper[K, I](keySerializer, cached)
	wrapper.cleanUp(db.Close)
	return wrapper
}

var sinkInt uint32
var sinkHash common.Hash

// toKey converts the key from an input uint32 to the generic Key
func (iw *indexWrapper[K, I]) toKey(key uint32) K {
	keyBytes := make([]byte, 32)
	binary.BigEndian.PutUint32(keyBytes, key)
	return iw.keySerializer.FromBytes(keyBytes)
}

func createConfiguration[K comparable, I common.Identifier](b *testing.B, keySerializer common.Serializer[K], indexSerializer common.Serializer[I], hasher common.Hasher[K], comparator common.Comparator[K]) []testConfig[K, I] {

	// the size of buffer for transaction execution
	transactWriteBuffer := 1024 * opt.MiB
	cacheCapacity := 1 << 25 // number of items: 2 ^ 25 * 32B = 1GB

	memoryIndexFunc := func() indexWrapper[K, I] { return createMemoryIndex[K, I](keySerializer) }
	linearHashMemoryIndexFunc := func() indexWrapper[K, I] {
		return createLinearHashMemoryIndex[K, I](keySerializer, indexSerializer, hasher, comparator)
	}
	linearHashFileIndexFunc := func() indexWrapper[K, I] {
		return createFileIndex[K, I](b, keySerializer, indexSerializer, hasher, comparator)
	}
	linearHashCachedFileIndexFunc := func() indexWrapper[K, I] {
		return createCachedFileIndex[K, I](b, cacheCapacity, keySerializer, indexSerializer, hasher, comparator)
	}
	levelDbIndexFunc := func() indexWrapper[K, I] { return createLevelDbIndex[K, I](b, keySerializer, indexSerializer) }
	sharedLevelDbIndexFunc := func() indexWrapper[K, I] {
		return createSharedMultiLevelDbIndex[K, I](b, keySerializer, indexSerializer)
	}
	eachLevelDbIndexFunc := func() indexWrapper[K, I] { return createEachMultiLevelDbIndex[K, I](b, keySerializer, indexSerializer) }
	transactLevelDbIndexFunc := func() indexWrapper[K, I] {
		return createTransactLevelDbIndex[K, I](b, transactWriteBuffer, keySerializer, indexSerializer)
	}
	cachedLevelDbIndexFunc := func() indexWrapper[K, I] {
		return createCachedLevelDbIndex[K, I](b, cacheCapacity, keySerializer, indexSerializer)
	}
	cachedTransactLevelDbIndexFunc := func() indexWrapper[K, I] {
		return createCachedTransactLevelDbIndex[K, I](b, transactWriteBuffer, cacheCapacity, keySerializer, indexSerializer)
	}

	initialSizes := []uint32{1 << 20, 1 << 24, 1 << 30}
	updateSizes := []uint32{100}

	return []testConfig[K, I]{
		{"Memory", initialSizes, updateSizes, memoryIndexFunc},
		{"MemoryLinearHash", initialSizes, updateSizes, linearHashMemoryIndexFunc},
		{"File", initialSizes, updateSizes, linearHashFileIndexFunc},
		{"CachedFile", initialSizes, updateSizes, linearHashCachedFileIndexFunc},
		{"LevelDb", initialSizes, updateSizes, levelDbIndexFunc},
		{"SharedLevelDb", initialSizes, updateSizes, sharedLevelDbIndexFunc},
		{"EachLevelDb", initialSizes, updateSizes, eachLevelDbIndexFunc},
		{"TransactLevelDb", initialSizes, updateSizes, transactLevelDbIndexFunc},
		{"CachedLevelDb", initialSizes, updateSizes, cachedLevelDbIndexFunc},
		{"CachedTransactLevelDb", initialSizes, updateSizes, cachedTransactLevelDbIndexFunc},
	}
}

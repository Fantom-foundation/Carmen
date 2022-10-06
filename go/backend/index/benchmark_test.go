package index_test

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/index/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/index/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
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
	name        string
	numElements []uint32                  // numbers of elements to insert.
	numUpdates  []uint32                  // numbers of extra elements to update.
	getIndex    func() indexWrapper[K, I] // create index implementation under test
}

// BenchmarkInsert benchmark inserts N keys into index implementations and measures addition of a sample .
func BenchmarkInsert(b *testing.B) {
	config := createConfiguration[common.Key, uint32](b, common.KeySerializer{}, common.Identifier32Serializer{})
	for _, d := range config {
		for _, n := range d.numElements {
			idx := d.getIndex() // create the Index instance
			// insert 0...N-1 elements before benchmark starts
			idx.insertKeys(b, n)
			keyShift := n // use keyShift to make sure the keys are inserted after already inserted keys during the benchmark
			// Execute benchmark!
			b.Run(fmt.Sprintf("Index %s numElements %d n %d", d.name, d.numElements, n), func(b *testing.B) {
				idx.benchmarkInsert(b, keyShift)
				keyShift += uint32(b.N)
			})
		}
	}
}

// BenchmarkRead benchmark inserts N keys into index implementations and measures read of a sample .
func BenchmarkRead(b *testing.B) {
	config := createConfiguration[common.Key, uint32](b, common.KeySerializer{}, common.Identifier32Serializer{})
	for _, d := range config {
		for _, n := range d.numElements {
			idx := d.getIndex() // create the Index instance
			// insert 0...N-1 keys before benchmark starts
			idx.insertKeys(b, n)
			// Execute benchmark for each distribution
			for _, dist := range common.GetDistributions(int(n)) {
				b.Run(fmt.Sprintf("Index %s numElements %d n %d dist %s", d.name, d.numElements, n, dist.Label), func(b *testing.B) {
					idx.benchmarkRead(b, dist)
				})
			}
		}
	}
}

// BenchmarkHash benchmark inserts N keys into index implementations and measures hashing of addition sample.
func BenchmarkHash(b *testing.B) {
	config := createConfiguration[common.Key, uint32](b, common.KeySerializer{}, common.Identifier32Serializer{})
	for _, d := range config {
		for _, n := range d.numElements {
			idx := d.getIndex() // create the Index instance
			// insert 0...N-1 elements before benchmark starts
			idx.insertKeys(b, n)
			_, _ = idx.idx.GetStateHash() // flush hash for initial keys
			keyShift := n                 // use keyShift to make sure the keys are inserted after already inserted keys during the benchmark
			// Execute benchmark!
			for _, m := range d.numUpdates {
				b.Run(fmt.Sprintf("Index %s numElements %d numUpdates %d n %d m %d", d.name, d.numElements, d.numUpdates, n, m), func(b *testing.B) {
					idx.benchmarkHash(b, m, keyShift)
					keyShift += uint32(b.N) * m // increase by the number of iterations and the number of extra inserted elements
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
func (iw *indexWrapper[K, I]) benchmarkHash(b *testing.B, inserts, keyShift uint32) {
	for i := 0; i < b.N; i++ {

		b.StopTimer()
		for n := uint32(0); n < inserts; n++ {
			if _, err := iw.idx.GetOrAdd(iw.toKey(n + keyShift)); err != nil {
				b.Fatalf("Error to insert eleent %d, %s", n, err)
			}
		}
		keyShift += inserts
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
	return indexWrapper[K, I]{keySerializer, indexSerializer, memory.NewMemory[K, I](keySerializer)}
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

	idx, err := ldb.NewKVIndex[K, I](db, common.SlotKey, keySerializer, indexSerializer)
	if err != nil {
		b.Fatalf("failed to init leveldb index; %s", err)
	}

	return indexWrapper[K, I]{keySerializer, indexSerializer, idx}
}

var sink interface{}

// toKey converts the key from an input uint32 to the generic Key
func (iw *indexWrapper[K, I]) toKey(key uint32) K {
	keyBytes := binary.BigEndian.AppendUint32([]byte{}, key)
	return iw.keySerializer.FromBytes(keyBytes)
}

func createConfiguration[K comparable, I common.Identifier](b *testing.B, keySerializer common.Serializer[K], indexSerializer common.Serializer[I]) []testConfig[K, I] {

	memoryIndexFunc := func() indexWrapper[K, I] { return createMemoryIndex[K, I](keySerializer, indexSerializer) }
	levelDbIndexFunc := func() indexWrapper[K, I] { return createLevelDbIndex[K, I](b, keySerializer, indexSerializer) }

	numElements := []uint32{0x1p20, 0x1p24, 0x1p30}
	numUpdates := []uint32{100}

	//numElements := []int{1 << 5, 1 << 10} // debug Ns
	//numUpdates := []int{1, 2}            // debug Ms

	return []testConfig[K, I]{
		{"MemoryIndex", numElements, numUpdates, memoryIndexFunc},
		{"LevelDbIndex", numElements, numUpdates, levelDbIndexFunc},
	}
}

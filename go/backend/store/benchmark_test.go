package store_test

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htfile"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htldb"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree/htmemory"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/backend/store/file"
	"github.com/Fantom-foundation/Carmen/go/backend/store/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/store/memory"
	"github.com/Fantom-foundation/Carmen/go/backend/store/pagedfile"
	"github.com/Fantom-foundation/Carmen/go/backend/store/pagedfile/eviction"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"math/rand"
	"testing"
)

// Benchmark of isolated Stores
// Use sub-benchmarks to test individual implementations with different parameters.
// The name of benchmark is in form:
// BenchmarkWrite/Store_File_initialSize_16777216_dist_Exponential
// where "File" is used Store implementation, "16777216" is the initial amount of items
// in the Store on the benchmark start and "Exponential" is a probability distribution
// with which are items (indexes) to write chosen.
// To run the benchmark for File-based impls and 2^24 initial items use regex like:
//     go test ./backend/store -bench=/.*File.*_16777216

const (
	PageSize        = 1 << 12 // = 4 KiB
	PoolSize        = 100
	BranchingFactor = 32
)

// initial number of values inserted into the Store before the benchmark
var initialSizes = []int{1 << 20, 1 << 24, 1 << 30}

// number of values updated before each measured hash recalculation
var updateSizes = []int{100}

var sinkValue common.Value
var sinkHash common.Hash

func BenchmarkInsert(b *testing.B) {
	for _, fac := range getStoresFactories() {
		for _, initialSize := range initialSizes {
			s := fac.getStore(b)
			b.Run(fmt.Sprintf("Store %s initialSize %d", fac.label, initialSize), func(b *testing.B) {
				initStoreContent(b, s, initialSize)
				benchmarkInsert(b, s)
			})
			_ = s.Close()
		}
	}
}

func benchmarkInsert(b *testing.B, store store.Store[uint32, common.Value]) {
	for i := 0; i < b.N; i++ {
		err := store.Set(uint32(i), toValue(uint32(i)))
		if err != nil {
			b.Fatalf("failed to set store item; %s", err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	for _, fac := range getStoresFactories() {
		for _, initialSize := range initialSizes {
			s := fac.getStore(b)
			initialized := false
			for _, dist := range common.GetDistributions(initialSize) {
				b.Run(fmt.Sprintf("Store %s initialSize %d dist %s", fac.label, initialSize, dist.Label), func(b *testing.B) {
					if !initialized {
						initStoreContent(b, s, initialSize)
						initialized = true
					}
					benchmarkRead(b, dist, s)
				})
			}
			_ = s.Close()
		}
	}
}

func benchmarkRead(b *testing.B, dist common.Distribution, store store.Store[uint32, common.Value]) {
	for i := 0; i < b.N; i++ {
		value, err := store.Get(dist.GetNext())
		if err != nil {
			b.Fatalf("failed to read item from store; %s", err)
		}
		sinkValue = value // prevent compiler to optimize it out
	}
}

func BenchmarkWrite(b *testing.B) {
	for _, fac := range getStoresFactories() {
		for _, initialSize := range initialSizes {
			s := fac.getStore(b)
			initialized := false
			for _, dist := range common.GetDistributions(initialSize) {
				b.Run(fmt.Sprintf("Store %s initialSize %d dist %s", fac.label, initialSize, dist.Label), func(b *testing.B) {
					if !initialized {
						initStoreContent(b, s, initialSize)
						initialized = true
					}
					benchmarkWrite(b, dist, s)
				})
			}
			_ = s.Close()
		}
	}
}

func benchmarkWrite(b *testing.B, dist common.Distribution, store store.Store[uint32, common.Value]) {
	for i := 0; i < b.N; i++ {
		err := store.Set(dist.GetNext(), toValue(uint32(i)))
		if err != nil {
			b.Fatalf("failed to set store item; %s", err)
		}
	}
}

func BenchmarkHash(b *testing.B) {
	for _, fac := range getStoresFactories() {
		for _, initialSize := range initialSizes {
			s := fac.getStore(b)
			initialized := false
			for _, updateSize := range updateSizes {
				for _, dist := range common.GetDistributions(initialSize) {
					b.Run(fmt.Sprintf("Store %s initialSize %d updateSize %d dist %s", fac.label, initialSize, updateSize, dist.Label), func(b *testing.B) {
						if !initialized {
							initStoreContent(b, s, initialSize)
							initialized = true
						}
						benchmarkHash(b, dist, updateSize, s)
					})
				}
			}
			_ = s.Close()
		}
	}
}

func benchmarkHash(b *testing.B, dist common.Distribution, updateSize int, store store.Store[uint32, common.Value]) {
	for i := 0; i < b.N; i++ {
		b.StopTimer() // don't measure the update
		for ii := 0; ii < updateSize; ii++ {
			err := store.Set(dist.GetNext(), toValue(rand.Uint32()))
			if err != nil {
				b.Fatalf("failed to set store item; %s", err)
			}
		}
		b.StartTimer()

		hash, err := store.GetStateHash()
		if err != nil {
			b.Fatalf("failed to hash store; %s", err)
		}
		sinkHash = hash // prevent compiler to optimize it out
	}
}

func BenchmarkWriteAndHash(b *testing.B) {
	for _, fac := range getStoresFactories() {
		for _, initialSize := range initialSizes {
			s := fac.getStore(b)
			initialized := false
			for _, updateSize := range updateSizes {
				for _, dist := range common.GetDistributions(initialSize) {
					b.Run(fmt.Sprintf("Store %s initialSize %d updateSize %d dist %s", fac.label, initialSize, updateSize, dist.Label), func(b *testing.B) {
						if !initialized {
							initStoreContent(b, s, initialSize)
							initialized = true
						}
						benchmarkWriteAndHash(b, dist, updateSize, s)
					})
				}
			}
			_ = s.Close()
		}
	}
}

func benchmarkWriteAndHash(b *testing.B, dist common.Distribution, updateSize int, store store.Store[uint32, common.Value]) {
	for i := 0; i < b.N; i++ {
		for ii := 0; ii < updateSize; ii++ {
			err := store.Set(dist.GetNext(), toValue(rand.Uint32()))
			if err != nil {
				b.Fatalf("failed to set store item; %s", err)
			}
		}

		hash, err := store.GetStateHash()
		if err != nil {
			b.Fatalf("failed to hash store; %s", err)
		}
		sinkHash = hash // prevent compiler to optimize it out
	}
}

type StoreFactory struct {
	label    string
	getStore func(b *testing.B) store.Store[uint32, common.Value]
}

func getStoresFactories() (stores []StoreFactory) {
	return []StoreFactory{
		{
			label:    "Memory",
			getStore: initMemStore,
		},
		{
			label:    "File",
			getStore: initFileStore,
		},
		{
			label:    "PagedFile",
			getStore: initPagedFileStore,
		},
		{
			label:    "LevelDb",
			getStore: initLevelDbStore,
		},
		{
			label:    "TransactLevelDb",
			getStore: initTransactLevelDbStore,
		},
	}
}

func toValue(i uint32) common.Value {
	value := common.Value{}
	binary.BigEndian.PutUint32(value[:], i)
	return value
}

func initMemStore(b *testing.B) (store store.Store[uint32, common.Value]) {
	str, err := memory.NewStore[uint32, common.Value](common.ValueSerializer{}, PageSize, htmemory.CreateHashTreeFactory(BranchingFactor))
	if err != nil {
		b.Fatalf("failed to init memory store; %s", err)
	}
	return str
}

func initFileStore(b *testing.B) (str store.Store[uint32, common.Value]) {
	str, err := file.NewStore[uint32, common.Value](b.TempDir(), common.ValueSerializer{}, PageSize, htfile.CreateHashTreeFactory(b.TempDir(), BranchingFactor))
	if err != nil {
		b.Fatalf("failed to init file store; %s", err)
	}
	return str
}

func initPagedFileStore(b *testing.B) (str store.Store[uint32, common.Value]) {
	str, err := pagedfile.NewStore[uint32, common.Value](b.TempDir(), common.ValueSerializer{}, PageSize, htfile.CreateHashTreeFactory(b.TempDir(), BranchingFactor), PoolSize, eviction.NewLRUPolicy(PoolSize))
	if err != nil {
		b.Fatalf("failed to init pagedfile store; %s", err)
	}
	return str
}

func initLevelDbStore(b *testing.B) (store store.Store[uint32, common.Value]) {
	db, err := leveldb.OpenFile(b.TempDir(), nil)
	if err != nil {
		b.Fatalf("failed to init leveldb store; %s", err)
	}
	hashTreeFac := htldb.CreateHashTreeFactory(db, common.ValueStoreKey, BranchingFactor)
	store, err = ldb.NewStore[uint32, common.Value](db, common.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, hashTreeFac, PageSize)
	if err != nil {
		b.Fatalf("failed to init leveldb store; %s", err)
	}
	b.Cleanup(func() {
		db.Close()
	})
	return store
}

func initTransactLevelDbStore(b *testing.B) (store store.Store[uint32, common.Value]) {
	writeBufferSize := 1024 * opt.MiB
	opts := opt.Options{WriteBuffer: writeBufferSize}
	db, err := leveldb.OpenFile(b.TempDir(), &opts)
	if err != nil {
		b.Fatalf("failed to init leveldb store; %s", err)
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

	hashTreeFac := htldb.CreateHashTreeFactory(tr, common.ValueStoreKey, BranchingFactor)
	store, err = ldb.NewStore[uint32, common.Value](tr, common.ValueStoreKey, common.ValueSerializer{}, common.Identifier32Serializer{}, hashTreeFac, PageSize)
	if err != nil {
		b.Fatalf("failed to init leveldb store; %s", err)
	}

	return store
}

func initStoreContent(b *testing.B, store store.Store[uint32, common.Value], dbSize int) {
	b.StopTimer() // dont measure initialization
	for i := 0; i < dbSize; i++ {
		value := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		err := store.Set(uint32(i), common.Value{value[0], value[1], value[2], value[3]})
		if err != nil {
			b.Fatalf("failed to set store item; %s", err)
		}
	}
	_, err := store.GetStateHash()
	if err != nil {
		b.Fatalf("failed to get store hash; %s", err)
	}
	b.StartTimer()
}

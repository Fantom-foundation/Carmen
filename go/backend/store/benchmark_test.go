package store_test

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/backend/store/file"
	"github.com/Fantom-foundation/Carmen/go/backend/store/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/store/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"math/rand"
	"testing"
)

const (
	PageSize        = 2
	BranchingFactor = 3
)

// initial number of values inserted into the Store before the benchmark
// var initialSizes = []int{1 << 20, 1 << 24, 1 << 30}
var initialSizes = []int{1 << 20, 1 << 24}

// number of values updated before each measured hash recalculation
var updateSizes = []int{100}

var sink interface{}

func BenchmarkInsert(b *testing.B) {
	for _, fac := range getStoresFactories() {
		for _, initialSize := range initialSizes {
			s := fac.getStore(b)
			initStoreContent(b, s, initialSize)
			b.Run(fmt.Sprintf("Store %s initialSize %d", fac.label, initialSize), func(b *testing.B) {
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
			initStoreContent(b, s, initialSize)
			for _, dist := range common.GetDistributions(initialSize) {
				b.Run(fmt.Sprintf("Store %s initialSize %d dist %s", fac.label, initialSize, dist.Label), func(b *testing.B) {
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
		sink = value // prevent compiler to optimize it out
	}
}

func BenchmarkWrite(b *testing.B) {
	for _, fac := range getStoresFactories() {
		for _, initialSize := range initialSizes {
			s := fac.getStore(b)
			initStoreContent(b, s, initialSize)
			for _, dist := range common.GetDistributions(initialSize) {
				b.Run(fmt.Sprintf("Store %s initialSize %d dist %s", fac.label, initialSize, dist.Label), func(b *testing.B) {
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
			initStoreContent(b, s, initialSize)
			for _, updateSize := range updateSizes {
				for _, dist := range common.GetDistributions(initialSize) {
					b.Run(fmt.Sprintf("Store %s initialSize %d updateSize %d dist %s", fac.label, initialSize, updateSize, dist.Label), func(b *testing.B) {
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
		sink = hash // prevent compiler to optimize it out
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
			label:    "LevelDb",
			getStore: initLevelDbStore,
		},
	}
}

func toValue(i uint32) common.Value {
	value := common.Value{}
	binary.BigEndian.PutUint32(value[:], i)
	return value
}

func initMemStore(b *testing.B) (store store.Store[uint32, common.Value]) {
	return memory.NewStore[uint32, common.Value](common.ValueSerializer{}, common.Value{}, PageSize, BranchingFactor)
}

func initFileStore(b *testing.B) (str store.Store[uint32, common.Value]) {
	str, err := file.NewStore[uint32, common.Value](b.TempDir(), common.ValueSerializer{}, common.Value{}, PageSize, BranchingFactor)
	if err != nil {
		b.Fatalf("failed to init file store; %s", err)
	}
	return str
}

func initLevelDbStore(b *testing.B) (store store.Store[uint32, common.Value]) {
	db, err := leveldb.OpenFile(b.TempDir(), nil)
	if err != nil {
		b.Fatalf("failed to init leveldb; %s", err)
	}
	hashTree := ldb.CreateHashTreeFactory(db, common.ValueKey, BranchingFactor)
	store, err = ldb.NewStore[uint32, common.Value](db, common.ValueKey, common.ValueSerializer{}, common.Identifier32Serializer{}, hashTree, common.Value{}, PageSize)
	if err != nil {
		b.Fatalf("failed to init leveldb store; %s", err)
	}
	b.Cleanup(func() {
		db.Close()
	})
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
	b.StartTimer()
}

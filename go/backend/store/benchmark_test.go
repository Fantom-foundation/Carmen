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

// initial sizes of databases in the amount of items to benchmark
var dbSizes = []int{1 << 5, 1 << 10}

var sink interface{}

func BenchmarkInsert(b *testing.B) {
	for _, fac := range getStoresFactories() {
		for _, dbSize := range dbSizes {
			s := fac.getStore(b)
			initStoreContent(b, s, dbSize)
			b.Run(fmt.Sprintf("%s", fac.label), func(b *testing.B) {
				benchmarkInsert(b, s)
			})
			_ = s.Close()
		}
	}
}

func benchmarkInsert(b *testing.B, store store.Store[uint32, common.Value]) {
	for i := 0; i < b.N; i++ {
		value := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		err := store.Set(uint32(i), common.Value{value[0], value[1], value[2], value[3]})
		if err != nil {
			b.Fatalf("failed to set store item; %s", err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	for _, fac := range getStoresFactories() {
		for _, dbSize := range dbSizes {
			s := fac.getStore(b)
			initStoreContent(b, s, dbSize)
			for _, dist := range getDistributions(dbSize) {
				b.Run(fmt.Sprintf("%s_%s_%d", dist.label, fac.label, dbSize), func(b *testing.B) {
					benchmarkRead(b, dist, s)
				})
			}
			_ = s.Close()
		}
	}
}

func benchmarkRead(b *testing.B, dist Distribution, store store.Store[uint32, common.Value]) {
	for i := 0; i < b.N; i++ {
		value, err := store.Get(dist.getId())
		if err != nil {
			b.Fatalf("failed to read item from store; %s", err)
		}
		sink = value // prevent compiler to optimize it out
	}
}

func BenchmarkWrite(b *testing.B) {
	for _, fac := range getStoresFactories() {
		for _, dbSize := range dbSizes {
			s := fac.getStore(b)
			initStoreContent(b, s, dbSize)
			for _, dist := range getDistributions(dbSize) {
				b.Run(fmt.Sprintf("%s_%s_%d", dist.label, fac.label, dbSize), func(b *testing.B) {
					benchmarkWrite(b, dist, s)
				})
			}
			_ = s.Close()
		}
	}
}

func benchmarkWrite(b *testing.B, dist Distribution, store store.Store[uint32, common.Value]) {
	for i := 0; i < b.N; i++ {
		value := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		err := store.Set(dist.getId(), common.Value{value[0], value[1], value[2], value[3]})
		if err != nil {
			b.Fatalf("failed to set store item; %s", err)
		}
	}
}

func BenchmarkHash(b *testing.B) {
	for _, fac := range getStoresFactories() {
		for _, dbSize := range dbSizes {
			s := fac.getStore(b)
			initStoreContent(b, s, dbSize)
			for _, dist := range getDistributions(dbSize) {
				b.Run(fmt.Sprintf("%s_%s_%d", dist.label, fac.label, dbSize), func(b *testing.B) {
					benchmarkHash(b, dist, s)
				})
			}
			_ = s.Close()
		}
	}
}

func benchmarkHash(b *testing.B, dist Distribution, store store.Store[uint32, common.Value]) {
	for i := 0; i < b.N; i++ {
		b.StopTimer() // don't measure the update
		for ii := 0; ii < 100; ii++ {
			value := binary.BigEndian.AppendUint32([]byte{}, rand.Uint32())
			err := store.Set(dist.getId(), common.Value{value[0], value[1], value[2], value[3]})
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
			label:    "memory",
			getStore: initMemStore,
		},
		{
			label:    "file",
			getStore: initFileStore,
		},
		{
			label:    "leveldb",
			getStore: initLevelDbStore,
		},
	}
}

type Distribution struct {
	label string
	getId func() uint32
}

func getDistributions(items int) []Distribution {
	expRate := float64(10) / float64(items)
	it := 0
	return []Distribution{
		{
			label: "Sequential",
			getId: func() uint32 {
				it = (it + 1) % items
				return uint32(it)
			},
		},
		{
			label: "Uniform",
			getId: func() uint32 {
				return uint32(rand.Intn(items))
			},
		},
		{
			label: "Exponential",
			getId: func() uint32 {
				return uint32(rand.ExpFloat64() / expRate)
			},
		},
	}
}

func initMemStore(b *testing.B) (store store.Store[uint32, common.Value]) {
	return memory.NewStore[uint32, common.Value](common.ValueSerializer{}, common.Value{}, PageSize, Factor)
}

func initFileStore(b *testing.B) (store store.Store[uint32, common.Value]) {
	store, err := file.NewStore[uint32, common.Value](b.TempDir(), common.ValueSerializer{}, common.Value{}, PageSize, Factor)
	if err != nil {
		b.Fatalf("failed to init file store; %s", err)
	}
	return store
}

func initLevelDbStore(b *testing.B) (store store.Store[uint32, common.Value]) {
	db, err := leveldb.OpenFile(b.TempDir(), nil)
	if err != nil {
		b.Fatalf("failed to init leveldb; %s", err)
	}
	hashTree := memory.CreateHashTreeFactory(Factor)
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

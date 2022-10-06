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

func BenchmarkInsert(b *testing.B) {
	stores := getLabeledStores(b)
	for _, s := range stores {
		b.Run(fmt.Sprintf("%s", s.label), func(b *testing.B) {
			benchmarkInsert(b, s.store)
		})
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
	stores := getLabeledStores(b)
	dbSizes := []int{1 << 5, 1 << 10}

	for _, s := range stores {
		for _, dbSize := range dbSizes {
			initStoreContent(b, s.store, dbSize)
			for _, selector := range getSelectors(dbSize) {
				b.Run(fmt.Sprintf("%s_%s_%d", selector.name, s.label, dbSize), func(b *testing.B) {
					benchmarkRead(b, selector, s.store)
				})
			}
		}
	}
}

func benchmarkRead(b *testing.B, selector Selector, store store.Store[uint32, common.Value]) {
	for i := 0; i < b.N; i++ {
		value, err := store.Get(selector.selector(i))
		if err != nil {
			b.Fatalf("failed to read item from store; %s", err)
		}
		sink = value // prevent compiler to optimize it out
	}
}

func BenchmarkWrite(b *testing.B) {
	stores := getLabeledStores(b)
	dbSizes := []int{1 << 5, 1 << 10}

	for _, s := range stores {
		for _, dbSize := range dbSizes {
			initStoreContent(b, s.store, dbSize)
			for _, selector := range getSelectors(dbSize) {
				b.Run(fmt.Sprintf("%s_%s_%d", selector.name, s.label, dbSize), func(b *testing.B) {
					benchmarkWrite(b, selector, s.store)
				})
			}
		}
	}
}

func benchmarkWrite(b *testing.B, selector Selector, store store.Store[uint32, common.Value]) {
	for i := 0; i < b.N; i++ {
		value := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		err := store.Set(selector.selector(i), common.Value{value[0], value[1], value[2], value[3]})
		if err != nil {
			b.Fatalf("failed to set store item; %s", err)
		}
	}
}

func BenchmarkHash(b *testing.B) {
	stores := getLabeledStores(b)
	dbSizes := []int{1 << 5, 1 << 10}

	for _, s := range stores {
		for _, dbSize := range dbSizes {
			initStoreContent(b, s.store, dbSize)
			for _, selector := range getSelectors(dbSize) {
				b.Run(fmt.Sprintf("%s_%s_%d", selector.name, s.label, dbSize), func(b *testing.B) {
					benchmarkHash(b, selector, s.store)
				})
			}
		}
	}
}

func benchmarkHash(b *testing.B, selector Selector, store store.Store[uint32, common.Value]) {
	for i := 0; i < b.N; i++ {
		b.StopTimer() // don't measure the update
		for ii := 0; ii < 100; ii++ {
			value := binary.BigEndian.AppendUint32([]byte{}, rand.Uint32())
			err := store.Set(selector.selector(i), common.Value{value[0], value[1], value[2], value[3]})
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

type Selector struct {
	name     string
	selector func(i int) uint32
}

func getSelectors(items int) []Selector {
	expRate := float64(10) / float64(items)
	return []Selector{
		{
			name: "Sequential",
			selector: func(i int) uint32 {
				return uint32(i % items)
			},
		},
		{
			name: "Uniform",
			selector: func(i int) uint32 {
				return uint32(rand.Intn(items))
			},
		},
		{
			name: "Sequential",
			selector: func(i int) uint32 {
				return uint32(rand.ExpFloat64() / expRate)
			},
		},
	}
}

type LabeledStore struct {
	label string
	store store.Store[uint32, common.Value]
}

func getLabeledStores(b *testing.B) (stores []LabeledStore) {
	return []LabeledStore{
		{
			label: "memory",
			store: initMemStore(),
		},
		{
			label: "file",
			store: initFileStore(b),
		},
		{
			label: "leveldb",
			store: initLevelDbStore(b),
		},
	}
}

func initMemStore() (store store.Store[uint32, common.Value]) {
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

var sink interface{}

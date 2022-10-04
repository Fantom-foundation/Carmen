package store

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/backend/store/file"
	"github.com/Fantom-foundation/Carmen/go/backend/store/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/store/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"io"
	"testing"
)

func BenchmarkWriteMemStore(b *testing.B) {
	var store = memory.NewStore[uint32, common.Value](common.ValueSerializer{}, common.Value{}, PageSize, Factor)
	benchmarkWriteStore(b, store)
}

func BenchmarkWriteFileStore(b *testing.B) {
	benchmarkWriteStore(b, initFileStore(b))
}

func BenchmarkWriteLevelDbStore(b *testing.B) {
	store, closer := initLevelDbStore(b)
	defer closer.Close()
	benchmarkWriteStore(b, store)
}

func BenchmarkReadMemStore(b *testing.B) {
	var store = memory.NewStore[uint32, common.Value](common.ValueSerializer{}, common.Value{}, PageSize, Factor)
	benchmarkReadStore(b, store)
}

func BenchmarkReadFileStore(b *testing.B) {
	benchmarkReadStore(b, initFileStore(b))
}

func BenchmarkReadLevelDbStore(b *testing.B) {
	store, closer := initLevelDbStore(b)
	defer closer.Close()
	benchmarkReadStore(b, store)
}

func BenchmarkHashMemStore(b *testing.B) {
	var store = memory.NewStore[uint32, common.Value](common.ValueSerializer{}, common.Value{}, PageSize, Factor)
	benchmarkHashStore(b, store)
}

func BenchmarkHashFileStore(b *testing.B) {
	benchmarkHashStore(b, initFileStore(b))
}

func BenchmarkHashLevelDbStore(b *testing.B) {
	store, closer := initLevelDbStore(b)
	defer closer.Close()
	benchmarkHashStore(b, store)
}

func initFileStore(b *testing.B) (idx Store[uint32, common.Value]) {
	store, err := file.NewStore[uint32, common.Value](b.TempDir(), common.ValueSerializer{}, common.Value{}, PageSize, Factor)
	if err != nil {
		b.Fatalf("failed to init file store; %s", err)
	}
	return store
}

func initLevelDbStore(b *testing.B) (store Store[uint32, common.Value], closer io.Closer) {
	db, err := leveldb.OpenFile(b.TempDir(), nil)
	if err != nil {
		b.Errorf("failed to init leveldb; %s", err)
	}
	hashTree := memory.CreateHashTreeFactory(Factor)
	store, err = ldb.NewStore[uint32, common.Value](db, common.ValueKey, common.ValueSerializer{}, common.Identifier32Serializer{}, hashTree, common.Value{}, PageSize)
	if err != nil {
		b.Fatalf("failed to init leveldb store; %s", err)
	}
	return store, db
}

var sink interface{}

func benchmarkWriteStore(b *testing.B, store Store[uint32, common.Value]) {
	for i := 0; i < b.N; i++ {
		// TODO: write at randomly selected index
		value := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		err := store.Set(uint32(i), common.Value{value[0], value[1], value[2], value[3]})
		if err != nil {
			b.Fatalf("failed to set store item; %s", err)
		}
	}
}

func benchmarkReadStore(b *testing.B, store Store[uint32, common.Value]) {
	b.StopTimer() // dont measure initialization
	for i := 0; i < b.N; i++ {
		value := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		err := store.Set(uint32(i), common.Value{value[0], value[1], value[2], value[3]})
		if err != nil {
			b.Fatalf("failed to set store item; %s", err)
		}
	}
	b.StartTimer() // end of initialization

	for i := 0; i < b.N; i++ {
		// TODO: read randomly selected items
		value, err := store.Get(uint32(i))
		if err != nil {
			b.Fatalf("failed to read item from store; %s", err)
		}
		sink = value // prevent compiler to optimize it out
	}
}

func benchmarkHashStore(b *testing.B, store Store[uint32, common.Value]) {
	b.StopTimer() // dont measure initialization
	for i := 0; i < b.N; i++ {
		// TODO: change batches of randomly selected items
		value := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		err := store.Set(uint32(i), common.Value{value[0], value[1], value[2], value[3]})
		if err != nil {
			b.Fatalf("failed to set store item; %s", err)
		}
		b.StartTimer()

		hash, err := store.GetStateHash()
		if err != nil {
			b.Fatalf("failed to hash store; %s", err)
		}
		sink = hash // prevent compiler to optimize it out

		b.StopTimer()
	}
}

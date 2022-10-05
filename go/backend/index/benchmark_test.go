package index

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/backend/index/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/index/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"io"
	"testing"
)

func BenchmarkWriteMemIndex(b *testing.B) {
	var index = memory.NewMemory[common.Key, uint32](common.KeySerializer{})
	benchmarkWriteIndex(b, index)
}

func BenchmarkWriteLevelDbStore(b *testing.B) {
	index, closer := initLevelDbIndex(b)
	defer closer.Close()
	benchmarkWriteIndex(b, index)
}

func BenchmarkReadMemIndex(b *testing.B) {
	var store = memory.NewMemory[common.Key, uint32](common.KeySerializer{})
	benchmarkReadIndex(b, store)
}

func BenchmarkReadLevelDbStore(b *testing.B) {
	index, closer := initLevelDbIndex(b)
	defer closer.Close()
	benchmarkReadIndex(b, index)
}

func BenchmarkHashMemIndex(b *testing.B) {
	var store = memory.NewMemory[common.Key, uint32](common.KeySerializer{})
	benchmarkHashIndex(b, store, 10)
}

func BenchmarkHashLevelDbStore(b *testing.B) {
	index, closer := initLevelDbIndex(b)
	defer closer.Close()
	benchmarkHashIndex(b, index, 10)
}

func initLevelDbIndex(b *testing.B) (idx Index[common.Key, uint32], closer io.Closer) {
	db, err := leveldb.OpenFile(b.TempDir(), nil)
	if err != nil {
		b.Errorf("failed to init leveldb; %s", err)
	}
	index, err := ldb.NewKVIndex[common.Key, uint32](db, common.SlotKey, common.KeySerializer{}, common.Identifier32Serializer{})
	if err != nil {
		b.Fatalf("failed to init leveldb index; %s", err)
	}
	return index, db
}

var sink interface{}

func benchmarkWriteIndex(b *testing.B, idx Index[common.Key, uint32]) {
	for i := 0; i < b.N; i++ {
		key := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		index, err := idx.GetOrAdd(common.Key{key[0], key[1], key[2], key[3]})
		if err != nil {
			b.Fatalf("failed to add item into index; %s", err)
		}
		sink = index // prevent compiler to optimize it out
	}
}

func benchmarkReadIndex(b *testing.B, idx Index[common.Key, uint32]) {
	b.StopTimer() // dont measure initialization
	for i := 0; i < b.N; i++ {
		key := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		_, err := idx.GetOrAdd(common.Key{key[0], key[1], key[2], key[3]})
		if err != nil {
			b.Fatalf("failed to add item into index; %s", err)
		}
	}
	b.StartTimer() // end of initialization

	for i := 0; i < b.N; i++ {
		// TODO: read randomly selected items
		key := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		index, err := idx.GetOrAdd(common.Key{key[0], key[1], key[2], key[3]})
		if err != nil {
			b.Fatalf("failed to add item into index; %s", err)
		}
		sink = index // prevent compiler to optimize it out
	}
}

func benchmarkHashIndex(b *testing.B, idx Index[common.Key, uint32], batchsize uint32) {
	b.StopTimer() // dont measure initialization
	for i := 0; i < b.N; i++ {
		keyPrefix := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
		for ii := uint32(0); ii < batchsize; ii++ {
			// TODO: change randomly selected items
			key := binary.BigEndian.AppendUint32(keyPrefix, ii)
			_, err := idx.GetOrAdd(common.Key{key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7]})
			if err != nil {
				b.Fatalf("failed to add item into index; %s", err)
			}
		}
		b.StartTimer()

		hash, err := idx.GetStateHash()
		if err != nil {
			b.Fatalf("failed to hash index; %s", err)
		}
		sink = hash // prevent compiler to optimize it out

		b.StopTimer()
	}
}

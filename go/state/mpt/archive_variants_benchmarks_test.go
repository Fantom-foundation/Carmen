package mpt

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

const IdUpdateRatio = 10

//
//	These benchmarks test writing many nodes in a storage.
//  It mimics writing keys in sorted order, just once in a configured ratio
//  a random key from the already inserted set is updated.
//  To run the experiment for many inserts, customize the command, for example:
//  > go test -bench=BenchmarkAdd_Nodes_In -benchtime=10000000x
//

func BenchmarkAdd_Nodes_In_FileStock(b *testing.B) {
	encoder := BranchNodeEncoder{}
	directory := b.TempDir()
	stock, err := file.OpenStock[int, BranchNode](encoder, directory)
	if err != nil {
		b.Fatalf("cannot open stock: %s", err)
	}
	defer stock.Close()

	var node BranchNode
	var max int
	for i := 0; i < b.N; i++ {
		var id int
		// Either generate a new ID or randomly get one of the previous ones based on configured ratio
		if max > 0 && i%IdUpdateRatio == 0 {
			id = rand.Intn(max)
		} else {
			if id, err = stock.New(); err != nil {
				b.Fatalf("cannot generate next ID: %s", err)
			}
			max = id
		}
		node.children[0] = NodeId(i) //prevent skip of empty nodes
		if err := stock.Set(id, node); err != nil {
			b.Fatalf("failure from stock: %s", err)
		}
	}
}

func BenchmarkAdd_Nodes_In_LevelDB(b *testing.B) {
	encoder := BranchNodeEncoder{}
	directory := b.TempDir()
	ldb, err := leveldb.OpenFile(directory, nil)
	if err != nil {
		b.Fatalf("cannot open leveldb: %s", err)
	}
	defer ldb.Close()

	var node BranchNode
	var max int

	valueBufferPool := sync.Pool{
		New: func() any {
			return make([]byte, encoder.GetEncodedSize())
		},
	}
	keyBufferPool := sync.Pool{
		New: func() any {
			return make([]byte, 8)
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		var id int
		// Either generate a new ID or randomly get one of the previous ones based on configured ratio
		if max > 0 && i%IdUpdateRatio == 0 {
			id = rand.Intn(max)
		} else {
			id = max
			max++
		}

		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			keyBuffer := keyBufferPool.Get().([]byte)
			valueBuffer := valueBufferPool.Get().([]byte)
			binary.BigEndian.PutUint64(keyBuffer, uint64(id))
			if err := encoder.Store(valueBuffer, &node); err != nil {
				log.Panicf("failure to encode value: %s", err)
			}
			if err := ldb.Put(keyBuffer, valueBuffer, nil); err != nil {
				log.Panicf("failure from stock: %s", err)
			}
			keyBufferPool.Put(keyBuffer)
			valueBufferPool.Put(valueBuffer)
		}(id)
	}

	wg.Wait()
}

func BenchmarkAdd_Nodes_In_LevelDB_Run_Parallel_Batches(b *testing.B) {
	encoder := BranchNodeEncoder{}
	directory := b.TempDir()
	ldb, err := leveldb.OpenFile(directory, nil)
	if err != nil {
		b.Fatalf("cannot open leveldb: %s", err)
	}
	defer ldb.Close()

	var node BranchNode
	var max int

	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := start.Add(int64(b.N))
		keyBuffer := make([]byte, 8)
		valueBuffer := make([]byte, encoder.GetEncodedSize())

		for pb.Next() {
			var id int
			// Either generate a new ID or randomly get one of the previous ones based on configured ratio
			if max > 0 && i%IdUpdateRatio == 0 {
				id = rand.Intn(max)
			} else {
				id = max
				max++
			}

			binary.BigEndian.PutUint64(keyBuffer, uint64(id))
			if err := encoder.Store(valueBuffer, &node); err != nil {
				log.Panicf("failure to encode value: %s", err)
			}
			if err := ldb.Put(keyBuffer, valueBuffer, nil); err != nil {
				log.Panicf("failure from stock: %s", err)
			}
		}
	})

}

package common

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// LevelDB is an interface missing in original LevelDB design.
// It contains methods common for the LevelDB instance and its Transactions.
// It allows for easy switching between transactional and non-transactional accesses.
type LevelDB interface {

	// Get gets the value for the given key. It returns ErrNotFound if the
	// DB does not contain the key.
	//
	// The returned slice is its own copy, it is safe to modify the contents
	// of the returned slice.
	// It is safe to modify the contents of the argument after Get returns.
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)

	// Has returns true if the DB does contain the given key.
	//
	// It is safe to modify the contents of the argument after Has returns.
	Has(key []byte, ro *opt.ReadOptions) (bool, error)

	// NewIterator returns an iterator for the latest snapshot of the
	// underlying DB.
	// The returned iterator is not safe for concurrent use, but it is safe to use
	// multiple iterators concurrently, with each in a dedicated goroutine.
	// It is also safe to use an iterator concurrently with modifying its
	// underlying DB. The resultant key/value pairs are guaranteed to be
	// consistent.
	//
	// Slice allows slicing the iterator to only contains keys in the given
	// range. A nil Range.Start is treated as a key before all keys in the
	// DB. And a nil Range.Limit is treated as a key after all keys in
	// the DB.
	//
	// WARNING: Any slice returned by iterator (e.g. slice returned by calling
	// Iterator.Key() or Iterator.Key() methods), its content should not be modified
	// unless noted otherwise.
	//
	// The iterator must be released after use, by calling Release method.
	//
	// Also read Iterator documentation of the leveldb/iterator package.
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator

	// Put sets the value for the given key. It overwrites any previous value
	// for that key; a DB is not a multi-map.
	// Please note that the transaction is not compacted until committed, so if you
	// write 10 same keys, then those 10 same keys are in the transaction.
	//
	// It is safe to modify the contents of the arguments after Put returns.
	Put(key, value []byte, wo *opt.WriteOptions) error

	// Delete deletes the value for the given key.
	// Please note that the transaction is not compacted until committed, so if you
	// write 10 same keys, then those 10 same keys are in the transaction.
	//
	// It is safe to modify the contents of the arguments after Delete returns.
	Delete(key []byte, wo *opt.WriteOptions) error

	// Write apply the given batch to the DB. The batch records will be applied
	// sequentially. Write might be used concurrently, when used concurrently and
	// batch is small enough, write will try to merge the batches. Set NoWriteMerge
	// option to true to disable write merge.
	//
	// It is safe to modify the contents of the arguments after Write returns but
	// not before. Write will not modify content of the batch.
	Write(batch *leveldb.Batch, wo *opt.WriteOptions) error

	// GetSnapshot returns a latest snapshot of the underlying DB. A snapshot
	// is a frozen snapshot of a DB state at a particular point in time. The
	// content of snapshot are guaranteed to be consistent.
	//
	// The snapshot must be released after use, by calling Release method.
	GetSnapshot() (*leveldb.Snapshot, error)

	MemoryFootprintProvider
}

// LevelDBReader is an interface missing in original LevelDB design.
// It contains methods common for the LevelDB instance and its Snapshots.
type LevelDBReader interface {
	// Get gets the value for the given key. It returns ErrNotFound if the
	// DB does not contain the key.
	//
	// The returned slice is its own copy, it is safe to modify the contents
	// of the returned slice.
	// It is safe to modify the contents of the argument after Get returns.
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)

	// Has returns true if the DB does contain the given key.
	//
	// It is safe to modify the contents of the argument after Has returns.
	Has(key []byte, ro *opt.ReadOptions) (bool, error)

	// NewIterator returns an iterator for the latest snapshot of the
	// underlying DB.
	// The returned iterator is not safe for concurrent use, but it is safe to use
	// multiple iterators concurrently, with each in a dedicated goroutine.
	// It is also safe to use an iterator concurrently with modifying its
	// underlying DB. The resultant key/value pairs are guaranteed to be
	// consistent.
	//
	// Slice allows slicing the iterator to only contains keys in the given
	// range. A nil Range.Start is treated as a key before all keys in the
	// DB. And a nil Range.Limit is treated as a key after all keys in
	// the DB.
	//
	// WARNING: Any slice returned by iterator (e.g. slice returned by calling
	// Iterator.Key() or Iterator.Key() methods), its content should not be modified
	// unless noted otherwise.
	//
	// The iterator must be released after use, by calling Release method.
	//
	// Also read Iterator documentation of the leveldb/iterator package.
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
}

// OpenLevelDb opens the LevelDB connection and provides it wrapped in memory-footprint-reporting object.
func OpenLevelDb(path string, options *opt.Options) (wrapped *LevelDbMemoryFootprintWrapper, err error) {
	ldb, err := leveldb.OpenFile(path, options)
	if err != nil {
		return nil, err
	}
	mf := NewMemoryFootprint(0)
	mf.AddChild("writeBuffer", NewMemoryFootprint(uintptr(options.GetWriteBuffer())))
	return &LevelDbMemoryFootprintWrapper{ldb, mf}, nil
}

// LevelDbMemoryFootprintWrapper is a LevelDB wrapper adding a memory footprint providing method.
type LevelDbMemoryFootprintWrapper struct {
	*leveldb.DB
	mf *MemoryFootprint
}

func (wrapper *LevelDbMemoryFootprintWrapper) GetMemoryFootprint() *MemoryFootprint {
	var ldbStats leveldb.DBStats
	err := wrapper.DB.Stats(&ldbStats)
	if err != nil {
		panic(fmt.Errorf("failed to get LevelDB Stats; %s", err))
	}
	wrapper.mf.AddChild("blockCache", NewMemoryFootprint(uintptr(ldbStats.BlockCacheSize)))
	return wrapper.mf
}

func (wrapper *LevelDbMemoryFootprintWrapper) OpenTransaction() (*LevelDbTransactionMemoryFootprintWrapper, error) {
	tx, err := wrapper.DB.OpenTransaction()
	if err != nil {
		return nil, err
	}
	return &LevelDbTransactionMemoryFootprintWrapper{tx, wrapper.mf}, nil
}

// LevelDbTransactionMemoryFootprintWrapper is a LevelDB transaction wrapper adding a memory footprint method.
type LevelDbTransactionMemoryFootprintWrapper struct {
	*leveldb.Transaction
	mf *MemoryFootprint
}

func (wrapper *LevelDbTransactionMemoryFootprintWrapper) GetSnapshot() (*leveldb.Snapshot, error) {
	return nil, fmt.Errorf("unable to get snapshot from a transaction")
}

func (wrapper *LevelDbTransactionMemoryFootprintWrapper) GetMemoryFootprint() *MemoryFootprint {
	return wrapper.mf
}

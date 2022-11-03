package common

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// LevelDB is an interface missing in original LevelDB design.
// It contains methods common for transactional and non-transactional LevelDB instances
// allowing for transparent switching between instances
type LevelDB interface {

	// Get gets the value for the given key. It returns ErrNotFound if the
	// DB does not contains the key.
	//
	// The returned slice is its own copy, it is safe to modify the contents
	// of the returned slice.
	// It is safe to modify the contents of the argument after Get returns.
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)

	// Has returns true if the DB does contains the given key.
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
	// WARNING: Any slice returned by interator (e.g. slice returned by calling
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
	// writes 10 same keys, then those 10 same keys are in the transaction.
	//
	// It is safe to modify the contents of the arguments after Put returns.
	Put(key, value []byte, wo *opt.WriteOptions) error

	// Delete deletes the value for the given key.
	// Please note that the transaction is not compacted until committed, so if you
	// writes 10 same keys, then those 10 same keys are in the transaction.
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
}

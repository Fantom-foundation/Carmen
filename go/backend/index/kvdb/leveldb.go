package kvdb

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
)

const (
	HashKey      = "hash"
	LastIndexKey = "last"
)

type Persistent[K comparable] struct {
	db         *leveldb.DB
	serializer common.Serializer[K]
	hashIndex  *index.HashIndex[K]
	lastIndex  uint32
}

// New creates a new instance of the index backed by a persisted database
func New[K comparable](path string, serializer common.Serializer[K]) (*Persistent[K], error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	// read the last hash from the database
	hash, err := db.Get([]byte(HashKey), nil)
	if err != nil && err != errors.ErrNotFound {
		return nil, err
	}
	// read the last index from the database
	last, err := db.Get([]byte(LastIndexKey), nil)
	if err != nil && err != errors.ErrNotFound {
		return nil, err
	}

	memory := Persistent[K]{
		db:         db,
		serializer: serializer,
		hashIndex:  index.InitHashIndex[K](hash, serializer),
		lastIndex:  binary.LittleEndian.Uint32(last),
	}

	return &memory, nil
}

func (m *Persistent[K]) GetOrAdd(key K) (uint32, error) {
	val, err := m.db.Get(m.serializer.ToBytes(key), nil)
	var idx uint32
	if err != nil {
		// if the error is actually a non-existing key, we assign a new index
		if err == errors.ErrNotFound {
			m.lastIndex = m.lastIndex + 1
			newVal := make([]byte, 4)
			binary.LittleEndian.PutUint32(newVal, m.lastIndex)
			err = m.db.Put(m.serializer.ToBytes(key), newVal, nil)
			if err != nil {
				return 0, err
			}
			m.hashIndex.AddKey(key)
			idx = m.lastIndex
		} else {
			return 0, err
		}
	} else {
		idx = binary.LittleEndian.Uint32(val)
	}

	return idx, nil
}

func (m *Persistent[K]) Contains(key K) bool {
	exists, _ := m.db.Has(m.serializer.ToBytes(key), nil)
	return exists
}

func (m *Persistent[K]) GetStateHash() (common.Hash, error) {
	return m.hashIndex.Commit()
}

func (m *Persistent[K]) Close() error {
	// store the last index and hash before closing
	newIdx := make([]byte, 4)
	binary.LittleEndian.PutUint32(newIdx, m.lastIndex)
	hash, err := m.hashIndex.Commit()
	if err != nil {
		return err
	}

	batch := new(leveldb.Batch)
	batch.Put([]byte(LastIndexKey), newIdx)
	batch.Put([]byte(HashKey), hash.Bytes())
	err = m.db.Write(batch, nil)
	if err != nil {
		return err
	}
	return m.db.Close()
}

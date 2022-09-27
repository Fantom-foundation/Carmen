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
func New[K comparable](path string, serializer common.Serializer[K]) (p *Persistent[K], err error) {
	var db *leveldb.DB
	if db, err = leveldb.OpenFile(path, nil); err != nil {
		return
	}

	// read the last hash from the database
	var hash []byte
	if hash, err = db.Get([]byte(HashKey), nil); err != nil {
		if err == errors.ErrNotFound {
			hash = []byte{}
		} else {
			return
		}
	}

	// read the last index from the database
	var last []byte
	if last, err = db.Get([]byte(LastIndexKey), nil); err != nil {
		if err == errors.ErrNotFound {
			last = make([]byte, 4)
		} else {
			return
		}
	}

	p = &Persistent[K]{
		db:         db,
		serializer: serializer,
		hashIndex:  index.InitHashIndex[K](hash, serializer),
		lastIndex:  binary.LittleEndian.Uint32(last),
	}

	return p, nil
}

func (m *Persistent[K]) GetOrAdd(key K) (idx uint32, err error) {
	var val []byte
	if val, err = m.db.Get(m.serializer.ToBytes(key), nil); err != nil {
		// if the error is actually a non-existing key, we assign a new index
		if err == errors.ErrNotFound {

			// map the input key to the next level as well as storing the next index
			idx = m.lastIndex
			idxArr := make([]byte, 4)
			binary.LittleEndian.PutUint32(idxArr, m.lastIndex)

			m.lastIndex = m.lastIndex + 1
			nextIdxArr := make([]byte, 4)
			binary.LittleEndian.PutUint32(nextIdxArr, m.lastIndex)

			batch := new(leveldb.Batch)
			batch.Put([]byte(LastIndexKey), nextIdxArr)
			batch.Put(m.serializer.ToBytes(key), idxArr)
			if err = m.db.Write(batch, nil); err != nil {
				return
			}
			m.hashIndex.AddKey(key)
		} else {
			return
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

func (m *Persistent[K]) GetStateHash() (hash common.Hash, err error) {
	// compute and persist new hash
	if hash, err = m.hashIndex.Commit(); err != nil {
		return
	}

	if err = m.db.Put([]byte(HashKey), hash.Bytes(), nil); err != nil {
		return
	}

	return
}

func (m *Persistent[K]) Close() (err error) {
	// commit the state
	if _, err = m.GetStateHash(); err == nil {
		err = m.db.Close()
	}
	return
}

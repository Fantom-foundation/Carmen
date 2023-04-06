package memory

import (
	"fmt"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/index/indexhash"
	"github.com/Fantom-foundation/Carmen/go/common"
)

const initCapacity = 10_000

// Index is an in-memory implementation of index.Index.
type Index[K comparable, I common.Identifier] struct {
	data          map[K]I
	list          []K
	hashes        []common.Hash
	keySerializer common.Serializer[K]
	hashIndex     *indexhash.IndexHash[K]
}

// NewIndex constructs a new Index instance.
func NewIndex[K comparable, I common.Identifier](serializer common.Serializer[K]) *Index[K, I] {
	memory := Index[K, I]{
		data:          make(map[K]I, initCapacity),
		list:          make([]K, 0, initCapacity),
		hashes:        make([]common.Hash, 0, initCapacity/4096),
		keySerializer: serializer,
		hashIndex:     indexhash.NewIndexHash[K](serializer),
	}
	return &memory
}

// Size returns the number of registered keys.
func (m *Index[K, I]) Size() I {
	return I(len(m.data))
}

// GetOrAdd returns an index mapping for the key, or creates the new index.
func (m *Index[K, I]) GetOrAdd(key K) (I, error) {
	idx, exists := m.data[key]
	if !exists {
		size := len(m.data)

		// commit hash for the snapshot block height window
		if size%index.GetKeysPerPart(m.keySerializer) == 0 {
			hash, err := m.GetStateHash()
			if err != nil {
				return idx, err
			}
			m.hashes = append(m.hashes, hash)
		}

		idx = I(size)
		m.data[key] = idx
		m.hashIndex.AddKey(key)
		m.list = append(m.list, key)

	}
	return idx, nil
}

// Get returns an index mapping for the key, returns index.ErrNotFound if not exists.
func (m *Index[K, I]) Get(key K) (I, error) {
	idx, exists := m.data[key]
	if !exists {
		return idx, index.ErrNotFound
	}
	return idx, nil
}

// Contains returns whether the key exists in the mapping or not.
func (m *Index[K, I]) Contains(key K) bool {
	_, exists := m.data[key]
	return exists
}

// GetStateHash returns the index hash.
func (m *Index[K, I]) GetStateHash() (common.Hash, error) {
	return m.hashIndex.Commit()
}

// Flush does nothing.
func (m *Index[K, I]) Flush() error {
	return nil
}

// Close closes the storage and clean-ups all possible dirty values.
func (m *Index[K, I]) Close() error {
	return nil
}

func (m *Index[K, I]) GetProof() (backend.Proof, error) {
	hash, err := m.GetStateHash()
	if err != nil {
		return nil, err
	}

	return index.NewIndexProof(common.Hash{}, hash), nil
}

func (m *Index[K, I]) CreateSnapshot() (backend.Snapshot, error) {
	hash, err := m.GetStateHash()
	if err != nil {
		return nil, err
	}

	return index.CreateIndexSnapshotFromIndex[K](
		m.keySerializer,
		hash,
		len(m.list),
		&indexSnapshotSource[K, I]{m, len(m.list), hash}), nil
}

func (m *Index[K, I]) Restore(data backend.SnapshotData) error {
	snapshot, err := index.CreateIndexSnapshotFromData(m.keySerializer, data)
	if err != nil {
		return err
	}

	// Reset and re-initialize the index.
	m.hashIndex.Clear()
	m.hashes = m.hashes[0:0]
	m.list = m.list[0:0]
	m.data = make(map[K]I, initCapacity)

	for j := 0; j < snapshot.GetNumParts(); j++ {
		part, err := snapshot.GetPart(j)
		if err != nil {
			return err
		}
		indexPart, ok := part.(*index.IndexPart[K])
		if !ok {
			return fmt.Errorf("invalid part format encountered")
		}
		for _, key := range indexPart.GetKeys() {
			if _, err := m.GetOrAdd(key); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *Index[K, I]) GetSnapshotVerifier([]byte) (backend.SnapshotVerifier, error) {
	return index.CreateIndexSnapshotVerifier(m.keySerializer), nil
}

type indexSnapshotSource[K comparable, I common.Identifier] struct {
	index   *Index[K, I] // The index this snapshot is based on.
	numKeys int          // The number of keys at the time the snapshot was created.
	hash    common.Hash  // The hash at the time the snapshot was created.
}

func (m *indexSnapshotSource[K, I]) GetHash(keyHeight int) (common.Hash, error) {
	keysPerPart := index.GetKeysPerPart(m.index.keySerializer)

	if keyHeight == m.numKeys {
		return m.hash, nil
	}
	if keyHeight > m.numKeys {
		return common.Hash{}, fmt.Errorf("invalid key height, not covered by snapshot")
	}

	if keyHeight%keysPerPart != 0 {
		return common.Hash{}, fmt.Errorf("invalid key height, only supported at part boundaries")
	}
	return m.index.hashes[keyHeight/keysPerPart], nil
}

func (m *indexSnapshotSource[K, I]) GetKeys(from, to int) ([]K, error) {
	return m.index.list[from:to], nil
}

func (m *indexSnapshotSource[K, I]) Release() error {
	// nothing to do
	return nil
}

// GetMemoryFootprint provides the size of the index in memory in bytes.
func (m *Index[K, I]) GetMemoryFootprint() *common.MemoryFootprint {
	dataMapItemSize := unsafe.Sizeof(struct {
		key K
		idx I
	}{})
	var k K
	var hash common.Hash
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m) + uintptr(len(m.data))*dataMapItemSize)
	mf.AddChild("hashIndex", m.hashIndex.GetMemoryFootprint())
	mf.AddChild("list", common.NewMemoryFootprint(uintptr(len(m.list))*unsafe.Sizeof(k)))
	mf.AddChild("hashes", common.NewMemoryFootprint(uintptr(len(m.hashes))*unsafe.Sizeof(hash)))
	mf.SetNote(fmt.Sprintf("(items: %d)", len(m.data)))
	return mf
}

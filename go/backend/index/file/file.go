package file

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/array"
	"github.com/Fantom-foundation/Carmen/go/backend/array/pagedarray"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/index/indexhash"
	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
)

const (
	// Customize initial size of buckets and the page pool size together!
	// The page pool size should be equals or greater to the initial size of buckets to prevent many page evictions
	// for keys falling into sparse buckets
	// A smaller number of initial buckets causes many splits, but small initial file. A higher number causes the opposite.
	defaultNumBuckets = 1 << 15
	pagePoolSize      = 1 << 17

	uint32ByteSize = 4

	bulkInsertKeysNum = 1 << 25 // the number of keys that are accumulated while snapshot restoration before they are actually inserted. Approx 1GB, depends on key size.
)

// Index is a file implementation of index.Index. It uses common.LinearHashMap to store key-identifier pairs.
// The pairs are stored using the linear-hash, a Hash Map data structure that is initiated with a number of collision buckets.
// When the buckets overflow, one extra bucket is added and keys from another bucket are split between the two buckets.
// The pairs are also divided into a set of fixed-size pagepool.Page that are stored and loaded via pagepool.PagePool
// from/to the disk.  All the keys that do not fit in the memory pagepool.PagePool are stored by pagepool.PageStorage on disk and loaded when needed.
// The least recently used policy is used to decide which pages to hold in the PagePool, i.e. the less frequently used
// pages are evicted, while every use of a page makes it more frequently used with a lower chance to get evicted.
// The pages are 4kB for an optimal IO when pages are stored/loaded from/to the disk.
type Index[K comparable, I common.Identifier] struct {
	table           *LinearHashMap[K, I]
	keys            array.Array[I, K]           // map of indexes to keys for snapshot
	hashes          array.Array[I, common.Hash] // map of indexes to hashes for snapshot
	keySerializer   common.Serializer[K]
	indexSerializer common.Serializer[I]
	hashIndex       *indexhash.IndexHash[K]
	pageStore       *TwoFilesPageStorage                         // pagestore for the main hash table
	pagePool        *pagepool.PagePool[PageId, *IndexPage[K, I]] // pagepool for the main hash table
	path            string

	maxIndex I // max index to fast compute and persists nex item
}

// NewIndex constructs a new Index instance.
func NewIndex[K comparable, I common.Identifier](
	path string,
	keySerializer common.Serializer[K],
	indexSerializer common.Serializer[I],
	hasher common.Hasher[K],
	comparator common.Comparator[K]) (inst *Index[K, I], err error) {

	return NewParamIndex[K, I](path, defaultNumBuckets, pagePoolSize, keySerializer, indexSerializer, hasher, comparator)
}

// NewParamIndex constructs a new Index instance, allowing to configure the number of linear hash buckets
// and the size of the page pool .
func NewParamIndex[K comparable, I common.Identifier](
	path string,
	defaultNumBuckets, pagePoolSize int,
	keySerializer common.Serializer[K],
	indexSerializer common.Serializer[I],
	hasher common.Hasher[K],
	comparator common.Comparator[K]) (inst *Index[K, I], err error) {

	// --- main table initialization ---
	hash, numBuckets, size, lastIndex, err := readMetadata[I](path, indexSerializer)
	if err != nil {
		return
	}

	if numBuckets == 0 {
		// number not in metadata
		numBuckets = defaultNumBuckets // 32K * 4kb -> 128MB.
	}
	// Do not customise, unless different size of page, etc. is needed
	// 4kB is the right fit for disk I/O
	pageSize := common.PageSize // 4kB
	pageStorage, err := NewTwoFilesPageStorage(path, pageSize)
	if err != nil {
		return
	}
	pageItems := numKeysPage(pageSize, keySerializer, indexSerializer)
	pageFactory := PageFactory(pageSize, keySerializer, indexSerializer, comparator)
	pagePool := pagepool.NewPagePool[PageId, *IndexPage[K, I]](pagePoolSize, pageStorage, pageFactory)

	// --- Reverse table initialisation ---
	keys := path + "/keys"
	if err := os.MkdirAll(keys, 0700); err != nil {
		return nil, err
	}
	hashes, err := pagedarray.NewArray[I, K](keys, keySerializer, common.PageSize, pagePoolSize)
	if err != nil {
		return
	}

	hashTablePath := path + "/hashes"
	if err := os.MkdirAll(hashTablePath, 0700); err != nil {
		return nil, err
	}
	hashesStore, err := pagedarray.NewArray[I, common.Hash](hashTablePath, common.HashSerializer{}, common.PageSize, pagePoolSize)

	inst = &Index[K, I]{
		table:           NewLinearHashMap[K, I](pageItems, numBuckets, size, pagePool, hasher, comparator),
		keys:            hashes,
		hashes:          hashesStore,
		keySerializer:   keySerializer,
		indexSerializer: indexSerializer,
		hashIndex:       indexhash.InitIndexHash[K](hash, keySerializer),
		pageStore:       pageStorage,
		pagePool:        pagePool,
		path:            path,
		maxIndex:        lastIndex,
	}

	return
}

// Size returns the number of registered keys.
func (m *Index[K, I]) Size() I {
	return m.maxIndex
}

// GetOrAdd returns an index mapping for the key, or creates the new index.
func (m *Index[K, I]) GetOrAdd(key K) (val I, err error) {
	val, exists, err := m.table.GetOrAdd(key, m.maxIndex)
	if err != nil {
		return
	}
	if !exists {
		if err := m.add(key); err != nil {
			return val, err
		}
	}

	return val, nil
}

// keyTuple is a helper structure that contains the key, its bucket and the index where the key belongs to.
// It is used for sorting the keys before doing their bulk insert
type keyTuple[K comparable, I common.Identifier] struct {
	key    K
	bucket uint
	index  I
}

// bulkInsert inserts many keys. It sorts the keys by their hash bucked ID first, and add them in the index next.
// It should reduce page misses when adding keys into the backend linear hash map.
// This method does not check existence of the input keys, it expects they do not exist
func (m *Index[K, I]) bulkInsert(keys []K) error {
	tuples := make([]keyTuple[K, I], 0, len(keys))
	for idx, key := range keys {
		tuples = append(tuples, keyTuple[K, I]{key, m.table.GetBucketId(&key), m.maxIndex + I(idx)})
	}

	// store values for snapshot using the original order
	for _, key := range keys {
		if err := m.add(key); err != nil {
			return err
		}
	}

	// sort by bucketIds before inserting into LinearHash for better performance
	sort.Slice(tuples, func(i, j int) bool {
		return tuples[i].bucket < tuples[j].bucket
	})

	// insert keys sorted by bucketIds
	for _, tuple := range tuples {
		if err := m.table.Put(tuple.key, tuple.index); err != nil {
			return err
		}
	}

	return nil
}

func (m *Index[K, I]) add(key K) error {
	val := m.maxIndex
	m.maxIndex += 1 // increment to next index

	// commit hash for the snapshot block height window
	keysPerPart := I(index.GetKeysPerPart(m.keySerializer))
	if val%keysPerPart == 0 {
		hash, err := m.GetStateHash()
		if err != nil {
			return err
		}
		if err := m.hashes.Set(val/keysPerPart, hash); err != nil {
			return err
		}
	}

	if err := m.keys.Set(val, key); err != nil {
		return err
	}

	m.hashIndex.AddKey(key)

	return nil
}

// Get returns an index mapping for the key, returns index.ErrNotFound if not exists.
func (m *Index[K, I]) Get(key K) (val I, err error) {
	val, exists, err := m.table.Get(key)
	if err != nil {
		return
	}

	if !exists {
		err = index.ErrNotFound
	}
	return
}

// Contains returns whether the key exists in the mapping or not.
func (m *Index[K, I]) Contains(key K) (exists bool) {
	_, exists, err := m.table.Get(key)
	if err != nil {
		return false
	}
	return
}

// GetStateHash returns the index hash.
func (m *Index[K, I]) GetStateHash() (common.Hash, error) {
	return m.hashIndex.Commit()
}

func (m *Index[K, I]) Flush() error {
	// flush dependencies
	if err := m.pagePool.Flush(); err != nil {
		return err
	}
	if err := m.pageStore.Flush(); err != nil {
		return err
	}
	if err := m.keys.Flush(); err != nil {
		return err
	}
	if err := m.hashes.Flush(); err != nil {
		return err
	}

	// store metadata
	if err := m.writeMetadata(); err != nil {
		return err
	}
	return nil
}

// Close closes the storage and clean-ups all possible dirty values
func (m *Index[K, I]) Close() error {
	if err := m.Flush(); err != nil {
		return err
	}
	if err := m.pagePool.Close(); err != nil {
		return err
	}
	if err := m.pageStore.Close(); err != nil {
		return err
	}
	if err := m.keys.Close(); err != nil {
		return err
	}
	if err := m.hashes.Close(); err != nil {
		return err
	}

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
		m.table.Size(),
		&indexSnapshotSource[K, I]{m, m.table.Size(), hash}), nil
}

func (m *Index[K, I]) Restore(data backend.SnapshotData) error {
	snapshot, err := index.CreateIndexSnapshotFromData(m.keySerializer, data)
	if err != nil {
		return err
	}

	// Reset and re-initialize the index.
	if err := m.table.Clear(); err != nil {
		return err
	}

	m.hashIndex.Clear()
	m.maxIndex = 0

	keysBuffer := make([]K, 0, bulkInsertKeysNum)
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
			keysBuffer = append(keysBuffer, key)
			// flush when needed
			if len(keysBuffer) == bulkInsertKeysNum {
				if err := m.bulkInsert(keysBuffer); err != nil {
					return err
				}
				keysBuffer = keysBuffer[0:0]
			}
		}
	}

	// flush remaining keys
	if len(keysBuffer) > 0 {
		if err := m.bulkInsert(keysBuffer); err != nil {
			return err
		}
	}

	return nil
}

func (m *Index[K, I]) GetSnapshotVerifier([]byte) (backend.SnapshotVerifier, error) {
	return index.CreateIndexSnapshotVerifier[K](m.keySerializer), nil
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

	hash, err := m.index.hashes.Get(I(keyHeight / keysPerPart))
	if err != nil {
		return hash, err
	}

	return hash, nil
}

func (m *indexSnapshotSource[K, I]) GetKeys(from, to int) ([]K, error) {
	keys := make([]K, 0, to-from)
	for i := from; i < to; i++ {
		key, err := m.index.keys.Get(I(i))
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func (m *indexSnapshotSource[K, I]) Release() error {
	// nothing to do
	return nil
}

func (m *Index[K, I]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*m)

	memoryFootprint := common.NewMemoryFootprint(selfSize)
	memoryFootprint.AddChild("hashIndex", m.hashIndex.GetMemoryFootprint())
	memoryFootprint.AddChild("linearHash", m.table.GetMemoryFootprint())
	memoryFootprint.AddChild("pagePool", m.pagePool.GetMemoryFootprint())
	memoryFootprint.AddChild("pageStore", m.pageStore.GetMemoryFootprint())
	memoryFootprint.AddChild("keys", m.keys.GetMemoryFootprint())
	memoryFootprint.AddChild("hashes", m.hashes.GetMemoryFootprint())
	memoryFootprint.SetNote(fmt.Sprintf("(items: %d)", m.maxIndex))
	return memoryFootprint
}

func readMetadata[I common.Identifier](path string, indexSerializer common.Serializer[I]) (hash common.Hash, numBuckets, records int, lastIndex I, err error) {
	metadataFile, err := os.OpenFile(path+"/metadata.dat", os.O_RDONLY|os.O_CREATE, 0600)
	if err != nil {
		return
	}
	defer metadataFile.Close()

	// read metadata
	size := len(hash) + indexSerializer.Size() + 2*uint32ByteSize
	data := make([]byte, size)
	_, err = metadataFile.Read(data)
	if err == nil {
		hash = *(*common.Hash)(data[0:32])
		numBuckets = int(binary.BigEndian.Uint32(data[32:36]))
		records = int(binary.BigEndian.Uint32(data[36:40]))
		lastIndex = indexSerializer.FromBytes(data[40:44])
	}

	if err == io.EOF {
		err = nil
	}

	return
}

func (m *Index[K, I]) writeMetadata() (err error) {
	metadataFile, err := os.OpenFile(m.path+"/metadata.dat", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return
	}
	defer metadataFile.Close()

	// computed new root
	hash, err := m.GetStateHash()
	if err != nil {
		return
	}

	// total size is: 32B size of bash + size of index + 2 times uint32
	size := len(hash) + m.indexSerializer.Size() + 2*uint32ByteSize
	metadata := make([]byte, 0, size)

	metadata = append(metadata, hash.ToBytes()...)
	metadata = binary.BigEndian.AppendUint32(metadata, uint32(m.table.GetNumBuckets()))
	metadata = binary.BigEndian.AppendUint32(metadata, uint32(m.table.Size()))
	metadata = append(metadata, m.indexSerializer.ToBytes(m.maxIndex)...)

	_, err = metadataFile.Write(metadata)

	return
}

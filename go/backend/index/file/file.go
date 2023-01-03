package file

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/index/indexhash"
	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"os"
	"unsafe"
)

const (
	// Customize initial size of buckets and the page pool size together!
	// The page pool size should be equals or greater to the initial size of buckets to prevent many page evictions
	// for keys falling into sparse buckets
	// A smaller number of initial buckets causes many splits, but small initial file. A higher number causes the opposite.
	defaultNumBuckets = 1 << 15
	pagePoolSize      = 1 << 17
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
	table           *pagepool.LinearHashMap[K, I]
	keySerializer   common.Serializer[K]
	indexSerializer common.Serializer[I]
	hashIndex       *indexhash.IndexHash[K]
	pageStore       *pagepool.TwoFilesPageStorage
	pagePool        *pagepool.PagePool[*pagepool.KVPage[K, I]]
	comparator      common.Comparator[K]
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

	hash, numBuckets, lastBucket, lastOverflow, lastIndex, freeIds, err := readMetadata[I](path, indexSerializer)
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
	pageStorage, err := pagepool.NewTwoFilesPageStorage(path, pageSize, lastBucket, lastOverflow)
	if err != nil {
		return
	}

	pageItems := pagepool.NumKeysPage(pageSize, keySerializer, indexSerializer)
	pageFactory := pagepool.KVPageFactory(pageSize, keySerializer, indexSerializer, comparator)
	pagePool := pagepool.NewPagePool[*pagepool.KVPage[K, I]](pagePoolSize, freeIds, pageStorage, pageFactory)

	inst = &Index[K, I]{
		table:           pagepool.NewLinearHashMap[K, I](pageItems, numBuckets, pagePool, hasher, comparator),
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

// GetOrAdd returns an index mapping for the key, or creates the new index
func (m *Index[K, I]) GetOrAdd(key K) (val I, err error) {
	val, exists, err := m.table.GetOrAdd(key, m.maxIndex)
	if err != nil {
		return
	}
	if !exists {
		val = m.maxIndex
		m.maxIndex += 1 // increment to next index
		m.hashIndex.AddKey(key)
	}
	return
}

// Get returns an index mapping for the key, returns index.ErrNotFound if not exists
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

	// store metadata
	if err := m.writeMetadata(); err != nil {
		return err
	}

	return nil
}

// Close closes the storage and clean-ups all possible dirty values
func (m *Index[K, I]) Close() (err error) {
	flushErr := m.Flush()
	poolErr := m.pagePool.Close()
	closeErr := m.pageStore.Close()

	if flushErr != nil || closeErr != nil || poolErr != nil {
		err = fmt.Errorf("close error: Flush: %s, PagePool Close: %s, PageStore Close: %s", flushErr, poolErr, closeErr)
	}

	return
}

func (m *Index[K, I]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*m)

	memoryFootprint := common.NewMemoryFootprint(selfSize)
	memoryFootprint.AddChild("hashIndex", m.hashIndex.GetMemoryFootprint())
	memoryFootprint.AddChild("linearHash", m.table.GetMemoryFootprint())
	memoryFootprint.AddChild("pagePool", m.pagePool.GetMemoryFootprint())
	memoryFootprint.AddChild("pageStore", m.pageStore.GetMemoryFootprint())
	memoryFootprint.SetNote(fmt.Sprintf("(items: %d)", m.maxIndex))
	return memoryFootprint
}

func readMetadata[I common.Identifier](path string, indexSerializer common.Serializer[I]) (hash common.Hash, numBuckets, lastBucket, lastOverflow int, lastIndex I, freeIds []int, err error) {
	metadataFile, err := os.OpenFile(path+"/metadata.dat", os.O_RDONLY|os.O_CREATE, 0600)
	if err != nil {
		return
	}
	defer metadataFile.Close()

	// read metadata
	size := len(hash) + indexSerializer.Size() + 3*4
	data := make([]byte, size)
	_, err = metadataFile.Read(data)
	if err == nil {
		hash = *(*common.Hash)(data[0:32])
		numBuckets = int(binary.BigEndian.Uint32(data[32:36]))
		lastBucket = int(binary.BigEndian.Uint32(data[36:40]))
		lastOverflow = int(binary.BigEndian.Uint32(data[40:44]))
		lastIndex = indexSerializer.FromBytes(data[44:48])
	}

	// read metadata - free IDs
	for err == nil {
		freeIdBytes := make([]byte, 4)
		_, err = metadataFile.Read(freeIdBytes)
		if err == nil {
			freeIds = append(freeIds, int(binary.BigEndian.Uint32(freeIdBytes)))
		}
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

	size := len(hash) + m.indexSerializer.Size() + (3+len(m.pagePool.GetFreeIds()))*4
	metadata := make([]byte, 0, size)

	metadata = append(metadata, hash.ToBytes()...)
	metadata = binary.BigEndian.AppendUint32(metadata, uint32(m.table.GetBuckets()))
	metadata = binary.BigEndian.AppendUint32(metadata, uint32(m.pageStore.GetLastId().Bucket()))
	metadata = binary.BigEndian.AppendUint32(metadata, uint32(m.pageStore.GetLastId().Overflow()))
	metadata = append(metadata, m.indexSerializer.ToBytes(m.maxIndex)...)

	for _, freeId := range m.pagePool.GetFreeIds() {
		metadata = binary.BigEndian.AppendUint32(metadata, uint32(freeId))
	}

	_, err = metadataFile.Write(metadata)

	return
}

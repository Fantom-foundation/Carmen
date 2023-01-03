package file

import (
	"encoding/binary"
	"fmt"
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

// MultiMap is a file-based multimap.MultiMap implementation
type MultiMap[K comparable, V comparable] struct {
	table     *pagepool.LinearHashMultiMap[K, V]
	pageStore *pagepool.TwoFilesPageStorage
	pagePool  *pagepool.PagePool[*pagepool.KVPage[K, V]]

	path string
}

// NewMultiMap crease a new instance of a MultiMap, which is backed by a file storage.
// Key-value pairs in this MultiMap are stored in LinearHash, where every collision bucket
// is furthermore divided into a set of Page. The Page is stored/loaded on the disk, and maintained
// via a PagePool serving as a memory cache of active pages.
// The pages that do  not fit in-memory are stored and potentially loaded later.
func NewMultiMap[K comparable, V comparable](
	path string,
	keySerializer common.Serializer[K],
	indexSerializer common.Serializer[V],
	hasher common.Hasher[K],
	comparator common.Comparator[K]) (*MultiMap[K, V], error) {

	numBuckets, lastBucket, lastOverflow, freeIds, err := readMetadata(path)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	pageItems := pagepool.NumKeysPage(pageSize, keySerializer, indexSerializer)
	pageFactory := pagepool.KVPageFactory(pageSize, keySerializer, indexSerializer, comparator)
	pagePool := pagepool.NewPagePool[*pagepool.KVPage[K, V]](pagePoolSize, freeIds, pageStorage, pageFactory)

	return &MultiMap[K, V]{
		table:     pagepool.NewLinearHashMultiMap[K, V](pageItems, numBuckets, pagePool, hasher, comparator),
		pageStore: pageStorage,
		pagePool:  pagePool,
		path:      path,
	}, nil
}

// Add adds the given key/value pair.
func (m *MultiMap[K, V]) Add(key K, value V) error {
	return m.table.Add(key, value)
}

// Remove removes a single key/value entry.
func (m *MultiMap[K, V]) Remove(key K, value V) error {
	_, err := m.table.Remove(key, value)
	return err
}

// RemoveAll removes all entries with the given key.
func (m *MultiMap[K, V]) RemoveAll(key K) error {
	return m.table.RemoveAll(key)
}

// GetAll provides all values associated with the given key.
func (m *MultiMap[K, V]) GetAll(key K) ([]V, error) {
	return m.table.GetAll(key)
}

// Flush the store
func (m *MultiMap[K, V]) Flush() error {
	// flush dependencies
	if err := m.pagePool.Flush(); err != nil {
		return err
	}
	if err := m.pageStore.Flush(); err != nil {
		return err
	}

	// store metadata
	if err := writeMetadata(m.path, m.table, m.pagePool, m.pageStore); err != nil {
		return err
	}

	return nil
}

// Close the store
func (m *MultiMap[K, V]) Close() error {
	flushErr := m.Flush()
	poolErr := m.pagePool.Close()
	closeErr := m.pageStore.Close()

	if flushErr != nil || closeErr != nil || poolErr != nil {
		return fmt.Errorf("close error: Flush: %s, PagePool Close: %s, PageStore Close: %s", flushErr, poolErr, closeErr)
	}

	return nil
}

// GetMemoryFootprint provides the size of the store in memory in bytes
func (m *MultiMap[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*m)

	memoryFootprint := common.NewMemoryFootprint(selfSize)
	memoryFootprint.AddChild("linearHash", m.table.GetMemoryFootprint())
	memoryFootprint.AddChild("pagePool", m.pagePool.GetMemoryFootprint())
	memoryFootprint.AddChild("pageStore", m.pageStore.GetMemoryFootprint())
	return memoryFootprint
}

func readMetadata(path string) (numBuckets, lastBucket, lastOverflow int, freeIds []int, err error) {
	metadataFile, err := os.OpenFile(path+"/metadata.dat", os.O_RDONLY|os.O_CREATE, 0600)
	if err != nil {
		return
	}
	defer metadataFile.Close()

	// read metadata
	size := 3 * 4
	data := make([]byte, size)
	_, err = metadataFile.Read(data)
	if err == nil {
		numBuckets = int(binary.BigEndian.Uint32(data[0:4]))
		lastBucket = int(binary.BigEndian.Uint32(data[4:8]))
		lastOverflow = int(binary.BigEndian.Uint32(data[8:12]))
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

func writeMetadata[K comparable, V comparable](
	path string,
	linearHash *pagepool.LinearHashMultiMap[K, V],
	pagePool *pagepool.PagePool[*pagepool.KVPage[K, V]],
	pageStore *pagepool.TwoFilesPageStorage) (err error) {

	metadataFile, err := os.OpenFile(path+"/metadata.dat", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return
	}
	defer metadataFile.Close()

	size := (3 + len(pagePool.GetFreeIds())) * 4
	metadata := make([]byte, 0, size)

	metadata = binary.BigEndian.AppendUint32(metadata, uint32(linearHash.GetBuckets()))
	metadata = binary.BigEndian.AppendUint32(metadata, uint32(pageStore.GetLastId().Bucket()))
	metadata = binary.BigEndian.AppendUint32(metadata, uint32(pageStore.GetLastId().Overflow()))

	for _, freeId := range pagePool.GetFreeIds() {
		metadata = binary.BigEndian.AppendUint32(metadata, uint32(freeId))
	}

	_, err = metadataFile.Write(metadata)

	return
}

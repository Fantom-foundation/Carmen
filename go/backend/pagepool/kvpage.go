package pagepool

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// KVPage is key-value structure that can be evicted, persisted, and reloaded from the disk.
// it tracks its updates and can tell if the stored values have been updated
type KVPage[K comparable, V comparable] struct {
	list            []common.MapEntry[K, V]
	comparator      common.Comparator[K]
	keySerializer   common.Serializer[K]
	indexSerializer common.Serializer[V]
	sizeBytes       int //   the size in bytes

	last    int
	isDirty bool // is dirty is set to true when a value is modified and used to determine whether the Page needs to be updated on disk

	next    int // position of next Page in the file
	hasNext bool
}

func NewKVPage[K comparable, V comparable](sizeBytes int, keySerializer common.Serializer[K], indexSerializer common.Serializer[V], comparator common.Comparator[K]) *KVPage[K, V] {
	pageItems := NumKeysPage(sizeBytes, keySerializer, indexSerializer)
	list := make([]common.MapEntry[K, V], pageItems)
	for i := 0; i < pageItems; i++ {
		list[i] = common.MapEntry[K, V]{}
	}

	return &KVPage[K, V]{
		list:            list,
		comparator:      comparator,
		keySerializer:   keySerializer,
		indexSerializer: indexSerializer,
		sizeBytes:       sizeBytes,
		isDirty:         true, // new page is dirty
	}
}

func (c *KVPage[K, V]) FromBytes(pageData []byte) {
	// read in metadata - link to the next page
	next := binary.BigEndian.Uint32(pageData[len(pageData)-4:])
	if next != 0 {
		c.setNext(int(next))
	}
	numItems := binary.BigEndian.Uint16(pageData[len(pageData)-6:])

	// convert to key value pairs
	keySize := c.keySerializer.Size()
	valSize := c.indexSerializer.Size()
	pairSize := keySize + valSize

	// update directly the entries for the best speed
	var dataIndex uint16
	for i := 0; i < c.sizeBytes-pairSize; i += pairSize {
		if numItems == dataIndex {
			break
		}
		key := c.keySerializer.FromBytes(pageData[i : i+keySize])
		val := c.indexSerializer.FromBytes(pageData[i+keySize : i+keySize+valSize])
		c.list[dataIndex].Key = key
		c.list[dataIndex].Val = val

		dataIndex += 1
	}
	c.setSize(int(numItems))
}

func (c *KVPage[K, V]) ToBytes(pageData []byte) {
	var offset int
	keySize := c.keySerializer.Size()
	valueSize := c.indexSerializer.Size()

	for _, item := range c.getEntries() {
		c.keySerializer.CopyBytes(item.Key, pageData[offset:offset+keySize])
		c.indexSerializer.CopyBytes(item.Val, pageData[offset+keySize:offset+keySize+valueSize])
		offset += keySize + valueSize
	}

	// put in metadata - the link to the next page
	if c.hasNext {
		binary.BigEndian.PutUint32(pageData[len(pageData)-4:], uint32(c.next))
	} else {
		binary.BigEndian.PutUint32(pageData[len(pageData)-4:], uint32(0))
	}

	// number of keys
	binary.BigEndian.PutUint16(pageData[len(pageData)-6:len(pageData)-4], uint16(c.size()))
}

// forEach calls the callback for each key-value pair in the list
func (c *KVPage[K, V]) forEach(callback func(K, V)) {
	for i := 0; i < c.last; i++ {
		callback(c.list[i].Key, c.list[i].Val)
	}
}

// get returns a value from the list or false.
func (c *KVPage[K, V]) get(key K) (val V, exists bool) {
	if index, exists := c.findItem(key); exists {
		return c.list[index].Val, true
	}

	return
}

// update only replaces the value at the input index
func (c *KVPage[K, V]) update(index int, val V) {
	c.isDirty = true
	c.list[index].Val = val
}

// update returns a value at the input index
func (c *KVPage[K, V]) getVal(index int) V {
	return c.list[index].Val
}

func (c *KVPage[K, V]) put(key K, val V) {
	index, exists := c.findItem(key)
	if exists {
		c.update(index, val)
		return
	}

	c.insert(index, key, val)
}

func (c *KVPage[K, V]) add(key K, val V) {
	index, exists := c.findValue(key, val)
	if !exists {
		c.insert(index, key, val)
	}
}

// insert adds the input key and value at the index position in this page
// items occupying this position and following items are shifted one position
// towards the end of the page
func (c *KVPage[K, V]) insert(index int, key K, val V) {
	c.isDirty = true
	// found insert
	if index < c.last {

		// shift
		for j := c.last - 1; j >= index; j-- {
			c.list[j+1] = c.list[j]
		}

		c.list[index].Key = key
		c.list[index].Val = val

		c.last += 1
		return
	}

	// no place found - put at the end
	c.list[c.last].Key = key
	c.list[c.last].Val = val

	c.last += 1
}

func (c *KVPage[K, V]) setNext(next int) {
	c.hasNext = true
	c.next = next
	c.isDirty = true
}

func (c *KVPage[K, V]) removeNext() {
	c.hasNext = false
	c.next = 0
	c.isDirty = true
}

// remove deletes the key from the map and returns whether an element was removed.
func (c *KVPage[K, V]) remove(key K) (exists bool) {
	if index, exists := c.findItem(key); exists {
		c.removeIndex(index)
		return true
	}

	return false
}

func (c *KVPage[K, V]) removeVal(key K, val V) bool {
	if index, exists := c.findValue(key, val); exists {
		c.removeIndex(index)
		return true
	}

	return false
}

func (c *KVPage[K, V]) removeIndex(index int) {
	c.isDirty = true
	for j := index; j < c.last-1; j++ {
		c.list[j] = c.list[j+1]
	}
	c.last -= 1
}

func (c *KVPage[K, V]) removeAll(key K) (start, end int, exists bool) {
	if start, end, exists = c.findRange(key); exists {
		// shift
		window := end - start
		for j := start; j < c.last-window; j++ {
			c.list[j] = c.list[j+window]
		}
		c.last -= window
		c.isDirty = true
	}

	return
}

func (c *KVPage[K, V]) bulkInsert(data []common.MapEntry[K, V]) {
	for i := 0; i < len(data); i++ {
		c.list[i+c.last] = data[i]
	}

	c.last += len(data)
	c.isDirty = true
}

func (c *KVPage[K, V]) getEntries() []common.MapEntry[K, V] {
	return c.list[0:c.last]
}

// appendAll appends the input slice with the values matching the input key
// and returns the updated slice
func (c *KVPage[K, V]) appendAll(key K, out []V) []V {
	if start, end, exists := c.findRange(key); exists {
		// copy values to the out list
		for j := start; j < end; j++ {
			out = append(out, c.list[j].Val)
		}
	}
	return out
}

// setSize allows for explicitly setting the size for situations
// where the page is loaded.
func (c *KVPage[K, V]) setSize(size int) {
	c.isDirty = true
	c.last = size
}

func (c *KVPage[K, V]) size() int {
	return c.last
}

func (c *KVPage[K, V]) SizeBytes() int {
	return c.sizeBytes
}

func (c *KVPage[K, V]) IsDirty() bool {
	return c.isDirty
}

func (c *KVPage[K, V]) SetDirty(dirty bool) {
	c.isDirty = dirty
}

func (c *KVPage[K, V]) Clear() {
	c.next = 0
	c.hasNext = false
	c.isDirty = true
	c.last = 0
}

// findRange finds a range where the key starts and ends in this page.
// it also returns true  when the key  exists in the page at all
func (c *KVPage[K, V]) findRange(key K) (start, end int, exists bool) {
	index, exists := c.findItem(key)
	start = index
	if exists {
		// check the same key is not already present after this index
		end = c.last
		for i := index; i < c.last-1; i++ {
			if c.list[i+1].Key != key {
				end = i + 1
				break // out of range
			}
		}
	}

	return start, end, exists
}

// findValue check whether the input value exists in the list,
// and it returns its position. If the value does not exist,
// the returned position is the position where the new key and the value should be inserted
func (c *KVPage[K, V]) findValue(key K, val V) (position int, exists bool) {
	index, keyExists := c.findItem(key)
	if keyExists {
		// check the same key is not already present after this index
		for i := index; i < c.last; i++ {
			if c.list[i].Key != key {
				break // out of range
			}
			if c.list[i].Val == val {
				return i, true
			}
		}
	}

	return index, false
}

// findItem finds a key in the list, if it exists.
// It returns the index of the key that was found, and it returns true.
// If the key does not exist, it returns false and the index is equal to the last
// visited position in the list, traversed using binary search.
// The index is increased by one when the last visited key was lower than the input key
// so the new key may be inserted after this key.
// It means the index can be used as a position to insert the key in the list.
func (c *KVPage[K, V]) findItem(key K) (index int, exists bool) {
	start := 0
	end := c.last - 1
	mid := start
	var res int
	for start <= end {
		mid = (start + end) / 2
		res = c.comparator.Compare(&c.list[mid].Key, &key)
		if res == 0 {
			// iterate "left" of this index to find the lower bound of possible
			// the same keys (for the multimap)
			var lower int
			for i := mid; i > 0; i-- {
				if c.list[i-1].Key != key {
					lower = i
					break // out of range
				}
			}
			return lower, true
		} else if res < 0 {
			start = mid + 1
		} else {
			end = mid - 1
		}
	}

	if res < 0 {
		mid += 1
	}
	return mid, false
}

func (c *KVPage[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*c)
	entrySize := unsafe.Sizeof(common.MapEntry[K, V]{})
	var v V
	valSize := unsafe.Sizeof(v)

	// the page is always fully allocated - i.e. use the capacity
	return common.NewMemoryFootprint(selfSize + uintptr(len(c.list))*(entrySize+valSize))
}

// NumKeysPage computes how many key-value pairs fit in the page with given byte size
func NumKeysPage[K any, V any](sizeBytes int, keySerializer common.Serializer[K], indexSerializer common.Serializer[V]) int {
	pageMetaSize := 2 + 4
	pageItems := (sizeBytes - pageMetaSize) / (keySerializer.Size() + indexSerializer.Size()) // number of key-value pairs per page
	return pageItems
}

// ByteSizePage computes the byte size of the page to store the input number of key-value pairs
func ByteSizePage[K any, V any](pageItems int, keySerializer common.Serializer[K], indexSerializer common.Serializer[V]) int {
	pageMetaSize := 2 + 4
	sizeBytes := pageMetaSize + pageItems*(keySerializer.Size()+indexSerializer.Size())
	return sizeBytes
}

// KVPageFactory creates a factory for KVPage defining its size in bytes
func KVPageFactory[K comparable, V comparable](pageSize int, keySerializer common.Serializer[K], indexSerializer common.Serializer[V], comparator common.Comparator[K]) func() *KVPage[K, V] {
	return func() *KVPage[K, V] {
		return NewKVPage[K, V](pageSize, keySerializer, indexSerializer, comparator)
	}
}

// KVPageFactoryNumItems creates a factory for KVPage, which defines its size as the number of max allowed key-value pairs.
func KVPageFactoryNumItems[K comparable, V comparable](pageItems int, keySerializer common.Serializer[K], indexSerializer common.Serializer[V], comparator common.Comparator[K]) func() *KVPage[K, V] {
	sizeBytes := ByteSizePage(pageItems, keySerializer, indexSerializer)
	return KVPageFactory[K, V](sizeBytes, keySerializer, indexSerializer, comparator)
}

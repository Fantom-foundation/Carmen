package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"unsafe"
)

// Page is key-value structure that can be evicted, persisted, and reloaded from the disk.
// it tracks its updates and can tell if the stored values have been updated
type Page[K comparable, V comparable] struct {
	list       []common.MapEntry[K, V]
	comparator common.Comparator[K]
	size       int

	isDirty bool // is dirty is set to true when a value is modified and used to determine whether the Page needs to be updated on disk

	next    PageId // position of next Page in the file
	hasNext bool
}

func NewPage[K comparable, V comparable](capacity int, comparator common.Comparator[K]) *Page[K, V] {
	list := make([]common.MapEntry[K, V], capacity)
	for i := 0; i < capacity; i++ {
		list[i] = common.MapEntry[K, V]{}
	}

	return &Page[K, V]{
		list:       list,
		comparator: comparator,
		isDirty:    true, // new page is dirty
	}
}

// ForEach calls the callback for each key-value pair in the list
func (c *Page[K, V]) ForEach(callback func(K, V)) {
	for i := 0; i < c.size; i++ {
		callback(c.list[i].Key, c.list[i].Val)
	}
}

// Get returns a value from the list or false.
func (c *Page[K, V]) Get(key K) (val V, exists bool) {
	if index, exists := c.findItem(key); exists {
		return c.list[index].Val, true
	}

	return
}

// Put associates a key to the list.
// If the key is already present, the value is updated.
func (c *Page[K, V]) Put(key K, val V) {
	index, exists := c.findItem(key)
	if exists {
		c.update(index, val)
		return
	}

	c.insert(index, key, val)
}

func (c *Page[K, V]) Add(key K, val V) {
	index, exists := c.findValue(key, val)
	if !exists {
		c.insert(index, key, val)
	}
}

// update only replaces the value at the input index
func (c *Page[K, V]) update(index int, val V) {
	c.isDirty = true
	c.list[index].Val = val
}

// update returns a value at the input index
func (c *Page[K, V]) get(index int) V {
	return c.list[index].Val
}

// insert adds the input key and value at the index position in this page
// items occupying this position and following items are shifted one position
// towards the end of the page
func (c *Page[K, V]) insert(index int, key K, val V) {
	c.isDirty = true
	// found insert
	if index < c.size {

		// shift
		for j := c.size - 1; j >= index; j-- {
			c.list[j+1] = c.list[j]
		}

		c.list[index].Key = key
		c.list[index].Val = val

		c.size += 1
		return
	}

	// no place found - put at the end
	c.list[c.size].Key = key
	c.list[c.size].Val = val

	c.size += 1
}

func (c *Page[K, V]) SetNext(next PageId) {
	c.hasNext = true
	c.next = next
	c.isDirty = true
}

func (c *Page[K, V]) RemoveNext() {
	c.hasNext = false
	c.next = PageId{}
	c.isDirty = true
}

func (c *Page[K, V]) HasNext() bool {
	return c.hasNext
}

func (c *Page[K, V]) NextPage() PageId {
	return c.next
}

// Remove deletes the key from the map and returns whether an element was removed.
func (c *Page[K, V]) Remove(key K) (exists bool) {
	if index, exists := c.findItem(key); exists {
		// shift
		for j := index; j < c.size-1; j++ {
			c.list[j] = c.list[j+1]
		}
		c.size -= 1
		c.isDirty = true

		return true
	}

	return false
}

func (c *Page[K, V]) RemoveAll(key K) {
	c.removeAll(key)
}

func (c *Page[K, V]) removeAll(key K) (start, end int, exists bool) {
	if start, end, exists = c.findRange(key); exists {
		// shift
		window := end - start
		for j := start; j < c.size-window; j++ {
			c.list[j] = c.list[j+window]
		}
		c.size -= window
		c.isDirty = true
	}

	return
}

func (c *Page[K, V]) BulkInsert(data []common.MapEntry[K, V]) {
	for i := 0; i < len(data); i++ {
		c.list[i+c.size] = data[i]
	}

	c.size += len(data)
	c.isDirty = true
}

func (c *Page[K, V]) GetEntries() []common.MapEntry[K, V] {
	return c.list[0:c.Size()]
}

func (c *Page[K, V]) GetAll(key K) []V {
	return c.appendAll(key, make([]V, 0, c.Size()))
}

// appendAll appends the input slice with the values matching the input key
// and returns the updated slice
func (c *Page[K, V]) appendAll(key K, out []V) []V {
	if start, end, exists := c.findRange(key); exists {
		// copy values to the out list
		for j := start; j < end; j++ {
			out = append(out, c.list[j].Val)
		}
	}
	return out
}

// GetRaw provides direct access to the entry list
// It contains all data based on the page capacity even if actual size is smaller
func (c *Page[K, V]) GetRaw() []common.MapEntry[K, V] {
	return c.list
}

func (c *Page[K, V]) Size() int {
	return c.size
}

// SetSize allows for explicitly setting the size for situations
// where the page is loaded.
func (c *Page[K, V]) SetSize(size int) {
	c.isDirty = true
	c.size = size
}

func (c *Page[K, V]) IsDirty() bool {
	return c.isDirty
}

func (c *Page[K, V]) Clear() {
	c.next = PageId{}
	c.hasNext = false
	c.isDirty = true
	c.size = 0
}

// findRange finds a range where the key starts and ends in this page.
// it also returns true  when the key  exists in the page at all
func (c *Page[K, V]) findRange(key K) (start, end int, exists bool) {
	index, exists := c.findItem(key)
	start = index
	if exists {
		// check the same key is not already present after this index
		end = c.size
		for i := index; i < c.size-1; i++ {
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
// the returned position is the possition where the new key and the value should be inserted
func (c *Page[K, V]) findValue(key K, val V) (position int, exists bool) {
	index, keyExists := c.findItem(key)
	if keyExists {
		// check the same key is not already present after this index
		for i := index; i < c.size; i++ {
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
func (c *Page[K, V]) findItem(key K) (index int, exists bool) {
	start := 0
	end := c.size - 1
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

func (c *Page[K, V]) GetMemoryFootprint() *common.MemoryFootprint {
	selfSize := unsafe.Sizeof(*c)
	entrySize := unsafe.Sizeof(common.MapEntry[K, V]{})
	var v V
	valSize := unsafe.Sizeof(v)

	// the page is always fully allocated - i.e. use the capacity
	return common.NewMemoryFootprint(selfSize + uintptr(len(c.list))*(entrySize+valSize))
}

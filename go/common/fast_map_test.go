package common

import (
	"testing"
)

// Map provides an interface to handle different implementations
// in test and benchmarks uniformely.
type Map[K comparable, V any] interface {
	Set(key Key, value V)
	Get(key Key) (V, bool)
	Delete(key Key) bool
	Length() int
	ForEach(func(K, V))
	Clear()
}

// BuildInMap adapts the language level map as a reference map for
// tests and benchmarks.
type BuildInMap[K comparable, V any] struct {
	data map[K]V
}

func NewBuildInMap[K comparable, V any]() *BuildInMap[K, V] {
	return &BuildInMap[K, V]{data: map[K]V{}}
}

func (m *BuildInMap[K, V]) Set(key K, value V) {
	m.data[key] = value
}

func (m *BuildInMap[K, V]) Get(key K) (V, bool) {
	value, exists := m.data[key]
	return value, exists
}

func (m *BuildInMap[K, V]) Delete(key K) bool {
	_, exists := m.data[key]
	delete(m.data, key)
	return exists
}

func (m *BuildInMap[K, V]) Length() int {
	return len(m.data)
}

func (m *BuildInMap[K, V]) ForEach(op func(K, V)) {
	for k, v := range m.data {
		op(k, v)
	}
}

func (m *BuildInMap[K, V]) Clear() {
	m.data = map[K]V{}
}

// KeyIntMap implements the FastMap structure for a fixed Key/Value type pair to evaluate
// the impact of the generic implementation of the FastMap.
type KeyIntMap struct {
	buckets     [kFastMapBuckets]fmPtr
	data        []keyIntMapEntry
	generation  uint16
	size        int
	usedBuckets []uint16
}

type keyIntMapEntry struct {
	key   Key
	value int
	next  fmPtr
}

func NewKeyIntMap() *KeyIntMap {
	res := &KeyIntMap{
		data:        make([]keyIntMapEntry, 0, 10000),
		usedBuckets: make([]uint16, 0, kFastMapBuckets),
	}
	res.Clear()
	return res
}

func (m *KeyIntMap) Get(key Key) (int, bool) {
	hash := KeyHasher{}.Hash(key)
	cur := m.toPos(m.buckets[hash])
	for 0 <= cur && cur < int64(len(m.data)) {
		if m.data[cur].key == key {
			return m.data[cur].value, true
		}
		cur = m.toPos(m.data[cur].next)
	}
	return 0, false
}

func (m *KeyIntMap) Set(key Key, value int) {
	hash := KeyHasher{}.Hash(key)
	cur := m.toPos(m.buckets[hash])

	if cur < 0 {
		m.usedBuckets = append(m.usedBuckets, hash)
	}

	for 0 <= cur && cur < int64(len(m.data)) {
		if m.data[cur].key == key {
			m.data[cur].value = value
			return
		}
		cur = m.toPos(m.data[cur].next)
	}
	m.size++
	new := len(m.data)
	m.data = append(m.data, keyIntMapEntry{})
	m.data[new].key = key
	m.data[new].value = value
	m.data[new].next = m.buckets[hash]
	m.buckets[hash] = m.toPtr(int64(new))
}

func (m *KeyIntMap) Delete(key Key) bool {
	hash := KeyHasher{}.Hash(key)
	cur := m.toPos(m.buckets[hash])
	ptr := &m.buckets[hash]
	for 0 <= cur && cur < int64(len(m.data)) {
		if m.data[cur].key == key {
			*ptr = m.data[cur].next
			m.size--
			return true
		}
		ptr = &m.data[cur].next
		cur = m.toPos(m.data[cur].next)
	}
	return false
}

func (m *KeyIntMap) Clear() {
	m.data = m.data[0:0]
	m.generation++
	m.size = 0
	m.usedBuckets = m.usedBuckets[0:0]
	if m.generation == 0 {
		for i := range m.buckets {
			m.buckets[i] = -1
		}
	}
}

func (m *KeyIntMap) Length() int {
	return m.size
}

func (m *KeyIntMap) ForEach(op func(Key, int)) {
	for _, i := range m.usedBuckets {
		pos := m.toPos(m.buckets[i])
		for 0 <= pos && pos < int64(len(m.data)) {
			entry := &m.data[pos]
			op(entry.key, entry.value)
			pos = m.toPos(entry.next)
		}
	}
}

func (m *KeyIntMap) toPtr(pos int64) fmPtr {
	if pos < 0 {
		return fmPtr(pos)
	}
	return fmPtr(int64(pos)<<16 | int64(m.generation))
}

func (m *KeyIntMap) toPos(ptr fmPtr) int64 {
	if ptr < 0 || uint16(ptr) != m.generation {
		return -1
	}
	return int64(ptr) >> 16
}

type namedMapConfig struct {
	name string
	get  func() Map[Key, int]
}

type KeyHasher struct{}

func (h KeyHasher) Hash(key Key) uint16 {
	return uint16(key[30])<<8 | uint16(key[31])
}

func getMapConfigs() []namedMapConfig {
	return []namedMapConfig{
		{"BuildIn", func() Map[Key, int] { return NewBuildInMap[Key, int]() }},
		{"FastMap", func() Map[Key, int] { return NewFastMap[Key, int](KeyHasher{}) }},
		{"SpecializedKeyIntMap", func() Map[Key, int] { return NewKeyIntMap() }},
	}
}

func TestMapInsertedIsContained(t *testing.T) {
	for _, config := range getMapConfigs() {
		data := config.get()
		t.Run(config.name, func(t *testing.T) {
			key := Key{12}
			if _, exists := data.Get(key); exists {
				t.Errorf("Data should not contain key 12")
			}
			data.Set(key, 14)
			if value, exists := data.Get(key); !exists || value != 14 {
				t.Errorf("Data should contain key 12 with value 14, got %v,%v", value, exists)
			}
		})
	}
}

func TestMapInsertedIsContainedExhaustive(t *testing.T) {
	const N = 1000
	for _, config := range getMapConfigs() {
		data := config.get()
		t.Run(config.name, func(t *testing.T) {
			key := Key{}
			for i := 0; i < N; i++ {
				for j := 0; j < N; j++ {
					key[30] = byte(j >> 8)
					key[31] = byte(j)
					want := j < i
					if _, got := data.Get(key); want != got {
						t.Errorf("Error in contains for %v/%v: wanted %v, got %v", i, j, want, got)
					}
				}
				key[30] = byte(i >> 8)
				key[31] = byte(i)
				data.Set(key, 1)
			}
		})
	}
}

func TestMapDeleteRemovesKey(t *testing.T) {
	for _, config := range getMapConfigs() {
		data := config.get()
		t.Run(config.name, func(t *testing.T) {
			key := Key{12}
			data.Set(key, 1)
			if _, exists := data.Get(key); !exists {
				t.Errorf("Data should contain key 12")
			}
			if was_present := data.Delete(key); !was_present {
				t.Errorf("Delete did not find key %v", key)
			}
			if _, exists := data.Get(key); exists {
				t.Errorf("Data should not contain key 12")
			}
		})
	}
}

func TestMapDeleteRemovesSelectedKeyFromBucket(t *testing.T) {
	for _, config := range getMapConfigs() {
		data := config.get()
		t.Run(config.name, func(t *testing.T) {

			for i := 0; i <= 10; i++ {
				// all those keys end up in the same bucket.
				key := Key{byte(i)}
				data.Set(key, i)
			}

			// remove the first element in the bucket list (last inserted)
			data.Delete(Key{byte(10)})
			if _, exists := data.Get(Key{byte(10)}); exists {
				t.Errorf("Failed to delete key 10")
			}

			for i := 0; i < 10; i++ {
				// all other keys should still be there
				key := Key{byte(i)}
				if _, exists := data.Get(key); !exists {
					t.Errorf("Missing key %v", key)
				}
			}

			// remove the first element in the bucket list (first inserted)
			data.Delete(Key{byte(0)})
			if _, exists := data.Get(Key{byte(0)}); exists {
				t.Errorf("Failed to delete key 0")
			}

			for i := 1; i < 10; i++ {
				// all other keys should still be there
				key := Key{byte(i)}
				if _, exists := data.Get(key); !exists {
					t.Errorf("Missing key %v", key)
				}
			}

			// remove an element in the middle of the bucket list
			data.Delete(Key{byte(5)})
			if _, exists := data.Get(Key{byte(5)}); exists {
				t.Errorf("Failed to delete key 5")
			}

			for i := 1; i < 10; i++ {
				if i == 5 {
					continue
				}
				// all other keys should still be there
				key := Key{byte(i)}
				if _, exists := data.Get(key); !exists {
					t.Errorf("Missing key %v", key)
				}
			}
		})
	}
}

func TestMapClearRemovesContent(t *testing.T) {
	for _, config := range getMapConfigs() {
		data := config.get()
		t.Run(config.name, func(t *testing.T) {
			key := Key{12}
			data.Set(key, 1)
			if _, exists := data.Get(key); !exists {
				t.Errorf("Data should contain key 12")
			}
			data.Clear()
			if _, exists := data.Get(key); exists {
				t.Errorf("Data should not contain key 12")
			}
		})
	}
}

func TestMapClearRemovesAllContent(t *testing.T) {
	const N = 1000
	for _, config := range getMapConfigs() {
		data := config.get()
		t.Run(config.name, func(t *testing.T) {
			key := Key{}
			for i := 0; i < N; i++ {
				key[30] = byte(i >> 8)
				key[31] = byte(i)
				data.Set(key, i)
			}
			for i := 0; i < N; i++ {
				key[30] = byte(i >> 8)
				key[31] = byte(i)
				if value, exists := data.Get(key); !exists || value != i {
					t.Errorf("Key not present or wrong value; present: %v, value=%v, should %v", exists, value, i)
				}
			}
			data.Clear()
			for i := 0; i < N; i++ {
				key[30] = byte(i >> 8)
				key[31] = byte(i)
				if _, exists := data.Get(key); exists {
					t.Errorf("Key still present: %v", key)
				}
			}
		})
	}
}

func TestMapAddingKeysIsReflectedInSize(t *testing.T) {
	for _, config := range getMapConfigs() {
		t.Run(config.name, func(t *testing.T) {
			data := config.get()
			want := 0
			if got := data.Length(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
			data.Set(Key{12}, 4)
			want = 1
			if got := data.Length(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
			data.Set(Key{14}, 6)
			want = 2
			if got := data.Length(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
			// Update does not change the size
			data.Set(Key{12}, 6)
			if got := data.Length(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
		})
	}
}

func TestMapDeletingKeysIsReflectedInSize(t *testing.T) {
	for _, config := range getMapConfigs() {
		t.Run(config.name, func(t *testing.T) {
			data := config.get()
			data.Set(Key{12}, 1)
			data.Set(Key{14}, 2)
			data.Set(Key{16}, 3)
			want := 3
			if got := data.Length(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
			data.Delete(Key{12})
			want = 2
			if got := data.Length(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
			data.Delete(Key{14})
			want = 1
			if got := data.Length(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
			// Deleting a missing key does not change the size
			data.Delete(Key{8})
			if got := data.Length(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
		})
	}
}

func TestMapClearResetsSize(t *testing.T) {
	for _, config := range getMapConfigs() {
		t.Run(config.name, func(t *testing.T) {
			data := config.get()
			data.Set(Key{12}, 1)
			data.Set(Key{14}, 2)
			data.Set(Key{16}, 3)
			want := 3
			if got := data.Length(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
			data.Clear()
			want = 0
			if got := data.Length(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
		})
	}
}

func TestMapForEachVisitsAllElements(t *testing.T) {
	for _, config := range getMapConfigs() {
		t.Run(config.name, func(t *testing.T) {
			data := config.get()

			for i := 0; i < 100; i++ {
				elements := map[Key]int{}
				data.ForEach(func(key Key, value int) {
					_, exists := elements[key]
					if exists {
						t.Errorf("Visited element more than once: %v", key)
					}
					elements[key] = value
				})

				if i != len(elements) {
					t.Errorf("Invalid number of elements visited, expected %v, got %v", i, len(elements))
				} else {
					for j := 0; j < i; j++ {
						key := Key{byte(j)}
						value, exists := elements[key]
						if !exists {
							t.Errorf("Failed to visit key %v", key)
						} else if value != j*j {
							t.Errorf("Wrong value assigned to key %v: wanted %v, got %v", key, j*j, value)
						}
					}
				}

				data.Set(Key{byte(i)}, i*i)
			}
		})
	}
}

func BenchmarkMapInsertAndClear(b *testing.B) {
	for _, config := range getMapConfigs() {
		key := Key{}
		data := config.get()
		data.Clear()
		b.Run(config.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for j := 0; j < 100; j++ {
					key[31] *= 7
					data.Set(key, j)
				}
				data.Clear()
			}
		})
	}
}

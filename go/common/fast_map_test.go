// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package common

import (
	"math/rand"
	"testing"
)

// BuildInMap adapts the language level map as a reference map for
// tests and benchmarks.
type BuildInMap[K comparable, V any] struct {
	data map[K]V
}

func NewBuildInMap[K comparable, V any]() *BuildInMap[K, V] {
	return &BuildInMap[K, V]{data: map[K]V{}}
}

func (m *BuildInMap[K, V]) Put(key K, value V) {
	m.data[key] = value
}

func (m *BuildInMap[K, V]) Get(key K) (V, bool) {
	value, exists := m.data[key]
	return value, exists
}

func (m *BuildInMap[K, V]) Remove(key K) (exists bool) {
	_, exists = m.data[key]
	delete(m.data, key)
	return
}

func (m *BuildInMap[K, V]) Size() int {
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
	hash := KeyShortHasher{}.Hash(key)
	cur := m.toPos(m.buckets[hash])
	for 0 <= cur && cur < int64(len(m.data)) {
		if m.data[cur].key == key {
			return m.data[cur].value, true
		}
		cur = m.toPos(m.data[cur].next)
	}
	return 0, false
}

func (m *KeyIntMap) Put(key Key, value int) {
	hash := KeyShortHasher{}.Hash(key)
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

func (m *KeyIntMap) Remove(key Key) bool {
	hash := KeyShortHasher{}.Hash(key)
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

func (m *KeyIntMap) Size() int {
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

type KeyShortHasher struct{}

func (h KeyShortHasher) Hash(key Key) uint16 {
	return uint16(key[30])<<8 | uint16(key[31])
}

func getMapConfigs() []namedMapConfig {
	return []namedMapConfig{
		{"BuildIn", func() Map[Key, int] { return NewBuildInMap[Key, int]() }},
		{"FastMap", func() Map[Key, int] { return NewFastMap[Key, int](KeyShortHasher{}) }},
		{"SpecializedKeyIntMap", func() Map[Key, int] { return NewKeyIntMap() }},
		{"SortedMap", func() Map[Key, int] { return NewSortedMap[Key, int](kFastMapBuckets, KeyComparator{}) }},
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
			data.Put(key, 14)
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
				data.Put(key, 1)
			}
		})
	}
}

func TestMapDeleteRemovesKey(t *testing.T) {
	for _, config := range getMapConfigs() {
		data := config.get()
		t.Run(config.name, func(t *testing.T) {
			key := Key{12}
			data.Put(key, 1)
			if _, exists := data.Get(key); !exists {
				t.Errorf("Data should contain key 12")
			}
			if was_present := data.Remove(key); !was_present {
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
				data.Put(key, i)
			}

			// remove the first element in the bucket list (last inserted)
			data.Remove(Key{byte(10)})
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
			data.Remove(Key{byte(0)})
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
			data.Remove(Key{byte(5)})
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
			data.Put(key, 1)
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
				data.Put(key, i)
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
			if got := data.Size(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
			data.Put(Key{12}, 4)
			want = 1
			if got := data.Size(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
			data.Put(Key{14}, 6)
			want = 2
			if got := data.Size(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
			// Update does not change the size
			data.Put(Key{12}, 6)
			if got := data.Size(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
		})
	}
}

func TestMapDeletingKeysIsReflectedInSize(t *testing.T) {
	for _, config := range getMapConfigs() {
		t.Run(config.name, func(t *testing.T) {
			data := config.get()
			data.Put(Key{12}, 1)
			data.Put(Key{14}, 2)
			data.Put(Key{16}, 3)
			want := 3
			if got := data.Size(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
			data.Remove(Key{12})
			want = 2
			if got := data.Size(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
			data.Remove(Key{14})
			want = 1
			if got := data.Size(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
			// Deleting a missing key does not change the size
			data.Remove(Key{8})
			if got := data.Size(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
		})
	}
}

func TestMapClearResetsSize(t *testing.T) {
	for _, config := range getMapConfigs() {
		t.Run(config.name, func(t *testing.T) {
			data := config.get()
			data.Put(Key{12}, 1)
			data.Put(Key{14}, 2)
			data.Put(Key{16}, 3)
			want := 3
			if got := data.Size(); want != got {
				t.Errorf("invalid length, wanted %v, got %v", want, got)
			}
			data.Clear()
			want = 0
			if got := data.Size(); want != got {
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

				data.Put(Key{byte(i)}, i*i)
			}
		})
	}
}

func TestMap_Fill_All_Generations(t *testing.T) {
	m := NewFastMap[Key, int](KeyShortHasher{})

	// fill in data
	var k Key
	for i := 0; i < 100; i++ {
		m.Put(k, i)
		k[i%32]++
	}

	// check some buckets filled
	var nonEmpty bool
	for _, d := range m.buckets {
		if d != -1 {
			nonEmpty = true
			break
		}
	}

	if !nonEmpty {
		t.Fatalf("some buckets should be used")
	}

	// have to use all generations
	for i := 0; i < 1<<16; i++ {
		m.Clear()
	}

	// check some buckets filled
	var nonEmptyAfter bool
	for _, d := range m.buckets {
		if d != -1 {
			nonEmptyAfter = true
			break
		}
	}

	if nonEmptyAfter {
		t.Fatalf("all buckets should be empty")
	}
}

func TestFastMap_CopyTo(t *testing.T) {
	m := NewFastMap[Key, int](KeyShortHasher{})

	// fill in data
	var k Key
	for i := 0; i < 100; i++ {
		m.Put(k, i)
		k[i%32]++
	}

	shadow := NewFastMap[Key, int](KeyShortHasher{})
	m.CopyTo(shadow)

	m.ForEach(func(key Key, val int) {
		if shadowVal, exists := shadow.Get(key); !exists || shadowVal != val {
			t.Errorf("values do not match: %v -> got %v != want %v", key, shadowVal, val)
		}
	})

	// do the same in reverse
	shadow.ForEach(func(key Key, shadowVal int) {
		if val, exists := m.Get(key); !exists || shadowVal != val {
			t.Errorf("values do not match: %v -> got %v != want %v", key, shadowVal, val)
		}
	})
}

func TestMap_Internal_Negative_Position(t *testing.T) {
	m := NewFastMap[Key, int](KeyShortHasher{})

	for i := 0; i < 1000; i++ {
		num := uint64(rand.Int63()) | uint64(1<<63) // always negative num
		if got, want := m.toPtr(int64(num)), num; got != fmPtr(want) {
			t.Errorf("negative value should be returned unchanged: %d != %d", got, want)
		}
	}
}

func BenchmarkMapInsertAndClear(b *testing.B) {
	for _, config := range getMapConfigs() {
		key := Key{}
		data := config.get()
		data.Clear()
		b.Run(config.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				key[31] = byte(rand.Intn(256))
				for j := 0; j < 100; j++ {
					key[31] *= 7
					data.Put(key, j)
				}
				data.Clear()
			}
		})
	}
}

// ------------------------------ Fuzzing --------------------------------

// To run a fuzzer on a fast map, we are generating sequences of commands
// executing modifications on fast maps and regular maps, and check whether
// their content is in sync.
//
// To make operations easy and fast, the targeted key and value types are bytes.
//
// To run this fuzz test, us the following command:
//
//     go test ./common -run FuzzMap -fuzz=FuzzMapOperations
//
// It will start the fuzzing and try to find inputs breaking the test case. For
// more information see: https://go.dev/security/fuzz/

// testByteHasher is the hashing algorithm used in the fuzzer, deliberately producing collisions.
type testByteHasher struct{}

func (b testByteHasher) Hash(d byte) uint16 {
	// By only using 6 of the 8 bytes for the hash, groups of 4 values have the same hash.
	return uint16(d & byte(0x3F))
}

const (
	op_clear byte = iota
	op_put
	op_remove

	// not really an op, must be last
	num_ops
)

// command is the interface of all commands that can be triggered by the fuzzer.
type command interface {
	apply(trg *FastMap[byte, byte], ref *map[byte]byte)
	appendTo([]byte) []byte
}

// clear is the command clearing the maps content.
type clear struct{}

func (c clear) apply(trg *FastMap[byte, byte], ref *map[byte]byte) {
	trg.Clear()
	*ref = map[byte]byte{}
}

func (c clear) appendTo(code []byte) []byte {
	return append(code, op_clear)
}

// put is a command adding a key/value pair to the map.
type put struct {
	key, value byte
}

func (c put) apply(trg *FastMap[byte, byte], ref *map[byte]byte) {
	trg.Put(c.key, c.value)
	(*ref)[c.key] = c.value
}

func (c put) appendTo(code []byte) []byte {
	return append(code, []byte{op_put, c.key, c.value}...)
}

// remove eliminates a key from the map.
type remove struct {
	key byte
}

func (c remove) apply(trg *FastMap[byte, byte], ref *map[byte]byte) {
	trg.Remove(c.key)
	delete(*ref, c.key)
}

func (c remove) appendTo(code []byte) []byte {
	return append(code, []byte{op_remove, c.key}...)
}

// parseCommands interprets the given byte sequence as a sequence of commands.
func parseCommands(encoded []byte) []command {
	res := []command{}
	for len(encoded) > 1 {
		switch encoded[0] % num_ops {
		case op_clear:
			res = append(res, clear{})
			encoded = encoded[1:]
		case op_put:
			if len(encoded) < 3 {
				return res
			}
			res = append(res, put{encoded[1], encoded[2]})
			encoded = encoded[3:]
		case op_remove:
			if len(encoded) < 2 {
				return res
			}
			res = append(res, remove{encoded[1]})
			encoded = encoded[2:]
		}
	}
	return res
}

// toBytes encodes a list of commands into a byte sequence.
func toBytes(commands []command) []byte {
	res := []byte{}
	for _, cmd := range commands {
		res = cmd.appendTo(res)
	}
	return res
}

func FuzzMapOperations(f *testing.F) {
	// the no-op case.
	f.Add(toBytes([]command{}))

	// a case for each command
	f.Add(toBytes([]command{clear{}}))
	f.Add(toBytes([]command{put{2, 3}}))
	f.Add(toBytes([]command{remove{3}}))

	// a combined case
	f.Add(toBytes([]command{put{2, 3}, remove{2}, put{2, 4}, clear{}}))

	// Test a full map
	cmds := make([]command, 0, 256)
	for i := 0; i < 256; i++ {
		cmds = append(cmds, put{byte(i), ^byte(i)})
	}
	f.Add(toBytes(cmds))

	// A case where all elements are added and removed.
	cmds = cmds[0:0]
	for i := 0; i < 256; i++ {
		cmds = append(cmds, put{byte(i), ^byte(i)})
	}
	for i := 0; i < 256; i++ {
		cmds = append(cmds, remove{byte(i)})
	}
	f.Add(toBytes(cmds))

	f.Fuzz(func(t *testing.T, data []byte) {
		cmds := parseCommands(data)
		trg := NewFastMap[byte, byte](testByteHasher{})
		ref := map[byte]byte{}
		for _, cmd := range cmds {

			// Apply the next operation.
			cmd.apply(trg, &ref)

			// Check that the test map and the reference map are identical.
			want_size := len(ref)
			have_size := trg.Size()
			if want_size != have_size {
				t.Errorf("invalid number of elements in map, wanted %d, got %d", want_size, have_size)
				return
			}

			for i := 0; i < 256; i++ {
				want_value, want_exists := ref[byte(i)]
				have_value, have_exists := trg.Get(byte(i))
				if want_exists != have_exists {
					t.Errorf("existence of %d wrong, wanted %v, got %v", i, want_exists, have_exists)
					return
				}
				if want_exists && want_value != have_value {
					t.Errorf("value of %d wrong, wanted %v, got %v", i, want_value, have_value)
					return
				}
			}
		}
	})
}

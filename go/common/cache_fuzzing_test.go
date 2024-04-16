//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

package common

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/fuzzing"
	"testing"
)

// This fuzzer triggers a sequence of random operations to cache implementations.
// It randomly generates executions against the cache interface using randomly
// generated keys and values.
// It uses a shadow data structure, which is a map, that mimics the same operations.
// The current cache state is compared to the shadow structure to verify its state is correct.
// The eviction policy of the cache is mimicked to the shadow structure as implemented by the cache,
// i.e., it is not checked whether the eviction policy is correct.

func FuzzLruCache_RandomOps(f *testing.F) {
	fn := func() Cache[int8, int16] {
		return NewLruCache[int8, int16](128)
	}
	fuzzing.Fuzz[cacheFuzzingContext](f, &cacheFuzzingCampaign{fn})
}

func FuzzNWays_RandomOps(f *testing.F) {
	fn := func() Cache[int8, int16] {
		return NewNWaysCache[int8, int16](128, 16)
	}
	fuzzing.Fuzz[cacheFuzzingContext](f, &cacheFuzzingCampaign{fn})
}

type cacheOpType byte

const (
	get cacheOpType = iota
	set
	getOrSet
	remove_
	iterate
	clear_
)

func (op cacheOpType) serialise() []byte {
	b := make([]byte, 1, 4) // 1 byte for optype + 1 + 2 bytes for key, val pair
	b[0] = byte(op)
	return b
}

type cacheFuzzingCampaign struct {
	initCache func() Cache[int8, int16] // factory to create cache instance.
}

func (c *cacheFuzzingCampaign) Init() []fuzzing.OperationSequence[cacheFuzzingContext] {
	setMany := make([]fuzzing.Operation[cacheFuzzingContext], 0, 255)
	for i := 0; i < 255; i++ {
		setMany = append(setMany, &opSet{int8(i), int16(i * 100)})
	}
	getMany := make([]fuzzing.Operation[cacheFuzzingContext], 0, 255)
	for i := 0; i < 255; i++ {
		getMany = append(getMany, &opGet{int8(i)})
	}

	// generate some adhoc sequences of operations
	data := []fuzzing.OperationSequence[cacheFuzzingContext]{
		{&opSet{0, 0}, &opGet{0}, &opRemove{0}},
		{&opSet{0, 0}, &opSet{1, 1}, &opSet{2, 2}, &opSet{5, 5}, &opSet{10, 10}, &opIterate{}},
		{&opSet{0, 0}, &opSet{1, 1}, &opSet{2, 2}, &opSet{5, 5}, &opSet{10, 10}, &opGet{10}, &opClear{}},
		{&opGet{10}, &opSet{10, 100}, &opRemove{10}},
		{&opGet{10}, &opSet{10, 100}, &opGet{10}, &opRemove{10}, &opGet{10}},
		{&opSet{0, 0}, &opSet{10, 10}, &opRemove{0}, &opGet{10}, &opGet{0}, &opClear{}, &opGet{10}},
		setMany,
		append(setMany, &opIterate{}),
		append(setMany, &opGet{120}),
		append(setMany, getMany...),
	}

	return data
}

func (c *cacheFuzzingCampaign) CreateContext(_ fuzzing.TestingT) *cacheFuzzingContext {
	cache := c.initCache()
	shadow := make(map[int8]int16)
	return &cacheFuzzingContext{cache, shadow}
}

func (c *cacheFuzzingCampaign) Deserialize(rawData []byte) []fuzzing.Operation[cacheFuzzingContext] {
	return parseOperations(rawData)
}

func (c *cacheFuzzingCampaign) Cleanup(fuzzing.TestingT, *cacheFuzzingContext) {
	// no clean-up
}

type cacheFuzzingContext struct {
	cache  Cache[int8, int16]
	shadow map[int8]int16
}

type opGet struct {
	key int8 // small address space to have key collisions
}

func (op *opGet) Serialize() []byte {
	b := get.serialise()
	b = append(b, byte(op.key))
	return b
}

func (op *opGet) Apply(t fuzzing.TestingT, c *cacheFuzzingContext) {
	val, exists := c.cache.Get(op.key)
	shadowValue, shadowExists := c.shadow[op.key]

	if exists != shadowExists {
		t.Errorf("tested and shadow cache diverged: %v != %v", exists, shadowExists)
	}

	if exists && val != shadowValue {
		t.Errorf("tested and shadow cache diverged: %v != %v", val, shadowValue)
	}
}

type opSet struct {
	key   int8
	value int16
}

func (op *opSet) Serialize() []byte {
	b := set.serialise()
	b = append(b, byte(op.key))
	b = binary.BigEndian.AppendUint16(b, uint16(op.value))
	return b
}

func (op *opSet) Apply(t fuzzing.TestingT, c *cacheFuzzingContext) {
	evictedKey, evictedValue, evicted := c.cache.Set(op.key, op.value)
	if evicted {
		shadowVal, shadowExists := c.shadow[evictedKey]
		if !shadowExists {
			t.Errorf("evicted key not found in shadow cache: %v", evictedKey)
		}
		if shadowVal != evictedValue {
			t.Errorf("evicted tested and shadow values diverged: %v != %v", shadowVal, evictedValue)
		}

		delete(c.shadow, evictedKey)
	}

	c.shadow[op.key] = op.value
}

type opGetOrSet struct {
	key   int8
	value int16
}

func (op *opGetOrSet) Serialize() []byte {
	b := getOrSet.serialise()
	b = append(b, byte(op.key))
	b = binary.BigEndian.AppendUint16(b, uint16(op.value))
	return b
}

func (op *opGetOrSet) Apply(t fuzzing.TestingT, c *cacheFuzzingContext) {
	val, exists, evictedKey, evictedValue, evicted := c.cache.GetOrSet(op.key, op.value)
	if evicted {
		shadowVal, shadowExists := c.shadow[evictedKey]
		if !shadowExists {
			t.Errorf("evicted key not found in shadow cache: %v", evictedKey)
		}
		if shadowVal != evictedValue {
			t.Errorf("evicted tested and shadow values diverged: %v != %v", shadowVal, evictedValue)
		}

		delete(c.shadow, evictedKey)
	}

	shadowValue, shadowExists := c.shadow[op.key]
	if exists != shadowExists {
		t.Errorf("existence of key does not match witch shadow: %v != %v", shadowExists, exists)
	}

	if exists {
		if val != shadowValue {
			t.Errorf("tested and shadow cache diverged: %v != %v", val, shadowValue)
		}
	} else {
		c.shadow[op.key] = op.value
	}
}

type opRemove struct {
	key int8
}

func (op *opRemove) Serialize() []byte {
	b := remove_.serialise()
	b = append(b, byte(op.key))
	return b
}

func (op *opRemove) Apply(t fuzzing.TestingT, c *cacheFuzzingContext) {
	val, exists := c.cache.Remove(op.key)
	shadowValue, shadowExists := c.shadow[op.key]

	if exists != shadowExists {
		t.Errorf("existence of key does not match witch shadow: %v != %v", shadowExists, exists)
	}

	if exists && val != shadowValue {
		t.Errorf("tested and shadow cache diverged: %v != %v", val, shadowValue)
	}
	delete(c.shadow, op.key)
}

type opIterate struct {
}

func (op *opIterate) Serialize() []byte {
	return iterate.serialise()
}

func (op *opIterate) Apply(t fuzzing.TestingT, c *cacheFuzzingContext) {
	shadowCopy := make(map[int8]int16, len(c.shadow))
	c.cache.Iterate(func(key int8, val int16) bool {
		shadowVal, shadowExists := c.shadow[key]
		if !shadowExists {
			t.Errorf("key is not present in shadow cache: %v", key)
		}
		if shadowVal != val {
			t.Errorf("tested and shadow cache diverged: %v != %v", val, shadowVal)
		}

		if _, exists := shadowCopy[key]; exists {
			t.Errorf("key visited twice througput the iteration: %d", key)
		}

		shadowCopy[key] = val
		return true
	})

	if got, want := len(shadowCopy), len(c.shadow); got != want {
		t.Errorf("number of iterated keys does not match the size of shadow cache: %d != %d", got, want)
	}
}

type opClear struct {
}

func (op *opClear) Serialize() []byte {
	return clear_.serialise()
}

func (op *opClear) Apply(_ fuzzing.TestingT, c *cacheFuzzingContext) {
	c.cache.Clear()
	c.shadow = map[int8]int16{}
}

// parseOperations converts the input byte array
// to the list of operations.
// It is converted from the format: <opType>[<key><value>]
// This method tries to parse as many of those tuples as possible, terminating when no more
// elements are available.
// The key is expected in the stream only for operations get and remove_.
// The key, value pair is expected only for operations set and getOrSet.
// For other operations, next opType is immediately followed.
func parseOperations(b []byte) []fuzzing.Operation[cacheFuzzingContext] {
	var ops []fuzzing.Operation[cacheFuzzingContext]
	for len(b) >= 1 {
		opType := cacheOpType(b[0] % 6)
		b = b[1:]

		var key int8
		var val int16
		if opType == set || opType == getOrSet {
			if len(b) >= 3 {
				key = int8(b[0])
				val = int16(binary.BigEndian.Uint16(b[1:3]))
				b = b[3:]
			} else {
				return ops
			}
		}
		if opType == get || opType == remove_ {
			if len(b) >= 1 {
				key = int8(b[0])
				b = b[1:]
			} else {
				return ops
			}
		}

		var op fuzzing.Operation[cacheFuzzingContext]
		switch opType {
		case get:
			op = &opGet{key}
		case set:
			op = &opSet{key, val}
		case getOrSet:
			op = &opGetOrSet{key, val}
		case remove_:
			op = &opRemove{key}
		case iterate:
			op = &opIterate{}
		case clear_:
			op = &opIterate{}
		}
		ops = append(ops, op)
	}

	return ops
}

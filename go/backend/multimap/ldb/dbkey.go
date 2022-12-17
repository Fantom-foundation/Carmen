package ldb

import "github.com/Fantom-foundation/Carmen/go/common"

const KeySize = 8
const ValueSize = 8

type DbKey[K any, V any] [1 + KeySize + ValueSize]byte

func (k *DbKey[K, V]) SetTableKey(table common.TableSpace, key K, keySerializer common.Serializer[K]) {
	k[0] = byte(table)
	keySerializer.CopyBytes(key, k[1:1+KeySize])
}

func (k *DbKey[K, V]) SetValue(value V, valueSerializer common.Serializer[V]) {
	valueSerializer.CopyBytes(value, k[1+KeySize:])
}

func (k *DbKey[K, V]) SetMaxValue() {
	for i := 1 + KeySize; i < 1+KeySize+ValueSize; i++ {
		k[i] = 0xFF
	}
}

func (k *DbKey[K, V]) CopyFrom(source *DbKey[K, V]) {
	copy(k[:], source[:])
}

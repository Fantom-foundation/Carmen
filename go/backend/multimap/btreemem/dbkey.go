package btreemem

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// dbKey is used as a key to the BTree backed Multimap.
// It concatenates Key-Value pairs into one database (BTree) key.
type dbKey[K any, V any] struct {
	key            K
	value          V
	maxVal, minVal bool // shortcuts for comparator of [K,V] with max value [K,0xF...F] or min [K,0x0...0]
}

// newDbKey creates a new key concatenating input key-value pair.
func newDbKey[K any, V any](k K, v V) dbKey[K, V] {
	return dbKey[K, V]{k, v, false, false}
}

// newDbKeyMaxVal creates a new key where the input key is used
// and the value is assumed as a maximal value from the address space
// i.e. dbKey := [key][0xFF..FF]
func newDbKeyMaxVal[K any, V any](k K) dbKey[K, V] {
	return dbKey[K, V]{key: k, maxVal: true, minVal: false}
}

// newDbKeyMinVal creates a new key where the input key is used
// and the value is assumed as a minimal value from the address space
// i.e. dbKey := [key][0x00..00]
func newDbKeyMinVal[K any, V any](k K) dbKey[K, V] {
	return dbKey[K, V]{key: k, maxVal: false, minVal: true}
}

func (c dbKey[K, V]) String() string {
	return fmt.Sprintf("%v_%v", c.key, c.value)
}

// dbKeyComparator comparator of DB key, which is composed of a Key-Value pair
type dbKeyComparator[K any, V any] struct {
	keyComparator   common.Comparator[K]
	valueComparator common.Comparator[V]
}

func (c dbKeyComparator[K, V]) Compare(a, b *dbKey[K, V]) int {
	res := c.keyComparator.Compare(&a.key, &b.key)
	if res == 0 {
		if b.minVal || a.maxVal {
			return +1
		}
		if a.minVal || b.maxVal {
			return -1
		}
		res = c.valueComparator.Compare(&a.value, &b.value)
	}

	return res
}

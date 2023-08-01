package cache

import (
	"github.com/Fantom-foundation/Carmen/go/backend/index/file"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/common"
)

var (
	address1 = common.Address{0x01}
	address2 = common.Address{0x02}
	address3 = common.Address{0x03}
	address4 = common.Address{0x04}
)

func TestIndexCacheFilled(t *testing.T) {
	path := t.TempDir()
	wrapped, err := file.NewIndex[common.Address, uint32](path, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressHasher{}, common.AddressComparator{})
	if err != nil {
		t.Fatalf("cannot create index: %s", err)
	}
	index := NewIndex[common.Address, uint32](wrapped, 3)

	_, _ = index.GetOrAdd(address1)
	val, exists := index.cache.Get(address1)
	if !exists || val != 0 {
		t.Errorf("Value is not propagated in cahce")
	}
}

func TestIndexCacheEviction(t *testing.T) {
	path := t.TempDir()
	wrapped, err := file.NewIndex[common.Address, uint32](path, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressHasher{}, common.AddressComparator{})
	if err != nil {
		t.Fatalf("cannot create index: %s", err)
	}
	index := NewIndex[common.Address, uint32](wrapped, 3)

	_, _ = index.GetOrAdd(address1)
	_, _ = index.GetOrAdd(address2)
	_, _ = index.GetOrAdd(address3)
	_, _ = index.GetOrAdd(address4)

	// fist item evicted from cache
	_, exists := index.cache.Get(address1)
	if exists {
		t.Errorf("Value is not evicted from cahce")
	}

	// it returns value in cache
	_ = index.Contains(address1)
	val, exists := index.cache.Get(address1)
	if !exists || val != 0 {
		t.Errorf("Value is not in cahce")
	}
}

var ErrNotFound = index.ErrNotFound

func TestNonExistingValuesAreNotCached(t *testing.T) {
	path := t.TempDir()
	wrapped, err := file.NewIndex[common.Address, uint32](path, common.AddressSerializer{}, common.Identifier32Serializer{}, common.AddressHasher{}, common.AddressComparator{})
	if err != nil {
		t.Fatalf("cannot create index: %s", err)
	}
	index := NewIndex[common.Address, uint32](wrapped, 3)
	_, err = index.Get(address1)
	if err != ErrNotFound {
		t.Errorf("Address 1 should not exist")
	}
	_, err = index.Get(address1)
	if err != ErrNotFound {
		t.Errorf("Address 1 should still not exist")
	}
}

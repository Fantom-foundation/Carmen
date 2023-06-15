package cache

import (
	"errors"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/common"
)

const defaultCacheCapacity = 1_000_000

type cachedStock[I stock.Index, V any] struct {
	underlying stock.Stock[I, V]
	cache      *common.Cache[I, *V]
}

func CreateCachedStock[I stock.Index, V any](stock stock.Stock[I, V]) stock.Stock[I, V] {
	return &cachedStock[I, V]{
		underlying: stock,
		cache:      common.NewCache[I, *V](defaultCacheCapacity),
	}
}

func (s *cachedStock[I, V]) New() (I, *V, error) {
	i, v, err := s.underlying.New()
	if err != nil {
		return 0, nil, err
	}
	s.cache.Set(i, v)
	return i, v, nil
}

func (s *cachedStock[I, V]) Get(index I) (*V, error) {
	// check the cache first
	if value, exists := s.cache.Get(index); exists {
		return value, nil
	}

	// Fetch it from the underlying implementation.
	ptr, err := s.underlying.Get(index)
	if err != nil {
		return nil, err
	}

	// Since the pointer may be invalidated with the next operation,
	// we need to to get a cache-owned copy of the data.
	value := new(V)
	*value = *ptr
	if evictedId, evictedKey, evicted := s.cache.Set(index, value); evicted {
		if err := s.underlying.Set(evictedId, evictedKey); err != nil {
			return nil, err
		}
	}
	return value, nil
}

func (s *cachedStock[I, V]) Set(index I, value *V) error {
	// Just update the cache, recording the value by pointer.
	if evictedId, evictedKey, evicted := s.cache.Set(index, value); evicted {
		if err := s.underlying.Set(evictedId, evictedKey); err != nil {
			return err
		}
	}
	return nil
}

func (s *cachedStock[I, V]) Delete(index I) error {
	if err := s.underlying.Delete(index); err != nil {
		return err
	}
	s.cache.Remove(index)
	return nil
}

func (s *cachedStock[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	var value V
	res := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	res.AddChild("underlying", s.underlying.GetMemoryFootprint())
	res.AddChild("cache", s.cache.GetMemoryFootprint(unsafe.Sizeof(value)*unsafe.Sizeof(&value))) // assuming V is not a complex object
	return res
}

func (s *cachedStock[I, V]) Flush() error {
	// Flush out all cached values. We do not track dirty values,
	// so all values need to be flushed.
	errs := []error{}
	s.cache.Iterate(func(index I, value *V) bool {
		if err := s.underlying.Set(index, value); err != nil {
			errs = append(errs, err)
		}
		return true
	})
	// Flush underlying storage.
	return errors.Join(
		errors.Join(errs...),
		s.underlying.Flush(),
	)
}

func (s *cachedStock[I, V]) Close() error {
	return errors.Join(
		s.Flush(),
		s.underlying.Close(),
	)
}

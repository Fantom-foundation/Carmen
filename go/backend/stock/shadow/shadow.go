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

package shadow

import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// shadowStock is a debug utility to run two Stock implementations in parallel and compare
// its behavior. Any deriviation will result in a panic.
type shadowStock[I stock.Index, V comparable] struct {
	primary, secondary stock.Stock[I, V]
}

func MakeShadowStock[I stock.Index, V comparable](primary, secondary stock.Stock[I, V]) stock.Stock[I, V] {
	return &shadowStock[I, V]{
		primary:   primary,
		secondary: secondary,
	}
}

func (s *shadowStock[I, V]) New() (I, error) {
	i, errA := s.primary.New()
	j, errB := s.secondary.New()
	if errA != nil || errB != nil {
		return 0, errors.Join(errA, errB)
	}
	if i != j {
		fmt.Printf("New result invalid\nwant: %v\n got: %v", j, i)
		panic("failed")
	}
	return i, nil
}

func (s *shadowStock[I, V]) Get(index I) (V, error) {
	a, errA := s.primary.Get(index)
	b, errB := s.secondary.Get(index)
	if errA != nil || errB != nil {
		return a, errors.Join(errA, errB)
	}
	if a != b {
		fmt.Printf("Retrieved for index %v:\nwant: %v\n got: %v\n", index, b, a)
		panic("failed")
	}
	return a, nil
}

func (s *shadowStock[I, V]) Set(index I, value V) error {
	return errors.Join(
		s.primary.Set(index, value),
		s.secondary.Set(index, value),
	)
}

func (s *shadowStock[I, V]) Delete(index I) error {
	return errors.Join(
		s.primary.Delete(index),
		s.secondary.Delete(index),
	)
}

func (s *shadowStock[I, V]) GetIds() (stock.IndexSet[I], error) {
	return s.primary.GetIds()
}

func (s *shadowStock[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	res := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	res.AddChild("primary", s.primary.GetMemoryFootprint())
	res.AddChild("secondary", s.secondary.GetMemoryFootprint())
	return res
}

func (s *shadowStock[I, V]) Flush() error {
	return errors.Join(
		s.primary.Flush(),
		s.secondary.Flush(),
	)
}

func (s *shadowStock[I, V]) Close() error {
	return errors.Join(
		s.primary.Close(),
		s.secondary.Close(),
	)
}

// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package synced

import (
	"sync"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/utils/checkpoint"
	"github.com/Fantom-foundation/Carmen/go/common"
)

type syncedStock[I stock.Index, V any] struct {
	mu     sync.Mutex
	nested stock.Stock[I, V]
}

// Sync wraps the provided stock into a synchronizing wrapper making sure that
// at any time only one operation is performed on the given stock.
func Sync[I stock.Index, V any](stock stock.Stock[I, V]) stock.Stock[I, V] {
	if res, ok := stock.(*syncedStock[I, V]); ok {
		return res
	}
	return &syncedStock[I, V]{nested: stock}
}

func (s *syncedStock[I, V]) New() (I, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nested.New()
}

func (s *syncedStock[I, V]) Get(index I) (V, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nested.Get(index)
}

func (s *syncedStock[I, V]) Set(index I, value V) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nested.Set(index, value)
}

func (s *syncedStock[I, V]) Delete(index I) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nested.Delete(index)
}

func (s *syncedStock[I, V]) GetIds() (stock.IndexSet[I], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nested.GetIds()
}

func (s *syncedStock[I, V]) GetMemoryFootprint() *common.MemoryFootprint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nested.GetMemoryFootprint()
}

func (s *syncedStock[I, V]) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nested.Flush()
}

func (s *syncedStock[I, V]) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nested.Close()
}

func (s *syncedStock[I, V]) GuaranteeCheckpoint(checkpoint checkpoint.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nested.GuaranteeCheckpoint(checkpoint)
}

func (s *syncedStock[I, V]) Prepare(checkpoint checkpoint.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nested.Prepare(checkpoint)
}

func (s *syncedStock[I, V]) Commit(checkpoint checkpoint.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nested.Commit(checkpoint)
}

func (s *syncedStock[I, V]) Abort(checkpoint checkpoint.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nested.Abort(checkpoint)
}

func (s *syncedStock[I, V]) Restore(checkpoint checkpoint.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nested.Restore(checkpoint)
}

//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public Licence v3.
//

package multimap

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/backend/multimap/btreemem"
	"github.com/Fantom-foundation/Carmen/go/backend/multimap/ldb"
	"github.com/Fantom-foundation/Carmen/go/backend/multimap/memory"
	"github.com/Fantom-foundation/Carmen/go/common"
	"sort"
	"testing"
)

type multimapFactory struct {
	label       string
	getMultiMap func(tempDir string) MultiMap[uint32, uint64]
}

func getMultiMapFactories(tb testing.TB) (stores []multimapFactory) {
	return []multimapFactory{
		{
			label: "Memory",
			getMultiMap: func(tempDir string) MultiMap[uint32, uint64] {
				return memory.NewMultiMap[uint32, uint64]()
			},
		},
		{
			label: "LevelDb",
			getMultiMap: func(tempDir string) MultiMap[uint32, uint64] {
				db, err := backend.OpenLevelDb(tempDir, nil)
				if err != nil {
					tb.Fatalf("failed to init leveldb store; %s", err)
				}

				mm := ldb.NewMultiMap[uint32, uint64](db, backend.AddressSlotMultiMapKey, common.Identifier32Serializer{}, common.Identifier64Serializer{})
				if err != nil {
					tb.Fatalf("failed to init leveldb store; %s", err)
				}
				return &multiMapClosingWrapper{mm, []func() error{db.Close}}
			},
		},
		{
			label: "BTree",
			getMultiMap: func(tempDir string) MultiMap[uint32, uint64] {
				return btreemem.NewMultiMap[uint32, uint64](common.Identifier32Serializer{}, common.Identifier64Serializer{}, common.Uint32Comparator{}, common.Uint64Comparator{})
			},
		},
	}
}

// multiMapClosingWrapper wraps an instance of the MultiMap to clean-up related resources when the MultiMap is closed
type multiMapClosingWrapper struct {
	MultiMap[uint32, uint64]
	cleanups []func() error
}

// Close executes clean-up
func (s *multiMapClosingWrapper) Close() error {
	for _, f := range s.cleanups {
		_ = f()
	}
	return s.MultiMap.Close()
}

func TestMultiMapAdd(t *testing.T) {
	for _, factory := range getMultiMapFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			m := factory.getMultiMap(t.TempDir())
			defer m.Close()

			if err := m.Add(1, 11); err != nil {
				t.Fatalf("failed to add into multimap")
			}
			if err := m.Add(1, 22); err != nil {
				t.Fatalf("failed to add into multimap")
			}
			if err := m.Add(2, 33); err != nil {
				t.Fatalf("failed to add into multimap")
			}
			if err := m.Add(3, 44); err != nil {
				t.Fatalf("failed to add into multimap")
			}
			if err := m.Add(2, 55); err != nil {
				t.Fatalf("failed to add into multimap")
			}

			if err := assertValues(m, 1, []uint64{11, 22}); err != nil {
				t.Errorf("unexpected key 1 values")
			}
			if err := assertValues(m, 2, []uint64{33, 55}); err != nil {
				t.Errorf("unexpected key 2 values")
			}
			if err := assertValues(m, 3, []uint64{44}); err != nil {
				t.Errorf("unexpected key 3 values")
			}
			if err := assertValues(m, 9, []uint64{}); err != nil {
				t.Errorf("unexpected not-existing key 9 values")
			}
		})
	}
}

func TestMultiMapRemove(t *testing.T) {
	for _, factory := range getMultiMapFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			m := factory.getMultiMap(t.TempDir())
			defer m.Close()

			if err := m.Add(1, 11); err != nil {
				t.Fatalf("failed to add into multimap")
			}
			if err := m.Add(1, 22); err != nil {
				t.Fatalf("failed to add into multimap")
			}
			if err := m.Add(1, 33); err != nil {
				t.Fatalf("failed to add into multimap")
			}
			if err := m.Remove(1, 22); err != nil {
				t.Fatalf("failed to remove from multimap")
			}
			if err := m.Remove(1, 99); err != nil {
				t.Fatalf("failed to remove from multimap")
			}

			if err := assertValues(m, 1, []uint64{11, 33}); err != nil {
				t.Errorf("unexpected key 1 values")
			}
		})
	}
}

func TestMultiMapRemoveAll(t *testing.T) {
	for _, factory := range getMultiMapFactories(t) {
		t.Run(factory.label, func(t *testing.T) {
			m := factory.getMultiMap(t.TempDir())
			defer m.Close()

			if err := m.Add(1, 11); err != nil {
				t.Fatalf("failed to add into multimap")
			}
			if err := m.Add(1, 22); err != nil {
				t.Fatalf("failed to add into multimap")
			}
			if err := m.Add(2, 33); err != nil {
				t.Fatalf("failed to add into multimap")
			}
			if err := m.RemoveAll(1); err != nil {
				t.Fatalf("failed to remove from multimap")
			}

			if err := assertValues(m, 1, []uint64{}); err != nil {
				t.Errorf("unexpected key 1 values")
			}
			if err := assertValues(m, 2, []uint64{33}); err != nil {
				t.Errorf("unexpected key 2 values")
			}
		})
	}
}

func assertValues(m MultiMap[uint32, uint64], key uint32, expectedValues []uint64) error {
	content, err := m.GetAll(key)
	if err != nil {
		return err
	}
	sort.Slice(content, func(i, j int) bool { return content[i] < content[j] })
	sort.Slice(expectedValues, func(i, j int) bool { return expectedValues[i] < expectedValues[j] })
	if len(content) != len(expectedValues) {
		return fmt.Errorf("assertValues failed")
	}
	for i := 0; i < len(content); i++ {
		if content[i] != expectedValues[i] {
			return fmt.Errorf("assertValues failed")
		}
	}
	return nil
}

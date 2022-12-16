package multimap

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/multimap/memory"
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
	}
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
	content := make([]uint64, 0)
	if err := m.ForEach(key, func(x uint64) { content = append(content, x) }); err != nil {
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

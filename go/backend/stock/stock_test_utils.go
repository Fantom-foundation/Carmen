// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package stock

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/utils/checkpoint"
)

type IntEncoder struct{}

func (IntEncoder) GetEncodedSize() int {
	return 4
}

func (IntEncoder) Load(src []byte, value *int) error {
	*value = int(binary.BigEndian.Uint32(src))
	return nil
}

func (IntEncoder) Store(trg []byte, value *int) error {
	binary.BigEndian.PutUint32(trg, uint32(*value))
	return nil
}

type NamedStockFactory struct {
	ImplementationName string
	Open               func(t *testing.T, directory string) (Stock[int, int], error)
}

// RunStockTests runs a set of black-box unit test against a generic Stock
// implementation defined by the given factory. It is intended to be used
// in implementation specific unit test packages to cover basic compliance
// properties as imposed by the Stock interface.
func RunStockTests(t *testing.T, factory NamedStockFactory) {
	wrap := func(test func(*testing.T, NamedStockFactory)) func(*testing.T) {
		return func(t *testing.T) {
			t.Parallel()
			test(t, factory)
		}
	}
	t.Run("NewCreatesFreshIndexValues", wrap(testNewCreatesFreshIndexValues))
	t.Run("LookUpsRetrieveTheSameValue", wrap(testLookUpsRetrieveTheSameValue))
	t.Run("DeletedElementsAreReused", wrap(testDeletedElementsAreReused))
	t.Run("ReusedElementsAreCleared", wrap(testReusedElementsAreCleared))
	t.Run("LargeNumberOfElements", wrap(testLargeNumberOfElements))
	t.Run("ProvidesMemoryFootprint", wrap(testProvidesMemoryFootprint))
	t.Run("CreatesMissingDirectories", wrap(testCreatesMissingDirectories))
	t.Run("CanBeFlushed", wrap(testCanBeFlushed))
	t.Run("CanBeClosed", wrap(testCanBeClosed))
	t.Run("CanBeClosedAndReopened", wrap(testCanBeClosedAndReopened))
	t.Run("GetIdsProducesAllIdsInTheStock", wrap(testGetIdsProducesAllIdsInTheStock))
	t.Run("GetDeleteIndexOutOfRange", wrap(testDeleteIndexOutOfRange))
	t.Run("CanCreateCheckpoint", wrap(testCanCreateCheckpoint))
	t.Run("CanBeCommittedAndSealed", wrap(testCanBeCommittedAndSealed))
	t.Run("CheckpointCreationCanBeAborted", wrap(testCheckpointCreationCanBeAborted))
	t.Run("CheckpointIsPersisted", wrap(testCheckpointIsPersisted))
	t.Run("StockElementsIncludedInCheckpointArePersisted", wrap(testStockElementsIncludedInCheckpointArePersisted))
	t.Run("CheckpointCanBeRecoverFromCrashAfterPrepare", wrap(testCheckpointCanBeRecoverFromCrashAfterPrepare))
	t.Run("MissingCheckpointIsDetected", wrap(testMissingCheckpointIsDetected))
	t.Run("CheckpointPrepareFailsOnWrongCheckpoint", wrap(testCheckpointPrepareFailsOnWrongCheckpoint))
	t.Run("CheckpointCommitFailsOnWrongCheckpoint", wrap(testCheckpointCommitFailsOnWrongCheckpoint))
	t.Run("CheckpointAbortFailsOnWrongCheckpoint", wrap(testCheckpointAbortFailsOnWrongCheckpoint))
}

func testNewCreatesFreshIndexValues(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()
	index1, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new element: %v", err)
	}

	index2, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new element: %v", err)
	}
	if index1 == index2 {
		t.Errorf("Expected different index values, got %v and %v", index1, index2)
	}
}

func testLookUpsRetrieveTheSameValue(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()
	index1, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new element: %v", err)
	}
	if err := stock.Set(index1, 1); err != nil {
		t.Fatalf("failed to update value for index 1: %v", err)
	}

	index2, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new element: %v", err)
	}
	if err := stock.Set(index2, 2); err != nil {
		t.Fatalf("failed to update value for index 2: %v", err)
	}

	got, err := stock.Get(index1)
	if err != nil {
		t.Errorf("failed to obtain value for index %d: got %v, with err %v", index1, got, err)
	}
	if got != 1 {
		t.Errorf("failed to obtain value for index %d: got %d, wanted %d", index1, got, 1)
	}

	got, err = stock.Get(index2)
	if err != nil {
		t.Errorf("failed to obtain value for index %d: got %v, with err %v", index2, got, err)
	}
	if got != 2 {
		t.Errorf("failed to obtain value for index %d: got %d, wanted %d", index2, got, 2)
	}
}

func testDeletedElementsAreReused(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()

	seen := map[int]bool{}
	for i := 0; i < 1_000_000; i++ {
		index, err := stock.New()
		if err != nil {
			t.Fatalf("failed to create new element: %v", err)
		}
		if _, exists := seen[index]; exists {
			return
		}
		seen[index] = true
		if err := stock.Delete(index); err != nil {
			t.Fatalf("failed to delete element with key %v: %v", index, err)
		}
	}
	t.Errorf("stock failed to reuse released index key")
}

func testReusedElementsAreCleared(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()

	seen := map[int]bool{}
	for i := 0; i < 1_000_000; i++ {
		index, err := stock.New()
		if err != nil {
			t.Fatalf("failed to create new element: %v", err)
		}
		if err := stock.Set(index, 52); err != nil {
			t.Fatalf("failed to udpate value for index %d: %v", index, err)
		}
		if _, exists := seen[index]; exists {
			return
		}
		seen[index] = true
		if err := stock.Delete(index); err != nil {
			t.Fatalf("failed to delete element with key %v: %v", index, err)
		}
	}
	t.Errorf("stock failed to reuse released index key")
}

func testLargeNumberOfElements(t *testing.T, factory NamedStockFactory) {
	const N = 100_000
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()
	indexes := map[int]int{}
	for i := 0; i < N; i++ {
		index, err := stock.New()
		if err != nil {
			t.Fatalf("failed to create new entry: %v", err)
		}
		indexes[i] = index
		if err := stock.Set(i, i); err != nil {
			t.Fatalf("failed to update value of element with index %d: %v", index, err)
		}
	}

	for i := 0; i < N; i++ {
		got, err := stock.Get(indexes[i])
		if err != nil {
			t.Fatalf("failed to locate element: %v", err)
		}
		if got != i {
			t.Errorf("invalid value mapped to index %d: wanted %d, got %d", indexes[i], i, got)
		}
	}
}

func testProvidesMemoryFootprint(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()
	if _, err := stock.New(); err != nil {
		t.Fatalf("failed to insert single element into empty stock: %v", err)
	}
	footprint := stock.GetMemoryFootprint()
	if footprint == nil {
		t.Fatalf("implementation does not provide memory footprint data")
	}
	if footprint.Total() <= 0 {
		t.Fatalf("implementations claims zero memory footprint")
	}
}

func testCreatesMissingDirectories(t *testing.T, factory NamedStockFactory) {
	directory := t.TempDir() + "/some/missing/directory"
	stock, err := factory.Open(t, directory)
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()
	if _, err := os.Stat(directory); err != nil {
		t.Errorf("failed to create output directory: %v", err)
	}
}

func testCanBeFlushed(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()
	if err := stock.Flush(); err != nil {
		t.Fatalf("failed to flush empty stock: %v", err)
	}
	if _, err := stock.New(); err != nil {
		t.Fatalf("failed to insert single element into empty stock: %v", err)
	}
	if err := stock.Flush(); err != nil {
		t.Fatalf("failed to flush non-empty stock: %v", err)
	}
}

func testCanBeClosed(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()
	if _, err := stock.New(); err != nil {
		t.Fatalf("failed to insert single element into empty stock: %v", err)
	}
	if err := stock.Close(); err != nil {
		t.Fatalf("failed to close non-empty stock: %v", err)
	}
}

func testCanBeClosedAndReopened(t *testing.T, factory NamedStockFactory) {
	dir := t.TempDir()
	stock, err := factory.Open(t, dir)
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()

	// The first element shall be a deleted element.
	key1, err := stock.New()
	if err != nil {
		t.Fatalf("failed to insert single element into empty stock: %v", err)
	}

	// The second element is an element with a value.
	key2, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new element in stock: %v", err)
	}
	if err := stock.Set(key2, 123); err != nil {
		t.Fatalf("failed to update value: %v", err)
	}

	// The third element is a default-value.
	key3, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new element in stock: %v", err)
	}

	if err := stock.Delete(key1); err != nil {
		t.Fatalf("failed to delete key from stock: %v", err)
	}
	if err := stock.Close(); err != nil {
		t.Fatalf("failed to close non-empty stock: %v", err)
	}
	stock = nil

	// After re-opening the stock all the information should be present.
	stock, err = factory.Open(t, dir)
	if err != nil {
		t.Fatalf("failed to reopen stock: %v", err)
	}

	got, err := stock.Get(key2)
	if err != nil {
		t.Fatalf("failed to read value from reopened stock: %v", err)
	}
	if got != 123 {
		t.Fatalf("invalid value read from reopened stock: got %v, wanted 123", got)
	}

	got, err = stock.Get(key3)
	if err != nil {
		t.Fatalf("failed to read value from reopened stock: %v", err)
	}
	if got != 0 {
		t.Fatalf("invalid value read from reopened stock: got %v, wanted 0", got)
	}

	keyX, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create new entry in reopened stock: %v", err)
	}
	if keyX != key1 {
		t.Errorf("expected key reuse, wanted %d, got %d", key1, keyX)
	}
}

func testGetIdsProducesAllIdsInTheStock(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()

	const N = 100
	ids := map[int]struct{}{}
	for i := 0; i < N; i++ {
		i, err := stock.New()
		if err != nil {
			t.Fatalf("failed to insert single element into empty stock: %v", err)
		}
		ids[i] = struct{}{}
	}

	for len(ids) > 0 {
		set, err := stock.GetIds()
		if err != nil {
			t.Fatalf("failed to produce an index set: %v", err)
		}

		// Check that all IDs are in the index set.
		for id := range ids {
			if !set.Contains(id) {
				t.Errorf("Id set does not contain valid ID %v", id)
			}
		}

		// The set does not contain extra elements.
		for i := set.GetLowerBound() - 10; i <= set.GetUpperBound()+10; i++ {
			got := set.Contains(i)
			_, want := ids[i]
			if got != want {
				t.Fatalf("unexpected membership of %d, wanted %t, got %t", i, want, got)
			}
		}

		// Remove a random element from the IDs.
		for i := range ids {
			delete(ids, i)
			if err := stock.Delete(i); err != nil {
				t.Fatalf("failed to delete an element from the set: %v", err)
			}
			break
		}
	}
}

func testDeleteIndexOutOfRange(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create empty stock: %v", err)
	}
	defer stock.Close()

	if err := stock.Delete(-1); err != nil {
		t.Errorf("deleting negative index should be no-op")
	}

	if err := stock.Delete(1); err != nil {
		t.Errorf("deleting index above range should be no-op")
	}
}

func testCanCreateCheckpoint(t *testing.T, factory NamedStockFactory) {
	commitDir := t.TempDir()
	stockDir := t.TempDir()

	stock, err := factory.Open(t, stockDir)
	if err != nil {
		t.Fatalf("failed to open stock: %v", err)
	}
	coordinator, err := checkpoint.NewCoordinator(commitDir, stock)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	if _, err := coordinator.CreateCheckpoint(); err != nil {
		t.Fatalf("failed to create checkpoint: %v", err)
	}

	if err := stock.Close(); err != nil {
		t.Fatalf("failed to close stock: %v", err)
	}
}

func testCanBeCommittedAndSealed(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Errorf("failed to open stock: %v", err)
	}

	id, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create item in stock: %v", err)
	}
	if err := stock.Set(id, 1); err != nil {
		t.Fatalf("failed to set value in stock: %v", err)
	}

	if err := stock.Prepare(checkpoint.Checkpoint(1)); err != nil {
		t.Fatalf("failed to prepare checkpoint: %v", err)
	}

	if err := stock.Commit(checkpoint.Checkpoint(1)); err != nil {
		t.Fatalf("failed to commit checkpoint: %v", err)
	}

	if err := stock.GuaranteeCheckpoint(checkpoint.Checkpoint(1)); err != nil {
		t.Fatalf("failed to check checkpoint: %v", err)
	}

	if err := stock.Set(id, 2); err == nil {
		t.Errorf("setting value should fail after the commit of a checkpoint")
	}
}

func testCheckpointCreationCanBeAborted(t *testing.T, factory NamedStockFactory) {
	stock, err := factory.Open(t, t.TempDir())
	if err != nil {
		t.Errorf("failed to open stock: %v", err)
	}

	id, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create item in stock: %v", err)
	}
	if err := stock.Set(id, 1); err != nil {
		t.Fatalf("failed to set value in stock: %v", err)
	}

	if err := stock.Prepare(checkpoint.Checkpoint(1)); err != nil {
		t.Fatalf("failed to prepare checkpoint: %v", err)
	}

	if err := stock.Abort(checkpoint.Checkpoint(1)); err != nil {
		t.Fatalf("failed to abort checkpoint creation: %v", err)
	}

	if err := stock.GuaranteeCheckpoint(checkpoint.Checkpoint(0)); err != nil {
		t.Fatalf("failed to check pre-existing checkpoint: %v", err)
	}

	if err := stock.Set(id, 2); err != nil {
		t.Errorf("as the checkpoint creation was aborted, setting value should succeed: %v", err)
	}
}

func testCheckpointIsPersisted(t *testing.T, factory NamedStockFactory) {
	dir := t.TempDir()
	stock, err := factory.Open(t, dir)
	if err != nil {
		t.Fatalf("failed to open stock: %v", err)
	}

	if err := stock.GuaranteeCheckpoint(checkpoint.Checkpoint(0)); err != nil {
		t.Fatalf("failed to check checkpoint: %v", err)
	}

	if err := stock.Prepare(checkpoint.Checkpoint(1)); err != nil {
		t.Fatalf("failed to prepare checkpoint: %v", err)
	}
	if err := stock.Commit(checkpoint.Checkpoint(1)); err != nil {
		t.Fatalf("failed to commit checkpoint: %v", err)
	}
	if err := stock.Close(); err != nil {
		t.Fatalf("failed to close stock: %v", err)
	}

	stock, err = factory.Open(t, dir)
	if err != nil {
		t.Fatalf("failed to open stock: %v", err)
	}

	if err := stock.GuaranteeCheckpoint(checkpoint.Checkpoint(1)); err != nil {
		t.Fatalf("failed to check checkpoint: %v", err)
	}
}

func testStockElementsIncludedInCheckpointArePersisted(t *testing.T, factory NamedStockFactory) {
	dir := t.TempDir()
	stock, err := factory.Open(t, dir)
	if err != nil {
		t.Fatalf("failed to open stock: %v", err)
	}

	id, err := stock.New()
	if err != nil {
		t.Fatalf("failed to create item in stock: %v", err)
	}
	if err := stock.Set(id, 1); err != nil {
		t.Fatalf("failed to set value in stock: %v", err)
	}

	if err := stock.GuaranteeCheckpoint(checkpoint.Checkpoint(0)); err != nil {
		t.Fatalf("failed to check checkpoint: %v", err)
	}

	if err := stock.Prepare(checkpoint.Checkpoint(1)); err != nil {
		t.Fatalf("failed to prepare checkpoint: %v", err)
	}
	if err := stock.Commit(checkpoint.Checkpoint(1)); err != nil {
		t.Fatalf("failed to checkpoint: %v", err)
	}
	if err := stock.Close(); err != nil {
		t.Fatalf("failed to close stock: %v", err)
	}

	stock, err = factory.Open(t, dir)
	if err != nil {
		t.Fatalf("failed to open stock: %v", err)
	}

	if err := stock.GuaranteeCheckpoint(checkpoint.Checkpoint(1)); err != nil {
		t.Fatalf("failed to check checkpoint: %v", err)
	}

	if err := stock.Set(id, 1); err == nil {
		t.Errorf("committed value should not be allowed to be set any more")
	}
}

func testCheckpointCanBeRecoverFromCrashAfterPrepare(t *testing.T, factory NamedStockFactory) {
	tests := map[string]struct {
		recoveryCommit checkpoint.Checkpoint
		shouldBeSealed bool
	}{
		"crash-before-commit": {
			recoveryCommit: checkpoint.Checkpoint(0),
			shouldBeSealed: false,
		},
		"crash-after-commit": {
			recoveryCommit: checkpoint.Checkpoint(1),
			shouldBeSealed: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			first, err := factory.Open(t, dir)
			if err != nil {
				t.Fatalf("failed to open stock: %v", err)
			}

			id, err := first.New()
			if err != nil {
				t.Fatalf("failed to create item in stock: %v", err)
			}
			if err := first.Set(id, 1); err != nil {
				t.Fatalf("failed to set value in stock: %v", err)
			}

			if err := first.Prepare(checkpoint.Checkpoint(1)); err != nil {
				t.Fatalf("failed to prepare checkpoint: %v", err)
			}

			// At this point the node is crashing and the stock is not closed.
			// For the first stock, it is unclear whether the commit was successful
			// and should be enforced or whether it failed, and should be rolled
			// back. This is decided during re-opening the stock and recovering
			// the state using the `GuaranteeCheckpoint` function.

			second, err := factory.Open(t, dir)
			if err != nil {
				t.Fatalf("failed to open stock: %v", err)
			}

			if err := second.GuaranteeCheckpoint(test.recoveryCommit); err != nil {
				t.Fatalf("failed to check checkpoint: %v", err)
			}

			if test.shouldBeSealed {
				if err := second.Set(id, 2); err == nil {
					t.Errorf("setting value should fail after the checkpoint has been committed")
				}
			} else {
				if err := second.Set(id, 2); err != nil {
					t.Errorf("setting value should succeed after abort: %v", err)
				}
			}
		})
	}
}

func testMissingCheckpointIsDetected(t *testing.T, factory NamedStockFactory) {
	dir := t.TempDir()
	stock, err := factory.Open(t, dir)
	if err != nil {
		t.Fatalf("failed to open stock: %v", err)
	}

	checkpoint := checkpoint.Checkpoint(1)

	if err := stock.GuaranteeCheckpoint(checkpoint); err == nil {
		t.Fatalf("missing checkpoint should be detected")
	}
}

func testCheckpointPrepareFailsOnWrongCheckpoint(t *testing.T, factory NamedStockFactory) {
	dir := t.TempDir()
	stock, err := factory.Open(t, dir)
	if err != nil {
		t.Fatalf("failed to open stock: %v", err)
	}

	c2 := checkpoint.Checkpoint(2)
	if err := stock.Prepare(c2); err == nil {
		t.Fatalf("preparing with wrong checkpoint should fail")
	}
}

func testCheckpointCommitFailsOnWrongCheckpoint(t *testing.T, factory NamedStockFactory) {
	dir := t.TempDir()
	stock, err := factory.Open(t, dir)
	if err != nil {
		t.Fatalf("failed to open stock: %v", err)
	}

	c1 := checkpoint.Checkpoint(1)
	c2 := checkpoint.Checkpoint(2)

	if err := stock.Prepare(c1); err != nil {
		t.Fatalf("failed to prepare checkpoint: %v", err)
	}

	if err := stock.Commit(c2); err == nil {
		t.Fatalf("committing with wrong checkpoint should fail")
	}
}

func testCheckpointAbortFailsOnWrongCheckpoint(t *testing.T, factory NamedStockFactory) {
	dir := t.TempDir()
	stock, err := factory.Open(t, dir)
	if err != nil {
		t.Fatalf("failed to open stock: %v", err)
	}

	c1 := checkpoint.Checkpoint(1)
	c2 := checkpoint.Checkpoint(2)

	if err := stock.Prepare(c1); err != nil {
		t.Fatalf("failed to prepare checkpoint: %v", err)
	}

	if err := stock.Abort(c2); err == nil {
		t.Fatalf("abort with wrong checkpoint should fail")
	}
}

// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package mpt

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
	"go.uber.org/mock/gomock"
)

func TestNodeFlusher_StartAndStop(t *testing.T) {
	flusher := startNodeFlusher(nil, nil, nodeFlusherConfig{})
	if err := flusher.Stop(); err != nil {
		t.Fatalf("failed to stop node flusher: %v", err)
	}
}

func TestNodeFlusher_StartAndStopWithDisabledFlusher(t *testing.T) {
	flusher := startNodeFlusher(nil, nil, nodeFlusherConfig{
		period: -1,
	})

	select {
	case <-flusher.done: // ok
	default:
		t.Errorf("flusher should be disabled")
	}

	if err := flusher.Stop(); err != nil {
		t.Fatalf("failed to stop node flusher: %v", err)
	}
}

func TestNodeFlusher_TriggersFlushesPeriodically(t *testing.T) {
	const period = time.Millisecond * 100
	ctrl := gomock.NewController(t)
	cache := NewMockNodeCache(ctrl)

	flushSignal := make(chan struct{}, 1)
	cache.EXPECT().ForEach(gomock.Any()).Times(3).Do(func(f func(id NodeId, node *shared.Shared[Node])) {
		flushSignal <- struct{}{}
	}).Return()

	flusher := startNodeFlusher(cache, nil, nodeFlusherConfig{
		period: period,
	})

	last := time.Now()
	for i := 0; i < 3; i++ {
		select {
		case <-flushSignal:
			if time.Since(last) < period/2 {
				t.Fatalf("flush signal received too early")
			}
			last = time.Now()
		case <-time.After(period * 2):
			t.Fatalf("flush signal not received")
		}
	}

	if err := flusher.Stop(); err != nil {
		t.Fatalf("failed to stop node flusher: %v", err)
	}
}

func TestNodeFlusher_ErrorsAreCollected(t *testing.T) {
	const period = time.Millisecond
	ctrl := gomock.NewController(t)
	cache := NewMockNodeCache(ctrl)
	sink := NewMockNodeSink(ctrl)

	id := ValueId(1)
	node := shared.MakeShared[Node](&ValueNode{nodeBase: nodeBase{clean: cleanStatusDirty}})

	done := make(chan struct{})
	counter := 0
	cache.EXPECT().ForEach(gomock.Any()).AnyTimes().Do(func(f func(NodeId, *shared.Shared[Node])) {
		f(id, node)
		counter++
		if counter == 3 {
			close(done)
		}
	}).Return()

	injectedError := fmt.Errorf("injected error")
	cache.EXPECT().Get(RefTo(id)).Return(node, true).AnyTimes()
	sink.EXPECT().Write(id, gomock.Any()).Return(injectedError).AnyTimes()

	flusher := startNodeFlusher(cache, sink, nodeFlusherConfig{
		period: period,
	})

	<-done

	if err := flusher.Stop(); !errors.Is(err, injectedError) {
		t.Errorf("expected injected error, got: %v", err)
	}
}

func TestNodeFlusher_FlushesOnlyDirtyNodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	cache := NewMockNodeCache(ctrl)
	sink := NewMockNodeSink(ctrl)

	nodes := map[NodeId]*shared.Shared[Node]{
		ValueId(1): shared.MakeShared[Node](&ValueNode{nodeBase: nodeBase{clean: cleanStatusClean}}),
		ValueId(2): shared.MakeShared[Node](&ValueNode{nodeBase: nodeBase{clean: cleanStatusDirty}}),
		ValueId(3): shared.MakeShared[Node](&ValueNode{nodeBase: nodeBase{clean: cleanStatusDirty}}),
		ValueId(4): shared.MakeShared[Node](&ValueNode{nodeBase: nodeBase{clean: cleanStatusClean}}),
	}

	// All nodes are checked.
	cache.EXPECT().ForEach(gomock.Any()).Do(func(f func(NodeId, *shared.Shared[Node])) {
		for i, node := range nodes {
			f(i, node)
		}
	})

	// Only the dirty ones are fetched.
	cache.EXPECT().Get(RefTo(ValueId(2))).Return(nodes[ValueId(2)], true)
	cache.EXPECT().Get(RefTo(ValueId(3))).Return(nodes[ValueId(3)], true)

	// Only the dirty nodes are flushed.
	sink.EXPECT().Write(ValueId(2), gomock.Any())
	sink.EXPECT().Write(ValueId(3), gomock.Any())

	if err := tryFlushDirtyNodes(cache, sink); err != nil {
		t.Fatalf("failed to flush dirty nodes: %v", err)
	}

	for _, node := range nodes {
		checkThatNodeIsNotLocked(t, node)
	}
}

func TestNodeFlusher_FlushedNodesAreMarkedClean(t *testing.T) {
	ctrl := gomock.NewController(t)
	cache := NewMockNodeCache(ctrl)
	sink := NewMockNodeSink(ctrl)

	id := ValueId(1)
	node := shared.MakeShared[Node](&ValueNode{nodeBase: nodeBase{clean: cleanStatusDirty}})
	cache.EXPECT().ForEach(gomock.Any()).Do(func(f func(NodeId, *shared.Shared[Node])) {
		f(id, node)
	})

	cache.EXPECT().Get(RefTo(id)).Return(node, true)
	sink.EXPECT().Write(id, gomock.Any())

	if err := tryFlushDirtyNodes(cache, sink); err != nil {
		t.Fatalf("failed to flush dirty nodes: %v", err)
	}

	handle := node.GetViewHandle()
	if dirty := handle.Get().IsDirty(); dirty {
		t.Errorf("flushed node was not marked as clean")
	}
	handle.Release()
	checkThatNodeIsNotLocked(t, node)
}

func TestNodeFlusher_NodesInUseAreIgnored(t *testing.T) {
	ctrl := gomock.NewController(t)
	cache := NewMockNodeCache(ctrl)
	sink := NewMockNodeSink(ctrl)

	id := ValueId(1)
	node := shared.MakeShared[Node](&ValueNode{nodeBase: nodeBase{clean: cleanStatusDirty}})
	cache.EXPECT().ForEach(gomock.Any()).Do(func(f func(NodeId, *shared.Shared[Node])) {
		f(id, node)
	})

	cache.EXPECT().Get(RefTo(id)).Return(node, true)

	// There shall be no write events (which is the default, but spelled out explicitly here).
	sink.EXPECT().Write(gomock.Any(), gomock.Any()).Times(0)

	// Get a lock on the node, which should prevent it from being flushed.
	handle := node.GetWriteHandle()

	if err := tryFlushDirtyNodes(cache, sink); err != nil {
		t.Fatalf("failed to flush dirty nodes: %v", err)
	}

	if dirty := handle.Get().IsDirty(); !dirty {
		t.Errorf("the test node should still be marked as dirty")
	}

	handle.Release()
	checkThatNodeIsNotLocked(t, node)
}

func TestNodeFlusher_NodesThatAreAccessedAfterBeingIdentifiedAsDirtyAreIgnored(t *testing.T) {
	ctrl := gomock.NewController(t)
	cache := NewMockNodeCache(ctrl)
	sink := NewMockNodeSink(ctrl)

	id := ValueId(1)
	node := shared.MakeShared[Node](&ValueNode{nodeBase: nodeBase{clean: cleanStatusDirty}})
	handle := shared.WriteHandle[Node]{}
	cache.EXPECT().ForEach(gomock.Any()).Do(func(f func(NodeId, *shared.Shared[Node])) {
		f(id, node)
		handle = node.GetWriteHandle() // < lock this node after it was identified as dirty
	})

	cache.EXPECT().Get(RefTo(id)).Return(node, true)

	// There shall be no write events (which is the default, but spelled out explicitly here).
	sink.EXPECT().Write(gomock.Any(), gomock.Any()).Times(0)

	if err := tryFlushDirtyNodes(cache, sink); err != nil {
		t.Fatalf("failed to flush dirty nodes: %v", err)
	}

	handle.Release()
	checkThatNodeIsNotLocked(t, node)
}

func TestNodeFlusher_EvictedNodesAreIgnored(t *testing.T) {
	ctrl := gomock.NewController(t)
	cache := NewMockNodeCache(ctrl)
	sink := NewMockNodeSink(ctrl)

	id := ValueId(1)
	node := shared.MakeShared[Node](&ValueNode{nodeBase: nodeBase{clean: cleanStatusDirty}})
	cache.EXPECT().ForEach(gomock.Any()).Do(func(f func(NodeId, *shared.Shared[Node])) {
		f(id, node)
	})

	// The node gets evicted between the for-each and the lookup.
	cache.EXPECT().Get(RefTo(id)).Return(nil, false)

	// There shall be no write events (which is the default, but spelled out explicitly here).
	sink.EXPECT().Write(gomock.Any(), gomock.Any()).Times(0)

	if err := tryFlushDirtyNodes(cache, sink); err != nil {
		t.Fatalf("failed to flush dirty nodes: %v", err)
	}

	checkThatNodeIsNotLocked(t, node)
}

func TestNodeFlusher_NodesThatGetMarkedCleanByThirdPartyAreIgnored(t *testing.T) {
	ctrl := gomock.NewController(t)
	cache := NewMockNodeCache(ctrl)
	sink := NewMockNodeSink(ctrl)

	id := ValueId(1)
	node := shared.MakeShared[Node](&ValueNode{nodeBase: nodeBase{clean: cleanStatusDirty}})
	cache.EXPECT().ForEach(gomock.Any()).Do(func(f func(NodeId, *shared.Shared[Node])) {
		f(id, node)
		handle := node.GetWriteHandle()
		handle.Get().MarkClean()
		handle.Release()
	})

	cache.EXPECT().Get(RefTo(id)).Return(node, true)

	// There shall be no write events (which is the default, but spelled out explicitly here).
	sink.EXPECT().Write(gomock.Any(), gomock.Any()).Times(0)

	if err := tryFlushDirtyNodes(cache, sink); err != nil {
		t.Fatalf("failed to flush dirty nodes: %v", err)
	}

	checkThatNodeIsNotLocked(t, node)
}

func TestNodeFlusher_NodesWithDirtyHashesAreIgnored(t *testing.T) {
	ctrl := gomock.NewController(t)
	cache := NewMockNodeCache(ctrl)
	sink := NewMockNodeSink(ctrl)

	id := ValueId(1)
	node := shared.MakeShared[Node](&ValueNode{nodeBase: nodeBase{clean: cleanStatusDirty, hashStatus: hashStatusDirty}})
	cache.EXPECT().ForEach(gomock.Any()).Do(func(f func(NodeId, *shared.Shared[Node])) {
		f(id, node)
	})

	cache.EXPECT().Get(RefTo(id)).Return(node, true)

	// There shall be no write events (which is the default, but spelled out explicitly here).
	sink.EXPECT().Write(gomock.Any(), gomock.Any()).Times(0)

	if err := tryFlushDirtyNodes(cache, sink); err != nil {
		t.Fatalf("failed to flush dirty nodes: %v", err)
	}

	checkThatNodeIsNotLocked(t, node)
}

func TestNodeFlusher_FlushErrorsArePropagated(t *testing.T) {
	ctrl := gomock.NewController(t)
	cache := NewMockNodeCache(ctrl)
	sink := NewMockNodeSink(ctrl)

	id := ValueId(1)
	node := shared.MakeShared[Node](&ValueNode{nodeBase: nodeBase{clean: cleanStatusDirty}})
	cache.EXPECT().ForEach(gomock.Any()).Do(func(f func(NodeId, *shared.Shared[Node])) {
		f(id, node)
	})

	injectedError := fmt.Errorf("injected error")
	cache.EXPECT().Get(RefTo(id)).Return(node, true)
	sink.EXPECT().Write(id, gomock.Any()).Return(injectedError)

	if err := tryFlushDirtyNodes(cache, sink); !errors.Is(err, injectedError) {
		t.Errorf("expected injected error, got: %v", err)
	}

	handle := node.GetViewHandle()
	if dirty := handle.Get().IsDirty(); !dirty {
		t.Errorf("failed flush should not lead to node marked as clean")
	}
	handle.Release()
	checkThatNodeIsNotLocked(t, node)
}

func TestNodeFlusher_FlushErrorsAreAggregated(t *testing.T) {
	ctrl := gomock.NewController(t)
	cache := NewMockNodeCache(ctrl)
	sink := NewMockNodeSink(ctrl)

	id1 := ValueId(1)
	id2 := ValueId(2)
	node := shared.MakeShared[Node](&ValueNode{nodeBase: nodeBase{clean: cleanStatusDirty}})
	cache.EXPECT().ForEach(gomock.Any()).Do(func(f func(NodeId, *shared.Shared[Node])) {
		f(id1, node)
		f(id2, node)
	})

	cache.EXPECT().Get(RefTo(id1)).Return(node, true)
	cache.EXPECT().Get(RefTo(id2)).Return(node, true)

	injectedError1 := fmt.Errorf("injected error - 1")
	injectedError2 := fmt.Errorf("injected error - 2")
	sink.EXPECT().Write(id1, gomock.Any()).Return(injectedError1)
	sink.EXPECT().Write(id2, gomock.Any()).Return(injectedError2)

	if err := tryFlushDirtyNodes(cache, sink); !errors.Is(err, injectedError1) || !errors.Is(err, injectedError2) {
		t.Errorf("expected injected errors, got: %v", err)
	}

	checkThatNodeIsNotLocked(t, node)
}

func checkThatNodeIsNotLocked(t *testing.T, node *shared.Shared[Node]) {
	t.Helper()
	handle, success := node.TryGetWriteHandle()
	if !success {
		t.Errorf("node is locked")
	}
	handle.Release()
}

func BenchmarkNodeFlusher_CollectDirtyNodes(b *testing.B) {
	sizes := []int{1_000, 10_000, 100_000, 1_000_000, 10_000_000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			// Fill a cache with clean nodes.
			cache := NewNodeCache(size)
			for i := 0; i < size; i++ {
				node := shared.MakeShared[Node](&ValueNode{nodeBase: nodeBase{clean: cleanStatusClean}})
				ref := NewNodeReference(ValueId(uint64(i)))
				cache.GetOrSet(&ref, node)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tryFlushDirtyNodes(cache, nil)
			}
		})
	}
}

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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
	"go.uber.org/mock/gomock"
)

func TestWriteBuffer_CanBeFlushedWhenEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	sink := NewMockNodeSink(ctrl)

	buffer := MakeWriteBuffer(sink)
	defer buffer.Close()

	if err := buffer.Flush(); err != nil {
		t.Errorf("failed to flush buffer: %v", err)
	}
}

func TestWriteBuffer_CanFlushASingleElement(t *testing.T) {
	ctrl := gomock.NewController(t)
	sink := NewMockNodeSink(ctrl)

	id := ValueId(12)
	node := shared.MakeShared[Node](&ValueNode{})
	view := node.GetViewHandle()
	sink.EXPECT().Write(id, view)
	view.Release()

	buffer := MakeWriteBuffer(sink)
	defer buffer.Close()

	buffer.Add(id, node)
	if err := buffer.Flush(); err != nil {
		t.Errorf("failed to flush buffer: %v", err)
	}
}

func TestWriteBuffer_CanBeClosedMultipleTimes(t *testing.T) {
	ctrl := gomock.NewController(t)
	sink := NewMockNodeSink(ctrl)

	buffer := MakeWriteBuffer(sink)
	buffer.Close()
	buffer.Close()
}

func TestWriteBuffer_EnqueuedElementCanBeCanceled(t *testing.T) {
	ctrl := gomock.NewController(t)
	sink := NewMockNodeSink(ctrl)

	id := ValueId(12)
	node := shared.MakeShared[Node](EmptyNode{})

	// Since cancel could be too late, a write may happen.
	sink.EXPECT().Write(id, gomock.Any()).MaxTimes(1)

	buffer := MakeWriteBuffer(sink)
	defer buffer.Close()

	buffer.Add(id, node)

	if got, found := buffer.Cancel(id); found && got != node {
		t.Fatalf("failed to cancel %v, wanted %v, got %v, found %v", id, node, got, found)
	}

	if err := buffer.Flush(); err != nil {
		t.Errorf("failed to flush buffer: %v", err)
	}
}

func TestWriteBuffer_CanFlushLargeNumberOfElements(t *testing.T) {
	ctrl := gomock.NewController(t)
	sink := NewMockNodeSink(ctrl)

	N := 1000

	// Setup expectations
	for i := 0; i < N; i++ {
		sink.EXPECT().Write(ValueId(uint64(i)), gomock.Any())
	}

	buffer := makeWriteBuffer(sink, N/10)
	defer buffer.Close()

	// Send in N nodes to be written to the sink.
	for i := 0; i < N; i++ {
		buffer.Add(ValueId(uint64(i)), shared.MakeShared[Node](&ValueNode{}))
	}

	if err := buffer.Flush(); err != nil {
		t.Errorf("failed to flush buffer: %v", err)
	}
}

func TestWriteBuffer_AllQueuedEntriesArePresentUntilWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	sink := NewMockNodeSink(ctrl)

	N := 1000
	buffer := makeWriteBuffer(sink, N/10).(*writeBuffer)
	defer buffer.Close()

	enqueued := map[NodeId]bool{}
	enqueuedLock := sync.Mutex{}

	written := map[NodeId]bool{}

	sink.EXPECT().Write(gomock.Any(), gomock.Any()).AnyTimes().Do(func(id NodeId, _ shared.ViewHandle[Node]) {
		// Check that everything that was enqueued and is not yet written is still present.
		enqueuedLock.Lock()
		for id := range enqueued {
			if _, found := written[id]; !found {
				if !buffer.contains(id) {
					t.Errorf("missing %v in buffer", id)
				}
			}
		}
		enqueuedLock.Unlock()
		written[id] = true
	})

	node := shared.MakeShared[Node](EmptyNode{})
	for i := 0; i < N; i++ {
		id := ValueId(uint64(i))
		buffer.Add(id, node)
		enqueuedLock.Lock()
		enqueued[id] = true
		enqueuedLock.Unlock()
	}

	if err := buffer.Flush(); err != nil {
		t.Errorf("failed to flush buffer: %v", err)
	}
}

func TestWriteBuffer_CheckThatLockedNodesAreWaitedFor(t *testing.T) {
	ctrl := gomock.NewController(t)
	sink := NewMockNodeSink(ctrl)

	id1 := ValueId(1)
	id2 := ValueId(2)
	value1 := shared.MakeShared[Node](&ValueNode{})
	value2 := shared.MakeShared[Node](&ValueNode{})

	view1 := value1.GetViewHandle()
	sink.EXPECT().Write(id1, view1)
	view1.Release()

	view2 := value2.GetViewHandle()
	sink.EXPECT().Write(id2, view2)
	view2.Release()

	buffer := makeWriteBuffer(sink, 100)
	defer buffer.Close()

	write := value2.GetWriteHandle()
	done := false
	go func() {
		time.Sleep(1 * time.Second)
		done = true
		write.Release()
	}()

	buffer.Add(id1, value1)
	buffer.Add(id2, value2)

	if err := buffer.Flush(); err != nil {
		t.Errorf("failed to flush buffer: %v", err)
	}
	if !done {
		t.Errorf("flush finished before write access was released!")
	}
}

func TestWriteBuffer_AFailedFlushIsReported(t *testing.T) {
	ctrl := gomock.NewController(t)
	sink := NewMockNodeSink(ctrl)

	id := ValueId(12)
	node := shared.MakeShared[Node](&ValueNode{})
	view := node.GetViewHandle()
	err := fmt.Errorf("TestError")
	sink.EXPECT().Write(id, view).Return(err)
	view.Release()

	buffer := MakeWriteBuffer(sink)
	defer buffer.Close()

	buffer.Add(id, node)
	if got := buffer.Flush(); !errors.Is(got, err) {
		t.Errorf("sink error was not propagated, wanted %v, got %v", err, got)
	}
}

func TestWriteBuffer_ElementsCanBeAddedInParallel(t *testing.T) {
	// This test checks for race conditions if --race is enabled.
	ctrl := gomock.NewController(t)
	sink := NewMockNodeSink(ctrl)

	N := 1000

	// Setup expectations
	for i := 0; i < N; i++ {
		sink.EXPECT().Write(ValueId(uint64(i)), gomock.Any())
	}

	buffer := makeWriteBuffer(sink, N/10)
	defer buffer.Close()

	// Send in N nodes in parallel.
	var wg sync.WaitGroup
	wg.Add(N / 100)
	for j := 0; j < N/100; j++ {
		go func(j int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				buffer.Add(ValueId(uint64(j*100+i)), shared.MakeShared[Node](&ValueNode{}))
			}
		}(j)
	}
	wg.Wait()

	if err := buffer.Flush(); err != nil {
		t.Errorf("failed to flush buffer: %v", err)
	}
}

func TestWriteBuffer_ElementsCanBeAddedAndCanceledInParallel(t *testing.T) {
	// This test checks for race conditions if --race is enabled.
	ctrl := gomock.NewController(t)
	sink := NewMockNodeSink(ctrl)

	N := 1000

	sink.EXPECT().Write(gomock.Any(), gomock.Any()).AnyTimes()

	buffer := makeWriteBuffer(sink, N/10)
	defer buffer.Close()

	// Send in N nodes in parallel.
	var wg sync.WaitGroup
	wg.Add(N / 100)
	for j := 0; j < N/100; j++ {
		go func(j int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				buffer.Add(ValueId(uint64(j*100+i)), shared.MakeShared[Node](&ValueNode{}))
			}
			for i := 0; i < 100; i++ {
				buffer.Cancel(ValueId(uint64(j*100 + i)))
			}
		}(j)
	}
	wg.Wait()

	if err := buffer.Flush(); err != nil {
		t.Errorf("failed to flush buffer: %v", err)
	}
}

func TestWriteBuffer_FlushedElementsAreMarkedAsClean(t *testing.T) {
	ctrl := gomock.NewController(t)
	sink := NewMockNodeSink(ctrl)
	mock := NewMockNode(ctrl)

	id := ValueId(12)
	node := shared.MakeShared[Node](mock)

	mock.EXPECT().IsDirty().Return(true)
	mock.EXPECT().MarkClean()

	view := node.GetViewHandle()
	sink.EXPECT().Write(id, view)
	view.Release()

	buffer := MakeWriteBuffer(sink)
	defer buffer.Close()

	buffer.Add(id, node)
	if err := buffer.Flush(); err != nil {
		t.Errorf("failed to flush buffer: %v", err)
	}
}

func TestWriteBuffer_CleanNodesAreNotWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	sink := NewMockNodeSink(ctrl)
	mock := NewMockNode(ctrl)

	id := ValueId(12)
	node := shared.MakeShared[Node](mock)

	mock.EXPECT().IsDirty().Return(false)

	// Note: no write to the sink is expected.

	buffer := MakeWriteBuffer(sink)
	defer buffer.Close()

	buffer.Add(id, node)
	if err := buffer.Flush(); err != nil {
		t.Errorf("failed to flush buffer: %v", err)
	}
}

func TestWriteBuffer_ZeroCap(t *testing.T) {
	ctrl := gomock.NewController(t)
	sink := NewMockNodeSink(ctrl)

	buffer := makeWriteBuffer(sink, 0)
	defer buffer.Close()

	if got, want := buffer.(*writeBuffer).capacity, 1; got != want {
		t.Errorf("default capacity does not match: %d != %d", got, want)
	}
}

func TestWriteBuffer_contains(t *testing.T) {
	ctrl := gomock.NewController(t)
	sink := NewMockNodeSink(ctrl)

	buffer := makeWriteBuffer(sink, 0)
	defer buffer.Close()

	buffer.Add(BranchId(123), nil)
	if got, want := buffer.(*writeBuffer).contains(BranchId(123)), true; got != want {
		t.Errorf("check for present item failed")
	}
	if got, want := buffer.(*writeBuffer).contains(BranchId(345)), false; got != want {
		t.Errorf("check for missing item failed")
	}
	buffer.Cancel(BranchId(123))
	if got, want := buffer.(*writeBuffer).contains(BranchId(123)), false; got != want {
		t.Errorf("check for canceled item failed")
	}

}

func TestWriteBuffer_Add_Cancel_Empty_DoesNotLock(t *testing.T) {
	ctrl := gomock.NewController(t)
	sink := NewMockNodeSink(ctrl)

	sink.EXPECT().Write(gomock.Any(), gomock.Any()).AnyTimes()

	buffer := makeWriteBuffer(sink, 0)
	defer func() {
		if err := buffer.Close(); err != nil {
			t.Fatalf("failed to close buffer: %v", err)
		}
	}()

	node := shared.MakeShared[Node](&BranchNode{})

	var started sync.WaitGroup
	var run atomic.Bool
	run.Store(true)
	id := BranchId(123)
	heartbeat := make(chan struct{}, 1000)

	started.Add(1)
	go func() {
		started.Done()
		for run.Load() {
			handle := node.GetReadHandle()
			buffer.Cancel(id)
			handle.Release()
			heartbeat <- struct{}{}
		}
	}()

	started.Add(1)
	go func() {
		started.Done()
		for run.Load() {
			buffer.Cancel(id)
			heartbeat <- struct{}{}
		}
	}()

	started.Add(1)
	go func() {
		started.Done()
		for run.Load() {
			buffer.(*writeBuffer).emptyBuffer()
			heartbeat <- struct{}{}
		}
	}()

	started.Add(1)
	go func() {
		started.Done()
		for run.Load() {
			select {
			case <-heartbeat:
			case <-time.After(10 * time.Second):
				run.Store(false)
				t.Errorf("likely deadlock detected")
			}
		}
	}()

	started.Wait()
	const loops = 10_000
	for i := 0; i < loops; i++ {
		buffer.Add(id, node)
	}

	run.Store(false)
}

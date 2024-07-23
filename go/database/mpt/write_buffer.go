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

//go:generate mockgen -source write_buffer.go -destination write_buffer_mocks.go -package mpt

import (
	"errors"
	"runtime"
	"sort"
	"sync"

	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
)

// ----------------------------------------------------------------------------
//                             Interfaces
// ----------------------------------------------------------------------------

// WriteBuffer is a utility buffering the flushing of nodes to some
// node sink. Its main task is to perform writes asynchronously in a
// managed background thread, queuing nodes to be written to the sink
// in an internal buffer.
type WriteBuffer interface {
	// Add adds the given node to the queue of nodes to be written to
	// the underlying sink. The timing and order of those write operations
	// is undefined. The only guarantee is that they may happen eventually
	// in an arbitrary order.
	Add(NodeId, *shared.Shared[Node])
	// Cancel aborts the flushing of the node with the given ID and returns
	// the node back to the caller. If present, the node is removed from the
	// buffer and no longer flushed. If no such node is present, (nil,false)
	// is returned.
	Cancel(NodeId) (*shared.Shared[Node], bool)
	// Flush forces all buffered elements to be written to the sink.
	Flush() error
	// Close flushes buffered elements and stops asynchronous operations.
	Close() error
}

// NodeSink defines an interface for where WriteBuffers are able to write
// node information to.
type NodeSink interface {
	Write(NodeId, shared.ViewHandle[Node]) error
}

func MakeWriteBuffer(sink NodeSink) WriteBuffer {
	return makeWriteBuffer(sink, 1024)
}

// ----------------------------------------------------------------------------
//                             Implementation
// ----------------------------------------------------------------------------

type writeBuffer struct {
	sink                    NodeSink
	capacity                int
	counter                 int
	buffer                  map[NodeId]*shared.Shared[Node]
	bufferMutex             sync.Mutex
	emptyBufferSignal       chan bool // true if an explicit flush is triggered, false for an implicit
	emptyBufferSignalMutex  sync.Mutex
	emptyBufferSignalClosed bool
	flushDone               <-chan struct{}
	done                    <-chan struct{}
	errs                    []error
	errsMutex               sync.Mutex
}

func makeWriteBuffer(sink NodeSink, capacity int) WriteBuffer {
	if capacity < 1 {
		capacity = 1
	}

	emptyBufferSignal := make(chan bool, 1)
	flushDone := make(chan struct{})
	done := make(chan struct{})

	res := &writeBuffer{
		sink:              sink,
		capacity:          capacity,
		buffer:            make(map[NodeId]*shared.Shared[Node], 2*capacity),
		emptyBufferSignal: emptyBufferSignal,
		flushDone:         flushDone,
		done:              done,
	}

	// A background goroutine flushing nodes to the sink and handling
	// synchronization tasks.
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		defer close(done)
		defer close(flushDone)
		for flush := range emptyBufferSignal {
			res.emptyBuffer()
			if flush {
				flushDone <- struct{}{}
			}
		}
	}()

	return res
}

func (b *writeBuffer) Add(id NodeId, node *shared.Shared[Node]) {
	// Empty nodes are ignored (and internally used for signaling flush requests).
	if id.IsEmpty() {
		return
	}
	b.bufferMutex.Lock()
	b.buffer[id] = node
	b.bufferMutex.Unlock()
	b.emptyBufferSignalMutex.Lock()
	b.counter++
	if b.counter > b.capacity && !b.emptyBufferSignalClosed {
		// The option to ignore a full signal channel here is important
		// to prevent a potential deadlock. See
		// https://github.com/Fantom-foundation/Carmen/issues/724
		// for more details.
		select {
		case b.emptyBufferSignal <- false: /* ok, a new clear-buffer operation is scheduled */
		default: /* also fine, an operation to clear the buffer is already pending */
		}
		b.counter = 0
	}
	b.emptyBufferSignalMutex.Unlock()
}

func (b *writeBuffer) contains(id NodeId) bool {
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()
	_, found := b.buffer[id]
	return found
}

func (b *writeBuffer) Cancel(id NodeId) (*shared.Shared[Node], bool) {
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()

	if res, found := b.buffer[id]; found {
		delete(b.buffer, id)
		return res, found
	}
	return nil, false
}

func (b *writeBuffer) Flush() error {
	b.emptyBufferSignalMutex.Lock()
	if !b.emptyBufferSignalClosed {
		b.emptyBufferSignal <- true
	}
	b.emptyBufferSignalMutex.Unlock()
	<-b.flushDone // finishes either due to flush signal or being closed
	b.errsMutex.Lock()
	defer b.errsMutex.Unlock()
	return errors.Join(b.errs...)
}

func (b *writeBuffer) Close() error {
	b.emptyBufferSignalMutex.Lock()
	if !b.emptyBufferSignalClosed {
		close(b.emptyBufferSignal)
		b.emptyBufferSignalClosed = true
	}
	b.emptyBufferSignalMutex.Unlock()
	<-b.done // finishes once all elements are written
	b.errsMutex.Lock()
	defer b.errsMutex.Unlock()
	return errors.Join(b.errs...)
}

func (b *writeBuffer) emptyBuffer() {

	// Collect a list of all IDs in the buffer.
	ids := make([]NodeId, 0, 2*b.capacity)
	b.bufferMutex.Lock()
	for id := range b.buffer {
		ids = append(ids, id)
	}
	b.bufferMutex.Unlock()

	// Sort IDs to minimize disk seeks.
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Flush all nodes of current patch.
	for i := 0; i < len(ids); i++ {
		id := ids[i]
		// Check whether the given node has not been canceled in the meantime.
		b.bufferMutex.Lock()
		node, found := b.buffer[id]
		if !found {
			b.bufferMutex.Unlock()
			continue
		}

		// Write a snapshot of the node to the disk.
		handle := node.GetWriteHandle() // write access is needed to clear the dirty flag.

		// To prevent the current node from being restored from the buffer
		// and modified by another goroutine, we need to keep the buffer
		// lock until we have write access. Otherwise, we might hold
		// a node that has been modified, yet its hash has not yet been
		// updated. Such nodes can not be written to the disk.
		b.bufferMutex.Unlock()

		if handle.Get().IsDirty() {
			if err := b.sink.Write(id, handle.AsViewHandle()); err != nil {
				b.errsMutex.Lock()
				b.errs = append(b.errs, err)
				b.errsMutex.Unlock()
			} else {
				handle.Get().MarkClean()
			}
		}

		b.bufferMutex.Lock()
		delete(b.buffer, id)
		b.bufferMutex.Unlock()

		// Only release access of the node after it has been removed from
		// the buffer such that subsequent updates are not lost.
		handle.Release()
	}
}

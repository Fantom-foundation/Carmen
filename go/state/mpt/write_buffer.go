package mpt

//go:generate mockgen -source write_buffer.go -destination write_buffer_mocks.go -package mpt

import (
	"errors"
	"runtime"
	"sort"
	"sync"

	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
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
	sink        NodeSink
	capacity    int
	buffer      map[NodeId]*shared.Shared[Node]
	bufferMutex sync.Mutex
	ids         chan NodeId
	idsMutex    sync.Mutex
	idsClosed   bool
	flushDone   <-chan struct{}
	done        <-chan struct{}
	errs        []error
	errsMutex   sync.Mutex
}

func makeWriteBuffer(sink NodeSink, capacity int) WriteBuffer {
	if capacity < 1 {
		capacity = 1
	}

	ids := make(chan NodeId, capacity)
	flushDone := make(chan struct{})
	done := make(chan struct{})

	res := &writeBuffer{
		sink:      sink,
		capacity:  capacity,
		buffer:    make(map[NodeId]*shared.Shared[Node], 2*capacity),
		ids:       ids,
		flushDone: flushDone,
		done:      done,
	}

	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		defer close(done)
		defer close(flushDone)
		counter := 0
		for id := range ids {
			// Check if this is a token signaling a flush request.
			if id.IsEmpty() {
				res.emptyBuffer()
				flushDone <- struct{}{}
				continue
			}
			// Otherwise, perform a flush every time the capacity is reached.
			counter++
			if counter >= capacity {
				res.emptyBuffer()
				counter = 0
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
	b.idsMutex.Lock()
	if !b.idsClosed {
		b.ids <- id
	}
	b.idsMutex.Unlock()
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
	b.idsMutex.Lock()
	if !b.idsClosed {
		b.ids <- EmptyId()
	}
	b.idsMutex.Unlock()
	<-b.flushDone // finishes either due to flush signal or being closed
	b.errsMutex.Lock()
	defer b.errsMutex.Unlock()
	return errors.Join(b.errs...)
}

func (b *writeBuffer) Close() error {
	b.idsMutex.Lock()
	if !b.idsClosed {
		close(b.ids)
		b.idsClosed = true
	}
	b.idsMutex.Unlock()
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
		b.bufferMutex.Unlock()
		if !found {
			continue
		}

		// Write a snapshot of the node to the disk.
		handle := node.GetViewHandle()
		if err := b.sink.Write(id, handle); err != nil {
			b.errsMutex.Lock()
			b.errs = append(b.errs, err)
			b.errsMutex.Unlock()
		}

		b.bufferMutex.Lock()
		delete(b.buffer, id)
		b.bufferMutex.Unlock()

		// Only release read access of the node after it has been removed from
		// the buffer such that subsequent updates are not lost.
		handle.Release()
	}
}

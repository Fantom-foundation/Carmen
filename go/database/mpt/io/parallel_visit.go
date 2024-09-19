// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package io

import (
	"errors"
	"fmt"
	"io"
	"path"
	"sync"
	"sync/atomic"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/Fantom-foundation/Carmen/go/common/heap"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
)

//go:generate mockgen -source parallel_visit.go -destination parallel_visit_mocks.go -package io

// visitAll visits all nodes in the trie rooted at the given node in depth-first pre-order order.
// This function accesses nodes using its own read-only node source, independently of a potential
// node source and cache managed by an MPT Forest instance.
// Thus, the caller needs to make sure that any concurrently open MPTs have been flushed
// before calling this function to avoid reading out-dated or inconsistent data.
func visitAll(
	directory string,
	config mpt.MptConfig,
	root mpt.NodeId,
	visitor noResponseNodeVisitor,
	pruneStorage bool,
) error {
	return visitAllWithSources(&stockNodeSourceFactory{directory, config}, root, visitor, pruneStorage)
}

// visitAllWithSources is an internal implementation of visitAll that allows to provide a custom node source factory.
func visitAllWithSources(
	sourceFactory nodeSourceFactory,
	root mpt.NodeId,
	visitor noResponseNodeVisitor,
	pruneStorage bool,
) error {
	return visitAllWithConfig(sourceFactory, root, visitor, visitAllConfig{
		pruneStorage: pruneStorage,
	})
}

type visitAllConfig struct {
	pruneStorage      bool // < whether to prune storage nodes
	numWorker         int  // < number of workers to be used for fetching nodes
	throttleThreshold int  // < buffer size triggering worker throttling
	batchSize         int  // < number of nodes to be prefetched in one go

	// for testing purposes
	monitor func(numResponses int)
}

func visitAllWithConfig(
	sourceFactory nodeSourceFactory,
	root mpt.NodeId,
	visitor noResponseNodeVisitor,
	config visitAllConfig,
) error {
	// The idea is to have workers processing a common queue of needed
	// nodes sorted by their position in the depth-first traversal of the
	// trie. The workers will fetch the nodes and put them into a shared
	// map of nodes. The main thread will consume the nodes from the map
	// and visit them.

	// Set default values for the configuration.
	if config.numWorker == 0 {
		config.numWorker = 16
	}
	if config.throttleThreshold == 0 {
		config.throttleThreshold = 100_000
	}
	if config.batchSize == 0 {
		config.batchSize = 1000
	}

	type request struct {
		position *position
		id       mpt.NodeId
	}

	requestsMutex := sync.Mutex{}
	requests := heap.New(func(a, b request) int {
		return b.position.compare(a.position)
	})

	type response struct {
		node mpt.Node
		err  error
	}
	responses := map[mpt.NodeId]response{}
	responsesMutex := sync.Mutex{}
	responsesConsumedCond := sync.NewCond(&responsesMutex)
	defer responsesConsumedCond.Broadcast() // < free potential waiting workers

	barrier := newBarrier(config.numWorker)
	defer barrier.release()

	done := atomic.Bool{}
	defer done.Store(true)

	requests.Add(request{nil, root})

	prefetchNext := func(source nodeSource) {
		// get the next job
		requestsMutex.Lock()
		req, present := requests.Pop()
		requestsMutex.Unlock()

		// process the request
		if !present {
			return
		}

		// fetch the node and put it into the responses
		node, err := source.get(req.id)

		responsesMutex.Lock()
		responses[req.id] = response{node, err}
		responsesMutex.Unlock()

		// if there was a fetch error, stop the workers
		if err != nil {
			return
		}

		// derive child nodes to be fetched
		switch node := node.(type) {
		case *mpt.BranchNode:
			children := node.GetChildren()
			requestsMutex.Lock()
			for i, child := range children {
				id := child.Id()
				if id.IsEmpty() {
					continue
				}
				pos := req.position.child(byte(i))
				requests.Add(request{pos, child.Id()})
			}
			requestsMutex.Unlock()
		case *mpt.ExtensionNode:
			next := node.GetNext()
			requestsMutex.Lock()
			pos := req.position.child(0)
			requests.Add(request{pos, next.Id()})
			requestsMutex.Unlock()
		case *mpt.AccountNode:
			if !config.pruneStorage {
				storage := node.GetStorage()
				id := storage.Id()
				if !id.IsEmpty() {
					requestsMutex.Lock()
					pos := req.position.child(0)
					requests.Add(request{pos, id})
					requestsMutex.Unlock()
				}
			}
		}
	}

	var workersDoneWg sync.WaitGroup
	var workersInitWg sync.WaitGroup
	workersDoneWg.Add(config.numWorker)
	workersInitWg.Add(config.numWorker)
	workersErrorChan := make(chan error, config.numWorker)

	// Workers discover nodes and put child references into a queue.
	// Then the workers check which node references are in the queue
	// and fetch nodes for them, again putting child references to the queue.
	// This way, the trie is completely read multi-threaded.
	// To favor the depth-first order, the node ids in the queue are
	// sorted in a priority queue so that the deepest nodes are read first.
	// To avoid prefetching too many far-future nodes, workers synchronize on a
	// barrier periodically. This way, if worker processing the node with the
	// highest priority is slow or stalled for some reason, the remaining
	// workers are not able to rush ahead and prefetch far-future nodes.
	for i := 0; i < config.numWorker; i++ {
		go func(id int) {
			defer workersDoneWg.Done()
			source, err := sourceFactory.open()
			if err != nil {
				workersErrorChan <- err
				workersInitWg.Done()
				return
			}
			workersInitWg.Done()
			defer func() {
				if err := source.Close(); err != nil {
					workersErrorChan <- err
				}
			}()
			throttleThreshold := config.throttleThreshold
			batchSize := config.batchSize
			for {
				// Sync all workers to avoid some workers rushing ahead fetching
				// nodes of far-future parts of the trie.
				barrier.wait()
				if done.Load() {
					break
				}

				// Throttle all workers if there are too many responses in
				// the system to avoid overloading memory resources.
				responsesMutex.Lock()
				for len(responses) > throttleThreshold {
					responsesConsumedCond.Wait()
				}
				responsesMutex.Unlock()

				// Do the actual prefetching in parallel.
				for i := 0; i < batchSize; i++ {
					prefetchNext(source)
				}
			}
		}(i)
	}

	// create a source for the main thread
	source, err := sourceFactory.open()

	// wait for all go routines start to check for init errors
	workersInitWg.Wait()
	// read possible error
	var chRead bool
	for !chRead {
		select {
		case workerErr := <-workersErrorChan:
			err = errors.Join(err, workerErr)
		default:
			chRead = true
		}
	}

	// if init was not successful, return the error
	if err != nil {
		return err
	}

	// Perform depth-first iteration through the trie.
	// This main thread iterates the trie on its own and
	// provides the nodes to the visitor in dept-first order.
	// This loop does not perform any I/O, and instead it queries
	// the workers for the nodes.
	// This provides a performance boost.
	stack := []mpt.NodeId{root}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		var res response
		responsesMutex.Lock()
		for {
			if config.monitor != nil {
				config.monitor(len(responses))
			}
			found := false
			res, found = responses[cur]
			if found {
				delete(responses, cur)
				responsesConsumedCond.Broadcast()
				break
			} else {
				// If the node is not yet available, join the workers
				// in loading the next node.
				responsesMutex.Unlock()
				prefetchNext(source)
				responsesMutex.Lock()
			}
		}
		responsesMutex.Unlock()

		if res.err != nil {
			err = res.err
			break
		}

		if visitErr := visitor.Visit(res.node, mpt.NodeInfo{Id: cur}); visitErr != nil {
			err = visitErr
			break
		}
		switch node := res.node.(type) {
		case *mpt.BranchNode:
			children := node.GetChildren()
			for i := len(children) - 1; i >= 0; i-- {
				id := children[i].Id()
				if !id.IsEmpty() {
					stack = append(stack, id)
				}
			}
		case *mpt.ExtensionNode:
			next := node.GetNext()
			stack = append(stack, next.Id())
		case *mpt.AccountNode:
			if !config.pruneStorage {
				storage := node.GetStorage()
				id := storage.Id()
				if !id.IsEmpty() {
					stack = append(stack, id)
				}
			}
		}
	}

	// wait until all workers are done to read errors
	done.Store(true)
	barrier.release()
	responsesConsumedCond.Broadcast()
	workersDoneWg.Wait()
	close(workersErrorChan)
	err = errors.Join(err, source.Close())
	for workerErr := range workersErrorChan {
		err = errors.Join(err, workerErr)
	}

	return err
}

// position addresses a node within a tree by listing the path from the root node to the respective node.
// Each position holds the distance of the node from the root, and an index of a branch the node
// is attached to. For a non-branch parent node, the index is always 0.
type position struct {
	parent *position
	pos    byte
	len    byte
}

// newPosition creates a new position from a byte slice.
func newPosition(pos []byte) *position {
	var res *position
	for i, step := range pos {
		res = &position{
			parent: res,
			pos:    step,
			len:    byte(i),
		}
	}
	return res
}

func (p *position) String() string {
	if p == nil {
		return ""
	}
	if p.parent == nil {
		return fmt.Sprintf("%d", p.pos)
	}
	return fmt.Sprintf("%s.%d", p.parent.String(), p.pos)
}

// child creates a new position that is a child of the current position.
// The new position is one step deeper than the current one.
func (p *position) child(step byte) *position {
	len := byte(0)
	if p != nil {
		len = p.len
	}
	return &position{
		parent: p,
		pos:    step,
		len:    len + 1,
	}
}

// compare compares two positions.
// The order of node positions is defined by the order in which positions would
// be visited by running an depth-first pre-order tree traversal. This is equivalent
// to a lexicographical order of the positions when interpreted as a list navigation steps.
func (p *position) compare(b *position) int {
	if p == b {
		return 0
	}
	if p == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// make sure a is the shorter one
	if p.len > b.len {
		return b.compare(p) * -1
	}

	// reduce the length of b to match a
	bIsLonger := p.len < b.len
	for p.len < b.len {
		b = b.parent
	}

	// compare the common part
	prefixResult := p._compare(b)
	if prefixResult != 0 {
		return prefixResult
	}
	if bIsLonger {
		return -1
	}
	return 0
}

func (p *position) _compare(b *position) int {
	if p == b {
		return 0
	}
	prefixResult := p.parent._compare(b.parent)
	if prefixResult != 0 {
		return prefixResult
	}
	if p.pos < b.pos {
		return -1
	}
	if p.pos > b.pos {
		return 1
	}
	return 0
}

// barrier is a synchronization utility allowing a group of goroutines to wait
// for each other. The size of the group needs to be defined during barrier
// creation.
type barrier struct {
	mutex    sync.Mutex
	cond     sync.Cond
	capacity int
	waiting  int
	released bool
}

// newBarrier creates a new barrier synchronizing a given number of goroutines.
func newBarrier(capacity int) *barrier {
	res := &barrier{
		capacity: capacity,
	}
	res.cond.L = &res.mutex
	return res
}

// wait blocks until all goroutines have called wait on the barrier or release
// has been called.
func (b *barrier) wait() {
	b.mutex.Lock()
	if b.released {
		b.mutex.Unlock()
		return
	}
	b.waiting++
	if b.waiting == b.capacity {
		b.cond.Broadcast()
		b.waiting = 0
	} else {
		b.cond.Wait()
	}
	b.mutex.Unlock()
}

// release releases all goroutines waiting on the barrier and disables the
// barrier. Any future wait call will return immediately.
func (b *barrier) release() {
	b.mutex.Lock()
	b.released = true
	b.cond.Broadcast()
	b.mutex.Unlock()
}

// ----------------------------------------------------------------------------
//                               nodeSource
// ----------------------------------------------------------------------------

// nodeSourceFactory is a factory for nodeSource instances.
// It provides read-only access to nodes, potentially side-channeling another infrastructure
// that already accesses to the name nodes. The user of the factory needs to ensure that
// this is not leading to inconsistencies by only accessing nodes that are not updated
// concurrently.
type nodeSourceFactory interface {
	open() (nodeSource, error)
}

// nodeSource is a source of nodes.
// It provides read-only access to nodes by their ids.
type nodeSource interface {
	io.Closer
	get(mpt.NodeId) (mpt.Node, error)
}

// stockNodeSourceFactory is a nodeSourceFactory implementation that creates stock backed sources to access nodes.
type stockNodeSourceFactory struct {
	directory string
	config    mpt.MptConfig
}

func (f *stockNodeSourceFactory) open() (nodeSource, error) {
	var toClose []io.Closer
	closeWithErr := func(err error) error {
		for _, s := range toClose {
			err = errors.Join(err, s.Close())
		}
		return err
	}

	aEncoder, bEncoder, eEncoder, vEncoder := f.config.GetEncoders()

	accounts, err := file.OpenReadOnlyStock[uint64, mpt.AccountNode](path.Join(f.directory, "accounts"), aEncoder)
	if err != nil {
		return nil, closeWithErr(err)
	}
	toClose = append(toClose, accounts)

	branches, err := file.OpenReadOnlyStock[uint64, mpt.BranchNode](path.Join(f.directory, "branches"), bEncoder)
	if err != nil {
		return nil, closeWithErr(err)
	}
	toClose = append(toClose, branches)

	extensions, err := file.OpenReadOnlyStock[uint64, mpt.ExtensionNode](path.Join(f.directory, "extensions"), eEncoder)
	if err != nil {
		return nil, closeWithErr(err)
	}
	toClose = append(toClose, extensions)

	values, err := file.OpenReadOnlyStock[uint64, mpt.ValueNode](path.Join(f.directory, "values"), vEncoder)
	if err != nil {
		return nil, closeWithErr(err)
	}
	toClose = append(toClose, values)

	return &stockNodeSource{
		accounts:   accounts,
		branches:   branches,
		extensions: extensions,
		values:     values,
	}, nil
}

// stockNodeSource is a nodeSource implementation that uses stock to access nodes.
type stockNodeSource struct {
	accounts   stock.ReadOnly[uint64, mpt.AccountNode]
	branches   stock.ReadOnly[uint64, mpt.BranchNode]
	extensions stock.ReadOnly[uint64, mpt.ExtensionNode]
	values     stock.ReadOnly[uint64, mpt.ValueNode]
}

func (s *stockNodeSource) get(id mpt.NodeId) (mpt.Node, error) {
	pos := id.Index()
	if id.IsEmpty() {
		return mpt.EmptyNode{}, nil
	}
	if id.IsAccount() {
		res, err := s.accounts.Get(pos)
		return &res, err
	}
	if id.IsBranch() {
		res, err := s.branches.Get(pos)
		return &res, err
	}
	if id.IsExtension() {
		res, err := s.extensions.Get(pos)
		return &res, err
	}
	if id.IsValue() {
		res, err := s.values.Get(pos)
		return &res, err
	}
	return nil, fmt.Errorf("unknown node type: %v", id)
}

func (s *stockNodeSource) Close() error {
	return errors.Join(
		s.accounts.Close(),
		s.branches.Close(),
		s.extensions.Close(),
		s.values.Close(),
	)
}

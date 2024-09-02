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
	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/Fantom-foundation/Carmen/go/common/heap"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"io"
	"path"
	"sync"
	"sync/atomic"
)

//go:generate mockgen -source parallel_visit.go -destination parallel_visit_mocks.go -package io

// nodeSourceFactory is a factory for nodeSource instances.
// It provides access to nodes side to another infrastructure
// that already accesses the nodes.
type nodeSourceFactory interface {
	open() (nodeSource, error)
}

// nodeSource is a source of nodes.
// It provides access to nodes by their ids.
type nodeSource interface {
	io.Closer
	get(mpt.NodeId) (mpt.Node, error)
}

// visitAll visits all nodes in the trie rooted at the given node in depth-first pre-order order.
// This function accesses nodes using its own read-only node source, independently of a potential node source and cache managed by an MPT Forest instance. Thus, the caller need to make sure that any concurrently open MPT Forest has been flushed before calling this function to avoid reading out-dated or inconsistent data.
func visitAll(
	sourceFactory nodeSourceFactory,
	root mpt.NodeId,
	visitor mpt.NodeVisitor,
	pruneStorage bool,
) error {

	// The idea is to have workers processing a common queue of needed
	// nodes sorted by their position in the depth-first traversal of the
	// trie. The workers will fetch the nodes and put them into a shared
	// map of nodes. The main thread will consume the nodes from the map
	// and visit them.

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
	responsesMutex := sync.Mutex{}
	responsesCond := sync.NewCond(&responsesMutex)
	responses := map[mpt.NodeId]response{}

	done := atomic.Bool{}
	defer done.Store(true)

	requests.Add(request{nil, root})

	var workersDoneWg sync.WaitGroup
	var workersReadyWg sync.WaitGroup
	const NumWorker = 16
	workersDoneWg.Add(NumWorker)
	workersReadyWg.Add(NumWorker)
	workerErrorsChan := make(chan error, NumWorker)
	for i := 0; i < NumWorker; i++ {
		go func() {
			defer func() {
				workersDoneWg.Done()
			}()
			source, err := sourceFactory.open()
			if err != nil {
				workerErrorsChan <- err
				workersReadyWg.Done()
				return
			}
			workersReadyWg.Done()
			defer func() {
				if err := source.Close(); err != nil {
					workerErrorsChan <- err
				}
			}()
			for !done.Load() {
				// TODO: implement throttling
				// get the next job
				requestsMutex.Lock()
				req, present := requests.Pop()
				requestsMutex.Unlock()

				// process the request
				if !present {
					continue
				}

				// fetch the node and put it into the responses
				node, err := source.get(req.id)

				responsesMutex.Lock()
				responses[req.id] = response{node, err}
				responsesCond.Signal()
				responsesMutex.Unlock()

				// if there was a fetch error, stop the workers
				if err != nil {
					done.Store(true)
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
						pos := req.position.Child(byte(i))
						requests.Add(request{pos, child.Id()})
					}
					requestsMutex.Unlock()
				case *mpt.ExtensionNode:
					next := node.GetNext()
					requestsMutex.Lock()
					pos := req.position.Child(0)
					requests.Add(request{pos, next.Id()})
					requestsMutex.Unlock()
				case *mpt.AccountNode:
					if !pruneStorage {
						storage := node.GetStorage()
						id := storage.Id()
						if !id.IsEmpty() {
							requestsMutex.Lock()
							pos := req.position.Child(0)
							requests.Add(request{pos, id})
							requestsMutex.Unlock()
						}
					}
				}
			}
		}()
	}

	var err error
	// wait for all go routines to be start to check for init errors
	workersReadyWg.Wait()
	// read possible error
	var chRead bool
	for !chRead {
		select {
		case workerErr := <-workerErrorsChan:
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
	stack := []mpt.NodeId{root}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		var res response
		responsesMutex.Lock()
		for {
			found := false
			res, found = responses[cur]
			if found {
				delete(responses, cur)
				break
			}
			responsesCond.Wait()
		}
		responsesMutex.Unlock()

		if res.err != nil {
			err = res.err
			break
		}

		// we do not check for visitor response here,
		// as pruning or abortion is not supported
		visitor.Visit(res.node, mpt.NodeInfo{Id: cur})

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
			if !pruneStorage {
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
	workersDoneWg.Wait()
	close(workerErrorsChan)
	for workerErr := range workerErrorsChan {
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

func (p *position) Child(step byte) *position {
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

// ----------------------------------------------------------------------------
//                               nodeSource
// ----------------------------------------------------------------------------

// stockNodeSourceFactory is a nodeSourceFactory implementation that creates stock backed sources to access nodes.
type stockNodeSourceFactory struct {
	directory string
}

func (f *stockNodeSourceFactory) open() (nodeSource, error) {
	var toClose []io.Closer
	closeWithErr := func(err error) error {
		for _, s := range toClose {
			if e := s.Close(); e != nil {
				err = errors.Join(err, e)
			}
		}
		return err
	}

	accounts, err := file.OpenReadOnlyStock[uint64, mpt.AccountNode](path.Join(f.directory, "accounts"), mpt.AccountNodeWithPathLengthEncoderWithNodeHash{})
	if err != nil {
		return nil, closeWithErr(err)
	}
	toClose = append(toClose, accounts)

	branches, err := file.OpenReadOnlyStock[uint64, mpt.BranchNode](path.Join(f.directory, "branches"), mpt.BranchNodeEncoderWithNodeHash{})
	if err != nil {
		return nil, closeWithErr(err)
	}
	toClose = append(toClose, branches)

	extensions, err := file.OpenReadOnlyStock[uint64, mpt.ExtensionNode](path.Join(f.directory, "extensions"), mpt.ExtensionNodeEncoderWithNodeHash{})
	if err != nil {
		return nil, closeWithErr(err)
	}
	toClose = append(toClose, extensions)

	values, err := file.OpenReadOnlyStock[uint64, mpt.ValueNode](path.Join(f.directory, "values"), mpt.ValueNodeWithPathLengthEncoderWithNodeHash{})
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

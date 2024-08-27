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
	"log"
	"path"
	"sync"
	"sync/atomic"
)

//go:generate mockgen -source parallel_visit.go -destination parallel_visit_mocks.go -package io

// NodeSourceFactory is a factory for NodeSource instances.
// It provides access to nodes side to another infrastructure
// that already accesses the nodes.
type NodeSourceFactory interface {
	Open() (NodeSource, error)
}

// NodeSource is a source of nodes.
// It provides access to nodes by their ids.
type NodeSource interface {
	Get(mpt.NodeId) (mpt.Node, error)
	Close() error
}

// visitAll visits all nodes in the trie rooted at the given node in depth-first pre-order order.
// This function accesses nodes using its own read-only node source, independently of a potential node source and cache managed by an MPT Forest instance. Thus, the caller need to make sure that any concurrently open MPT Forest has been flushed before calling this function to avoid reading out-dated or inconsistent data.
func visitAll(
	sourceFactory NodeSourceFactory,
	root mpt.NodeId,
	visitor mpt.NodeVisitor,
	cutAtAccounts bool,
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

	const NumWorker = 16

	// open all sources for the number of workers and track possible errors
	sources := make([]NodeSource, 0, NumWorker)
	openingErrors := make([]error, 0, NumWorker)
	for i := 0; i < NumWorker; i++ {
		source, err := sourceFactory.Open()
		sources = append(sources, source)
		openingErrors = append(openingErrors, err)
	}

	defer func() {
		// only log errors when closing the sources
		// as data are opened parallel to the main program
		// and thus the error should not invalidate the result
		for _, source := range sources {
			if source != nil {
				if err := source.Close(); err != nil {
					log.Printf("failed to close source: %v", err)
				}
			}
		}
	}()

	// error in one of the sources interrupts processing
	if err := errors.Join(openingErrors...); err != nil {
		return err
	}

	for i := 0; i < NumWorker; i++ {
		go func(source NodeSource) {
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
				node, err := source.Get(req.id)

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
					if !cutAtAccounts {
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
		}(sources[i])
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
			return res.err
		}

		switch visitor.Visit(res.node, mpt.NodeInfo{Id: cur}) {
		case mpt.VisitResponseAbort:
			return nil
		case mpt.VisitResponsePrune:
			continue
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
			if !cutAtAccounts {
				storage := node.GetStorage()
				id := storage.Id()
				if !id.IsEmpty() {
					stack = append(stack, id)
				}
			}
		}
	}

	return nil
}

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
//                               NodeSource
// ----------------------------------------------------------------------------

type nodeSourceFactory struct {
	directory string
}

func (f *nodeSourceFactory) Open() (NodeSource, error) {
	var toClose []io.Closer
	closeWithErr := func(err error) error {
		for _, s := range toClose {
			if e := s.Close(); e != nil {
				err = errors.Join(err, e)
			}
		}
		return err
	}

	accounts, err := file.OpenReadOnlyStock[mpt.NodeId, mpt.AccountNode](path.Join(f.directory, "accounts"), mpt.AccountNodeWithPathLengthEncoderWithNodeHash{})
	if err != nil {
		return nil, closeWithErr(err)
	}
	toClose = append(toClose, accounts)

	branches, err := file.OpenReadOnlyStock[mpt.NodeId, mpt.BranchNode](path.Join(f.directory, "branches"), mpt.BranchNodeEncoderWithNodeHash{})
	if err != nil {
		return nil, closeWithErr(err)
	}
	toClose = append(toClose, branches)

	extensions, err := file.OpenReadOnlyStock[mpt.NodeId, mpt.ExtensionNode](path.Join(f.directory, "extensions"), mpt.ExtensionNodeEncoderWithNodeHash{})
	if err != nil {
		return nil, closeWithErr(err)
	}
	toClose = append(toClose, extensions)

	values, err := file.OpenReadOnlyStock[mpt.NodeId, mpt.ValueNode](path.Join(f.directory, "values"), mpt.ValueNodeWithPathLengthEncoderWithNodeHash{})
	if err != nil {
		return nil, closeWithErr(err)
	}
	toClose = append(toClose, values)

	return &nodeSource{
		accounts:   accounts,
		branches:   branches,
		extensions: extensions,
		values:     values,
	}, nil
}

type nodeSource struct {
	accounts   stock.ReadOnly[mpt.NodeId, mpt.AccountNode]
	branches   stock.ReadOnly[mpt.NodeId, mpt.BranchNode]
	extensions stock.ReadOnly[mpt.NodeId, mpt.ExtensionNode]
	values     stock.ReadOnly[mpt.NodeId, mpt.ValueNode]
}

func (s *nodeSource) Get(id mpt.NodeId) (mpt.Node, error) {
	if id.IsEmpty() {
		return mpt.EmptyNode{}, nil
	}
	if id.IsAccount() {
		res, err := s.accounts.Get(id)
		return &res, err
	}
	if id.IsBranch() {
		res, err := s.branches.Get(id)
		return &res, err
	}
	if id.IsExtension() {
		res, err := s.extensions.Get(id)
		return &res, err
	}
	if id.IsValue() {
		res, err := s.values.Get(id)
		return &res, err
	}
	return nil, fmt.Errorf("unknown node type: %v", id)
}

func (s *nodeSource) Close() error {
	return errors.Join(
		s.accounts.Close(),
		s.branches.Close(),
		s.extensions.Close(),
		s.values.Close(),
	)
}

package io

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
)

type NodeSourceFactory interface {
	Open() (NodeSource, error)
}

type NodeSource interface {
	Get(mpt.NodeId) (mpt.Node, error)
	Close() error
}

func visitAll(
	directory string,
	root mpt.NodeId,
	visitor mpt.NodeVisitor,
) error {
	if false {
		return visitAll_1(directory, root, visitor)
	}
	return visitAll_2(directory, root, visitor)
}

func visitAll_1(
	directory string,
	root mpt.NodeId,
	visitor mpt.NodeVisitor,
) error {

	fmt.Printf("Visiting all nodes in %s using parallel node fetching 1\n", directory)

	sourceFactory := &nodeSourceFactory{
		directory: directory,
	}

	type response struct {
		node mpt.Node
		err  error
	}

	type request struct {
		id  mpt.NodeId
		res chan<- response
	}

	requests := make(chan request, 100)
	defer close(requests)

	// Start goroutines fetching nodes in parallel.
	for i := 0; i < 16; i++ {
		source, err := sourceFactory.Open()
		if err != nil {
			return err
		}
		go func() {
			defer source.Close()
			for req := range requests {
				node, err := source.Get(req.id)
				req.res <- response{node, err}
			}
		}()
	}

	stack := []chan response{}

	scheduleLoad := func(ref mpt.NodeReference) {
		id := ref.Id()
		if id.IsEmpty() {
			return
		}
		res := make(chan response, 1)
		requests <- request{id, res}
		stack = append(stack, res)
	}

	counter := atomic.Int64{}
	last := int64(0)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				cur := counter.Load()
				fmt.Printf("Node rate: %f nodes/s\n", float64(cur-last)/10)
				last = cur
			case <-stop:
				return
			}
		}
	}()

	scheduleLoad(mpt.NewNodeReference(root))
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		res := <-cur
		if res.err != nil {
			return res.err
		}
		close(cur)

		node := res.node
		counter.Add(1)
		switch visitor.Visit(node, mpt.NodeInfo{}) {
		case mpt.VisitResponseAbort:
			return nil
		case mpt.VisitResponsePrune:
			continue
		case mpt.VisitResponseContinue:
			// nothing to do
		}

		switch cur := node.(type) {
		case *mpt.BranchNode:
			// add child nodes in reverse order to the stack
			children := cur.GetChildren()
			for i := len(children) - 1; i >= 0; i-- {
				scheduleLoad(children[i])
			}
		case *mpt.AccountNode:
			scheduleLoad(cur.GetStorage())
		case *mpt.ExtensionNode:
			scheduleLoad(cur.GetNext())
		case *mpt.ValueNode:
			// nothing to do
		}
	}

	return nil
}

func visitAll_2(
	directory string,
	root mpt.NodeId,
	visitor mpt.NodeVisitor,
) error {

	fmt.Printf("Visiting all nodes in %s using parallel node fetching 2\n", directory)

	sourceFactory := &nodeSourceFactory{
		directory: directory,
	}

	type response struct {
		nodes []mpt.Node
		err   error
	}

	type request struct {
		id  mpt.NodeId
		res chan<- response
	}

	requests := make(chan request, 100)
	defer close(requests)

	// Start goroutines fetching nodes in parallel.
	for i := 0; i < 16; i++ {
		source, err := sourceFactory.Open()
		if err != nil {
			return err
		}
		go func() {
			defer source.Close()
			for req := range requests {
				nodes, err := getLeftMostPath(req.id, source)
				req.res <- response{nodes, err}
			}
		}()
	}

	stack := []chan response{}

	scheduleLoad := func(ref mpt.NodeReference) {
		id := ref.Id()
		if id.IsEmpty() {
			return
		}
		res := make(chan response, 1)
		requests <- request{id, res}
		stack = append(stack, res)
	}

	counter := atomic.Int64{}
	last := int64(0)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				cur := counter.Load()
				fmt.Printf("Node rate: %f nodes/s\n", float64(cur-last)/10)
				last = cur
			case <-stop:
				return
			}
		}
	}()

	scheduleLoad(mpt.NewNodeReference(root))
outer:
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		res := <-cur
		if res.err != nil {
			return res.err
		}
		close(cur)

		for _, node := range res.nodes {
			counter.Add(1)
			switch visitor.Visit(node, mpt.NodeInfo{}) {
			case mpt.VisitResponseAbort:
				return nil
			case mpt.VisitResponsePrune:
				continue outer
			case mpt.VisitResponseContinue:
				// nothing to do
			}

			if branch, ok := node.(*mpt.BranchNode); ok {
				// add child nodes in reverse order to the stack
				// ignoring the left-most child
				leftMost := getLeftMostChild(branch)
				children := branch.GetChildren()
				for i := len(children) - 1; i >= 0; i-- {
					if children[i].Id() != leftMost {
						scheduleLoad(children[i])
					}
				}
			}
		}
	}

	return nil
}

func getLeftMostPath(id mpt.NodeId, source NodeSource) ([]mpt.Node, error) {
	path := make([]mpt.Node, 0, 50)
	for {
		node, err := source.Get(id)
		if err != nil {
			return nil, err
		}
		path = append(path, node)

		switch cur := node.(type) {
		case *mpt.BranchNode:
			id = getLeftMostChild(cur)
		case *mpt.ExtensionNode:
			ref := cur.GetNext()
			id = ref.Id()
		case *mpt.AccountNode:
			ref := cur.GetStorage()
			id = ref.Id()
		default:
			return path, nil
		}
	}
}

func getLeftMostChild(branch *mpt.BranchNode) mpt.NodeId {
	for _, child := range branch.GetChildren() {
		id := child.Id()
		if !id.IsEmpty() {
			return id
		}
	}
	panic("no left-most child")
}

type nodeSourceFactory struct {
	directory string
}

func (f *nodeSourceFactory) Open() (NodeSource, error) {
	accounts, err := openSource[mpt.AccountNode](f.directory, "accounts", mpt.AccountNodeWithPathLengthEncoderWithNodeHash{})
	if err != nil {
		return nil, err
	}
	branches, err := openSource[mpt.BranchNode](f.directory, "branches", mpt.BranchNodeEncoderWithNodeHash{})
	if err != nil {
		return nil, err
	}
	extensions, err := openSource[mpt.ExtensionNode](f.directory, "extensions", mpt.ExtensionNodeEncoderWithNodeHash{})
	if err != nil {
		return nil, err
	}
	values, err := openSource[mpt.ValueNode](f.directory, "values", mpt.ValueNodeWithPathLengthEncoderWithNodeHash{})
	if err != nil {
		return nil, err
	}
	return &nodeSource{
		accounts:   accounts,
		branches:   branches,
		extensions: extensions,
		values:     values,
	}, nil
}

type nodeSource struct {
	accounts   source[mpt.AccountNode]
	branches   source[mpt.BranchNode]
	extensions source[mpt.ExtensionNode]
	values     source[mpt.ValueNode]
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

type source[T any] struct {
	file    *os.File
	encoder stock.ValueEncoder[T]
	buffer  []byte
}

func openSource[T any](directory, name string, encoder stock.ValueEncoder[T]) (source[T], error) {
	file, err := os.Open(fmt.Sprintf("%s/%s/values.dat", directory, name))
	if err != nil {
		return source[T]{}, err
	}
	return source[T]{
		file:    file,
		encoder: encoder,
		buffer:  make([]byte, encoder.GetEncodedSize()),
	}, nil
}

func (s *source[T]) Get(id mpt.NodeId) (T, error) {
	pos := id.Index()
	var res T
	_, err := s.file.Seek(int64(pos)*int64(s.encoder.GetEncodedSize()), 0)
	if err != nil {
		return res, err
	}
	_, err = s.file.Read(s.buffer)
	if err != nil {
		return res, err
	}
	if err := s.encoder.Load(s.buffer, &res); err != nil {
		return res, err
	}
	return res, nil
}

func (s *source[T]) Close() error {
	return s.file.Close()
}

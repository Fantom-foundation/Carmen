package io

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/io/heap"
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
	cutAtAccounts bool,
) error {
	if false {
		return visitAll_1(directory, root, visitor)
	}
	if false {
		return visitAll_2(directory, root, visitor)
	}
	if false {
		return visitAll_3(directory, root, visitor)
	}
	if false {
		return visitAll_4(directory, root, visitor, cutAtAccounts)
	}
	if false {
		return visitAll_4_stats(directory, root, cutAtAccounts)
	}
	if false {
		return visitAll_5(directory, root, visitor, cutAtAccounts)
	}
	return visitAll_6(directory, root, visitor, cutAtAccounts)
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

// -- Variant 3 --

func visitAll_3(
	directory string,
	root mpt.NodeId,
	visitor mpt.NodeVisitor,
) error {

	const PatchDepth = 3
	const NumWorkers = 16
	maxLeaves := int(math.Pow(16, PatchDepth+1))

	fmt.Printf("Visiting all nodes in %s using parallel node fetching 3\n", directory)

	sourceFactory := &nodeSourceFactory{
		directory: directory,
	}

	type response struct {
		patch triePatch
		err   error
	}

	type request struct {
		id  mpt.NodeId
		res chan<- response
	}

	requests := make([]request, 0, 10*maxLeaves)
	requestsMutex := sync.Mutex{}
	done := atomic.Bool{}
	defer done.Store(true)

	// Start goroutines fetching nodes in parallel.
	for i := 0; i < NumWorkers; i++ {
		source, err := sourceFactory.Open()
		if err != nil {
			return err
		}
		go func() {
			defer source.Close()
			for !done.Load() {
				requestsMutex.Lock()
				if len(requests) == 0 {
					requestsMutex.Unlock()
					time.Sleep(10 * time.Millisecond)
					continue
				}
				req := requests[len(requests)-1]
				requests = requests[:len(requests)-1]
				requestsMutex.Unlock()

				patch, err := getTriePatch(req.id, source, PatchDepth)
				if err == nil && len(patch.leaves) > 4000 {
					fmt.Printf("Fetched %d nodes and %d leaves\n", len(patch.nodes), len(patch.leaves))
				}

				/*
					if err == nil {
						fmt.Printf("Fetched %d nodes and %d leaves\n", len(patch.nodes), len(patch.leaves))
					} else {
						fmt.Printf("Failed to fetch patch for %v: %v\n", req.id, err)
					}
				*/
				req.res <- response{patch, err}
			}
		}()
	}

	jobs := map[mpt.NodeId]chan response{}
	scheduleLoad := func(id mpt.NodeId) {
		if id.IsEmpty() {
			return
		}
		res := make(chan response, 1)
		requestsMutex.Lock()
		requests = append(requests, request{id, res})
		requestsMutex.Unlock()
		jobs[id] = res
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

	scheduleLoad(root)
	res := <-jobs[root]
	delete(jobs, root)
	if res.err != nil {
		return res.err
	}
	for _, leave := range res.patch.leaves {
		scheduleLoad(leave)
	}

	visit := mpt.MakeVisitor(func(node mpt.Node, info mpt.NodeInfo) mpt.VisitResponse {
		counter.Add(1)
		return visitor.Visit(node, info)
	})

	_, err := res.patch.visit(visit, func(id mpt.NodeId) (*triePatch, error) {
		res := <-jobs[id]
		delete(jobs, id)
		if res.err != nil {
			return nil, res.err
		}
		for _, leave := range res.patch.leaves {
			scheduleLoad(leave)
		}
		return &res.patch, nil
	})

	return err
}

type triePatch struct {
	root   mpt.NodeId
	nodes  map[mpt.NodeId]mpt.Node
	leaves []mpt.NodeId
}

func getTriePatch(
	id mpt.NodeId,
	source NodeSource,
	maxDepth int,
) (
	triePatch,
	error,
) {
	nodes := make(map[mpt.NodeId]mpt.Node)
	leaves := make([]mpt.NodeId, 0, int(math.Pow(16, float64(maxDepth+1))))

	type entry struct {
		id    mpt.NodeId
		depth int
	}

	queue := []entry{{id, 0}}
	for len(queue) > 0 {
		cur := queue[len(queue)-1]
		queue = queue[:len(queue)-1]

		node, err := source.Get(cur.id)
		if err != nil {
			return triePatch{}, err
		}
		nodes[cur.id] = node

		consume := func(id mpt.NodeId) {
			if !id.IsEmpty() {
				queue = append(queue, entry{id, cur.depth + 1})
			}
		}
		if cur.depth >= maxDepth {
			consume = func(id mpt.NodeId) {
				if !id.IsEmpty() {
					leaves = append(leaves, id)
				}
			}
		}

		switch node := node.(type) {
		case *mpt.BranchNode:
			for _, child := range node.GetChildren() {
				consume(child.Id())
			}
		case *mpt.ExtensionNode:
			next := node.GetNext()
			consume(next.Id())
		case *mpt.AccountNode:
			storage := node.GetStorage()
			consume(storage.Id())
		}
	}
	return triePatch{
		root:   id,
		nodes:  nodes,
		leaves: leaves,
	}, nil
}

func (p *triePatch) visit(
	visitor mpt.NodeVisitor,
	source func(mpt.NodeId) (*triePatch, error),
) (mpt.VisitResponse, error) {
	stack := []mpt.NodeId{p.root}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		node, found := p.nodes[cur]
		if !found {
			patch, err := source(cur)
			if err != nil {
				return mpt.VisitResponseAbort, err
			}
			res, err := patch.visit(visitor, source)
			if err != nil {
				return mpt.VisitResponseAbort, err
			}
			if res == mpt.VisitResponseAbort {
				return mpt.VisitResponseAbort, nil
			}
			continue
		}

		switch visitor.Visit(node, mpt.NodeInfo{Id: cur}) {
		case mpt.VisitResponseAbort:
			return mpt.VisitResponseAbort, nil
		case mpt.VisitResponsePrune:
			continue
		}

		switch node := node.(type) {
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
			storage := node.GetStorage()
			id := storage.Id()
			if !id.IsEmpty() {
				stack = append(stack, id)
			}
		}
	}
	return mpt.VisitResponseContinue, nil
}

// -- Variant 4 --

func visitAll_4(
	directory string,
	root mpt.NodeId,
	visitor mpt.NodeVisitor,
	cutAtAccounts bool,
) error {
	const TipHeight = 4
	const NumWorker = 16

	fmt.Printf("Visiting all nodes in %s using parallel node fetching 4\n", directory)

	sourceFactory := &nodeSourceFactory{
		directory: directory,
	}

	source, err := sourceFactory.Open()
	if err != nil {
		return err
	}
	defer source.Close()

	// Start by consuming the first 3 layers of the trie.
	patch, err := getTrieTip(root, source, TipHeight, cutAtAccounts)
	if err != nil {
		return err
	}

	fmt.Printf("Fetched tip with %d leaves\n", len(patch.leaves))
	//fmt.Printf("Leaves: %v\n", patch.leaves)

	type response struct {
		trie subTrie
		err  error
	}

	// Allocate buffers for requests and responses.
	requests := make([]mpt.NodeId, len(patch.leaves))
	responses := make([]chan response, len(patch.leaves))
	for i, leave := range patch.leaves {
		requests[i] = leave
		responses[i] = make(chan response, 1)
	}

	const CapacityLimit = 500_000_000
	capacityMutex := sync.Mutex{}
	capacityCond := sync.NewCond(&capacityMutex)
	capacity := int32(0)
	nextJob := 0

	var wg sync.WaitGroup
	wg.Add(NumWorker)
	for i := 0; i < NumWorker; i++ {
		go func() {
			source, err := sourceFactory.Open()
			if err != nil {
				panic(err)
			}
			defer source.Close()
			defer wg.Done()
			// TODO: stop this loop on failure
			capacityMutex.Lock()
			for {
				for capacity >= CapacityLimit {
					capacityCond.Wait()
				}
				if nextJob >= len(requests) {
					capacityMutex.Unlock()
					return
				}
				job := nextJob
				nextJob += 1
				capacityMutex.Unlock()

				trie, err := getSubTrie(requests[job], source, cutAtAccounts)
				responses[job] <- response{trie, err}

				capacityMutex.Lock()
				capacity += int32(len(trie.nodes))
				//fmt.Printf("Capacity: %d (+%d)\n", capacity, len(trie.nodes))
			}
		}()
	}

	// Start a progress counter.
	counter := atomic.Int64{}
	last := int64(0)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		start := time.Now()
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				cur := counter.Load()
				duration := time.Since(start).Seconds()
				fmt.Printf("Node rate: %.1f nodes/s / overall %.1f nodes/s\n", float64(cur-last)/10, float64(cur)/duration)
				last = cur
			case <-stop:
				return
			}
		}
	}()

	countingVisitor := mpt.MakeVisitor(func(node mpt.Node, info mpt.NodeInfo) mpt.VisitResponse {
		counter.Add(1)
		return visitor.Visit(node, info)
	})

	nextSubTrie := 0
	stack := make([]mpt.NodeId, 0, 50)
	stack = append(stack, patch.root)
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		node, found := patch.nodes[cur]
		if !found {
			//fmt.Printf("Fetching node %v\n", cur)
			c := responses[nextSubTrie]
			res := <-c
			close(c)
			nextSubTrie += 1
			if res.err != nil {
				return res.err
			}
			if res.trie.root != cur {
				panic(fmt.Sprintf("unexpected node, wanted %v, got %v", cur, res.trie.root))
			}
			if err := res.trie.visit(countingVisitor); err != nil {
				return err
			}

			capacityMutex.Lock()
			capacity -= int32(len(res.trie.nodes))
			//fmt.Printf("Capacity: %d (-%d)\n", capacity, len(res.trie.nodes))
			capacityCond.Broadcast()
			capacityMutex.Unlock()
			continue
		}

		switch countingVisitor.Visit(node, mpt.NodeInfo{Id: cur}) {
		case mpt.VisitResponseAbort:
			return nil
		case mpt.VisitResponsePrune:
			continue
		}

		switch node := node.(type) {
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
			storage := node.GetStorage()
			id := storage.Id()
			if !id.IsEmpty() {
				stack = append(stack, id)
			}
		}
	}

	return nil

}

func getTrieTip(
	id mpt.NodeId,
	source NodeSource,
	maxDepth int,
	cutAtAccounts bool,
) (
	triePatch,
	error,
) {
	nodes := make(map[mpt.NodeId]mpt.Node)
	leaves := make([]mpt.NodeId, 0, int(math.Pow(16, float64(maxDepth+1))))

	type entry struct {
		id    mpt.NodeId
		depth int
	}

	queue := []entry{{id, 0}}
	for len(queue) > 0 {
		cur := queue[len(queue)-1]
		queue = queue[:len(queue)-1]

		node, err := source.Get(cur.id)
		if err != nil {
			return triePatch{}, err
		}
		nodes[cur.id] = node

		consume := func(id mpt.NodeId) {
			if !id.IsEmpty() {
				queue = append(queue, entry{id, cur.depth + 1})
			}
		}
		if cur.depth >= maxDepth {
			consume = func(id mpt.NodeId) {
				if !id.IsEmpty() {
					leaves = append(leaves, id)
				}
			}
		}

		switch node := node.(type) {
		case *mpt.BranchNode:
			children := node.GetChildren()
			if cur.depth >= maxDepth {
				for _, child := range children {
					consume(child.Id())
				}
			} else {
				for i := len(children) - 1; i >= 0; i-- {
					consume(children[i].Id())
				}
			}
		case *mpt.ExtensionNode:
			next := node.GetNext()
			consume(next.Id())
		case *mpt.AccountNode:
			if !cutAtAccounts {
				storage := node.GetStorage()
				consume(storage.Id())
			}
		}
	}
	return triePatch{
		root:   id,
		nodes:  nodes,
		leaves: leaves,
	}, nil
}

type subTrie struct {
	root  mpt.NodeId
	nodes map[mpt.NodeId]mpt.Node
}

func getSubTrie(root mpt.NodeId, source NodeSource, cutAtAccounts bool) (subTrie, error) {
	nodes := make(map[mpt.NodeId]mpt.Node)
	stack := make([]mpt.NodeId, 0, 50)
	stack = append(stack, root)
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		node, err := source.Get(cur)
		if err != nil {
			return subTrie{}, err
		}

		nodes[cur] = node

		switch node := node.(type) {
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
	return subTrie{root, nodes}, nil
}

func (t *subTrie) visit(visitor mpt.NodeVisitor) error {
	stack := make([]mpt.NodeId, 0, 50)
	stack = append(stack, t.root)
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		node, found := t.nodes[cur]
		if !found {
			panic(fmt.Sprintf("node %v not found", cur))
		}

		switch visitor.Visit(node, mpt.NodeInfo{Id: cur}) {
		case mpt.VisitResponseAbort:
			return nil
		case mpt.VisitResponsePrune:
			continue
		}

		switch node := node.(type) {
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
			storage := node.GetStorage()
			id := storage.Id()
			if !id.IsEmpty() {
				stack = append(stack, id)
			}
		}
	}
	return nil
}

func visitAll_4_stats(
	directory string,
	root mpt.NodeId,
	cutAtAccounts bool,
) error {
	const TipHeight = 4
	const NumWorker = 16

	fmt.Printf("Visiting all nodes in %s using parallel node fetching 4 (stats)\n", directory)

	sourceFactory := &nodeSourceFactory{
		directory: directory,
	}

	source, err := sourceFactory.Open()
	if err != nil {
		return err
	}
	defer source.Close()

	// Start by consuming the first 3 layers of the trie.
	patch, err := getTrieTip(root, source, TipHeight, cutAtAccounts)
	if err != nil {
		return err
	}

	fmt.Printf("Fetched tip with %d leaves\n", len(patch.leaves))
	//fmt.Printf("Leaves: %v\n", patch.leaves)

	type response struct {
		numNodes int
		err      error
	}

	// Allocate buffers for requests and responses.
	requests := make([]mpt.NodeId, len(patch.leaves))
	responses := make([]response, len(patch.leaves))
	for i, leave := range patch.leaves {
		requests[i] = leave
	}

	nextJob := atomic.Int32{}

	var wg sync.WaitGroup
	wg.Add(NumWorker)
	for i := 0; i < NumWorker; i++ {
		go func() {
			source, err := sourceFactory.Open()
			if err != nil {
				panic(err)
			}
			defer source.Close()
			defer wg.Done()
			for {
				job := nextJob.Add(1) - 1
				if job >= int32(len(requests)) {
					return
				}
				size, err := getSubTrieSize(requests[job], source, cutAtAccounts)
				responses[job] = response{size, err}
				if err != nil {
					fmt.Printf("Failed to fetch subtrie for job %d: %v\n", job, err)
				} else {
					fmt.Printf("Fetched subtrie for job %d with %d nodes\n", job, size)
				}
			}
		}()
	}

	wg.Wait()

	for i := 0; i < len(responses); i++ {
		res := responses[i]
		if res.err != nil {
			fmt.Printf("%d, %v\n", i, res.err)
		} else {
			fmt.Printf("%d, %d\n", i, res.numNodes)
		}
	}

	return nil
}

func getSubTrieSize(root mpt.NodeId, source NodeSource, cutAtAccounts bool) (int, error) {
	counter := 0
	stack := make([]mpt.NodeId, 0, 50)
	stack = append(stack, root)
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		node, err := source.Get(cur)
		if err != nil {
			return 0, err
		}

		counter++

		switch node := node.(type) {
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
	return counter, nil
}

// -- Variant 5 --

func visitAll_5(
	directory string,
	root mpt.NodeId,
	visitor mpt.NodeVisitor,
	cutAtAccounts bool,
) error {
	const TipHeight = 4
	const NumWorker = 16

	fmt.Printf("Visiting all nodes in %s using parallel node fetching 5\n", directory)

	sourceFactory := &nodeSourceFactory{
		directory: directory,
	}

	source, err := sourceFactory.Open()
	if err != nil {
		return err
	}
	defer source.Close()

	// Step 1: load all accounts.

	// Start by loading the tip of the trie.
	patch, err := getTrieTip(root, source, TipHeight, true)
	if err != nil {
		return err
	}

	fmt.Printf("Fetched tip with %d leaves\n", len(patch.leaves))
	//fmt.Printf("Leaves: %v\n", patch.leaves)

	// Start a progress counter.
	counter := atomic.Int64{}
	last := int64(0)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		start := time.Now()
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				cur := counter.Load()
				duration := time.Since(start).Seconds()
				fmt.Printf("Node rate: %.1f nodes/s / overall %.1f nodes/s\n", float64(cur-last)/10, float64(cur)/duration)
				last = cur
			case <-stop:
				return
			}
		}
	}()

	type accountInfo struct {
		accountId mpt.NodeId
		storageId mpt.NodeId
	}

	accountIds := make([]mpt.NodeId, 0, 5_000_000)
	accountIdsCollector := mpt.MakeVisitor(func(node mpt.Node, info mpt.NodeInfo) mpt.VisitResponse {
		counter.Add(1)
		if _, ok := node.(*mpt.AccountNode); ok {
			if cutAtAccounts {
				visitor.Visit(node, info)
			}
			accountIds = append(accountIds, info.Id)
			return mpt.VisitResponsePrune
		}
		return mpt.VisitResponseContinue
	})

	if err := visitTrieUnderTip(patch, sourceFactory, accountIdsCollector); err != nil {
		return err
	}

	fmt.Printf("Loaded %d accounts\n", len(accountIds))

	if cutAtAccounts {
		return nil
	}

	// Step 2: load all account storages.

	type tipResponse struct {
		node mpt.Node
		tip  triePatch
		err  error
	}

	storageTips := make([]chan tipResponse, len(accountIds))
	for i := range accountIds {
		storageTips[i] = make(chan tipResponse, 1)
	}

	// Start a team of workers just fetching storage trie tips.
	const CapacityLimit = 1_000
	capacityMutex := sync.Mutex{}
	capacityCond := sync.NewCond(&capacityMutex)
	capacity := int32(0)
	nextJob := 0

	for i := 0; i < NumWorker; i++ {
		go func() {
			source, err := sourceFactory.Open()
			if err != nil {
				panic(err)
			}
			defer source.Close()
			// TODO: stop this loop on failure
			capacityMutex.Lock()
			for {
				for capacity >= CapacityLimit {
					capacityCond.Wait()
				}
				if nextJob >= len(storageTips) {
					capacityMutex.Unlock()
					return
				}
				job := nextJob
				nextJob += 1
				capacityMutex.Unlock()

				accountId := accountIds[job]
				node, err := source.Get(accountId)
				if err != nil {
					storageTips[job] <- tipResponse{nil, triePatch{}, err}
				}
				ref := node.(*mpt.AccountNode).GetStorage()
				id := ref.Id()
				tip := triePatch{}
				if !id.IsEmpty() {
					tip, err = getTrieTip(id, source, TipHeight, false)
				}
				storageTips[job] <- tipResponse{node, tip, err}

				capacityMutex.Lock()
				capacity += int32(len(tip.nodes))
				//fmt.Printf("Capacity: %d (+%d)\n", capacity, len(tip.nodes))
			}
		}()
	}

	countingVisitor := mpt.MakeVisitor(func(node mpt.Node, info mpt.NodeInfo) mpt.VisitResponse {
		counter.Add(1)
		return visitor.Visit(node, info)
	})

	// Consume the storage of accounts in order.
	for i, accountId := range accountIds {
		// Get the account node fetched async plus a potential storage
		// tree tip to consume the storage state.
		res := <-storageTips[i]
		close(storageTips[i])
		if res.err != nil {
			return res.err
		}

		// visit the account first
		countingVisitor.Visit(res.node, mpt.NodeInfo{Id: accountId})

		ref := res.node.(*mpt.AccountNode).GetStorage()
		storageId := ref.Id()
		if storageId.IsEmpty() {
			continue
		}

		tip := res.tip

		if err := visitTrieUnderTip(tip, sourceFactory, countingVisitor); err != nil {
			return err
		}

		capacityMutex.Lock()
		capacity -= int32(len(tip.nodes))
		capacityCond.Broadcast()
		//fmt.Printf("Capacity: %d (-%d)\n", capacity, len(tip.nodes))
		capacityMutex.Unlock()
	}

	return nil

}

func visitTrieUnderTip(
	tip triePatch,
	sourceFactory *nodeSourceFactory,
	visitor mpt.NodeVisitor,
) error {
	const NumWorker = 16

	type response struct {
		trie subTrie
		err  error
	}

	// Allocate buffers for requests and responses.
	requests := tip.leaves
	responses := make([]chan response, len(tip.leaves))
	for i := range requests {
		responses[i] = make(chan response, 1)
	}

	const CapacityLimit = 10_000
	capacityMutex := sync.Mutex{}
	capacityCond := sync.NewCond(&capacityMutex)
	capacity := int32(0)
	nextJob := 0

	var wg sync.WaitGroup
	wg.Add(NumWorker)
	for i := 0; i < NumWorker; i++ {
		go func() {
			source, err := sourceFactory.Open()
			if err != nil {
				panic(err)
			}
			defer source.Close()
			defer wg.Done()
			// TODO: stop this loop on failure
			capacityMutex.Lock()
			for {
				for capacity >= CapacityLimit {
					capacityCond.Wait()
				}
				if nextJob >= len(requests) {
					capacityMutex.Unlock()
					return
				}
				job := nextJob
				nextJob += 1
				capacityMutex.Unlock()

				trie, err := getSubTrie(requests[job], source, true)
				responses[job] <- response{trie, err}

				capacityMutex.Lock()
				capacity += int32(len(trie.nodes))
				//fmt.Printf("Capacity: %d (+%d)\n", capacity, len(trie.nodes))
			}
		}()
	}

	nextSubTrie := 0
	stack := make([]mpt.NodeId, 0, 50)
	stack = append(stack, tip.root)
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		node, found := tip.nodes[cur]
		if !found {
			//fmt.Printf("Fetching node %v\n", cur)
			c := responses[nextSubTrie]
			res := <-c
			close(c)
			nextSubTrie += 1
			if res.err != nil {
				return res.err
			}
			if res.trie.root != cur {
				panic(fmt.Sprintf("unexpected node, wanted %v, got %v", cur, res.trie.root))
			}
			if err := res.trie.visit(visitor); err != nil {
				return err
			}

			capacityMutex.Lock()
			capacity -= int32(len(res.trie.nodes))
			//fmt.Printf("Capacity: %d (-%d)\n", capacity, len(res.trie.nodes))
			capacityCond.Broadcast()
			capacityMutex.Unlock()
			continue
		}

		switch visitor.Visit(node, mpt.NodeInfo{Id: cur}) {
		case mpt.VisitResponseAbort:
			return nil
		case mpt.VisitResponsePrune:
			continue
		}

		switch node := node.(type) {
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
		}
	}
	return nil
}

// -- Variant 6 --

func visitAll_6(
	directory string,
	root mpt.NodeId,
	visitor mpt.NodeVisitor,
	cutAtAccounts bool,
) error {
	fmt.Printf("Visiting all nodes in %s using parallel node fetching 6\n", directory)

	const debug = true

	sourceFactory := &nodeSourceFactory{
		directory: directory,
	}

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
	for i := 0; i < NumWorker; i++ {
		go func() {
			source, err := sourceFactory.Open()
			if err != nil {
				panic(err)
			}
			defer source.Close()
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
				if debug {
					fmt.Printf("Fetching %v (%v) ...\n", req.position, req.id)
				}
				node, err := source.Get(req.id)

				responsesMutex.Lock()
				responses[req.id] = response{node, err}
				responsesCond.Signal()
				responsesMutex.Unlock()
				if debug {
					fmt.Printf("Fetched %v (%v) ...\n", req.position, req.id)
				}

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
		}()
	}

	// Start a progress counter.
	counter := atomic.Int64{}
	last := int64(0)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		start := time.Now()
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				cur := counter.Load()
				duration := time.Since(start).Seconds()
				fmt.Printf("Node rate: %.1f nodes/s / overall %.1f nodes/s\n", float64(cur-last)/10, float64(cur)/duration)
				last = cur
			case <-stop:
				return
			}
		}
	}()

	countingVisitor := mpt.MakeVisitor(func(node mpt.Node, info mpt.NodeInfo) mpt.VisitResponse {
		counter.Add(1)
		return visitor.Visit(node, info)
	})

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
			if debug {
				fmt.Printf("Waiting for %v (buffer size %d)...\n", cur, len(responses))
			}
			responsesCond.Wait()
		}
		responsesMutex.Unlock()

		if res.err != nil {
			return nil
		}

		switch countingVisitor.Visit(res.node, mpt.NodeInfo{Id: cur}) {
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

	fmt.Printf("Lost %d nodes\n", len(responses))

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

func (a *position) compare(b *position) int {
	if a == b {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// make sure a is the shorter one
	if a.len > b.len {
		return b.compare(a) * -1
	}

	// reduce the length of b to match a
	bIsLonger := a.len < b.len
	for a.len < b.len {
		b = b.parent
	}

	// compare the common part
	prefixResult := a._compare(b)
	if prefixResult != 0 {
		return prefixResult
	}
	if bIsLonger {
		return -1
	}
	return 0
}

func (a *position) _compare(b *position) int {
	if a == b {
		return 0
	}
	prefixResult := a.parent._compare(b.parent)
	if prefixResult != 0 {
		return prefixResult
	}
	if a.pos < b.pos {
		return -1
	}
	if a.pos > b.pos {
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

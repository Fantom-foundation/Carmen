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
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"

	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"go.uber.org/mock/gomock"
)

var allMptConfigs = []mpt.MptConfig{
	mpt.S4LiveConfig, mpt.S4ArchiveConfig,
	mpt.S5LiveConfig, mpt.S5ArchiveConfig,
}

func TestPosition_CanBeCreatedAndPrinted(t *testing.T) {
	tests := []struct {
		steps []byte
		print string
	}{
		{nil, ""},
		{[]byte{0}, "0"},
		{[]byte{1}, "1"},
		{[]byte{0, 0}, "0.0"},
		{[]byte{0, 1}, "0.1"},
		{[]byte{1, 0}, "1.0"},
		{[]byte{1, 2, 3}, "1.2.3"},
	}

	for _, test := range tests {
		pos := newPosition(test.steps)
		if pos.String() != test.print {
			t.Errorf("expected %s, got %s", test.print, pos.String())
		}
	}
}

func TestPosition_ChildrenCanBeDerived(t *testing.T) {
	tests := []struct {
		base  []byte
		step  byte
		print string
	}{
		{nil, 1, "1"},
		{nil, 2, "2"},
		{[]byte{1, 2}, 3, "1.2.3"},
	}

	for _, test := range tests {
		pos := newPosition(test.base)
		pos = pos.child(test.step)
		if pos.String() != test.print {
			t.Errorf("expected %s, got %s", test.print, pos.String())
		}
	}
}

func TestPosition_AreOrdered(t *testing.T) {
	paths := [][]byte{
		nil,
		{1},
		{1, 2},
		{1, 2, 3},
		{2, 2, 3},
	}

	for _, a := range paths {
		for _, b := range paths {
			aa := newPosition(a)
			bb := newPosition(b)
			want := bytes.Compare(a, b)
			if got := aa.compare(bb); got != want {
				t.Errorf("expected compare(%v,%v)=%d, got %d", aa, bb, got, want)
			}
		}
	}
}

func TestPosition_AreOrderedAndWorkWithSharedPrefixes(t *testing.T) {
	positions := []*position{}
	var position *position
	positions = append(positions, position)
	for i := 0; i < 5; i++ {
		position = position.child(byte(i))
		positions = append(positions, position)
	}

	for _, a := range positions {
		for _, b := range positions {
			want := strings.Compare(a.String(), b.String())
			if got := a.compare(b); got != want {
				t.Errorf("expected compare(%v,%v)=%d, got %d", a, b, got, want)
			}
		}
	}
}

func TestNodeSource_CanRead_Nodes(t *testing.T) {
	for _, config := range allMptConfigs {
		config := config
		t.Run(config.Name, func(t *testing.T) {
			runTestWithArchive(t, config, func(trie *mpt.ArchiveTrie) {
				t.Parallel()

				// trie must be flushed before opening the parallel source
				if err := trie.Flush(); err != nil {
					t.Fatalf("failed to flush archive: %v", err)
				}

				blocks, _, err := trie.GetBlockHeight()
				if err != nil {
					t.Fatalf("failed to get block height: %v", err)
				}

				factory := stockNodeSourceFactory{directory: trie.Directory(), config: trie.GetConfig()}
				source, err := factory.open()
				if err != nil {
					t.Fatalf("failed to open node source: %v", err)
				}

				// iterate all nodes in the trie for all blocks
				for i := uint64(0); i <= blocks; i++ {
					if err := trie.VisitTrie(i, mpt.MakeVisitor(func(node mpt.Node, info mpt.NodeInfo) mpt.VisitResponse {
						sourceNode, err := source.get(info.Id)
						if err != nil {
							t.Fatalf("failed to get node from source: %v", err)
						}
						matchNodes(t, node, sourceNode)
						return mpt.VisitResponseContinue
					})); err != nil {
						t.Fatalf("failed to visit trie: %v", err)
					}
				}
			})
		})
	}
}

func TestVisit_CanHandleSlowConsumer(t *testing.T) {
	// Create a reasonable large trie.
	config := mpt.S5LiveConfig
	dir := t.TempDir()
	live, err := mpt.OpenGoFileState(dir, config, mpt.NodeCacheConfig{Capacity: 1024})
	if err != nil {
		t.Fatalf("failed to open live db: %v", err)
	}
	defer func() {
		if err := live.Close(); err != nil {
			t.Fatalf("failed to close live db: %v", err)
		}
	}()

	addr := common.Address{}
	err = errors.Join(
		live.CreateAccount(addr),
		live.SetNonce(addr, common.Nonce{1}),
	)
	if err != nil {
		t.Fatalf("failed to create account: %v", err)
	}
	for i := 0; i < 100; i++ {
		for j := 0; j < 100; j++ {
			key := common.Key{byte(i), byte(j)}
			err = live.SetStorage(addr, key, common.Value{1})
			if err != nil {
				t.Fatalf("failed to set storage: %v", err)
			}
		}
		if _, err := live.GetHash(); err != nil {
			t.Fatalf("failed to get hash: %v", err)
		}
	}
	if err := live.Flush(); err != nil {
		t.Fatalf("failed to flush live db: %v", err)
	}

	numNodes := 0
	err = live.Visit(mpt.MakeVisitor(func(node mpt.Node, info mpt.NodeInfo) mpt.VisitResponse {
		numNodes++
		return mpt.VisitResponseContinue
	}))
	if err != nil {
		t.Fatalf("failed to visit trie: %v", err)
	}

	root := live.GetRootId()

	// This visitor is stalling from time to time providing the pre-fetcher
	// workers room to rush ahead and filling up the prefetch buffer.
	numVisited := 0
	visitor := makeNoResponseVisitor(func(mpt.Node, mpt.NodeInfo) error {
		numVisited++
		if numVisited%1000 == 0 {
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	})

	err = visitAllWithConfig(
		&stockNodeSourceFactory{dir, config},
		root,
		visitor,
		visitAllConfig{
			pruneStorage:      false,
			numWorker:         4,
			throttleThreshold: 100,
			batchSize:         1,
			monitor: func(numResponses int) {
				// The actual upper limit is a combination of the threshold for
				// throttling, the number of workers, the batch size, and the
				// structure of the trie. The limit used here is a conservative
				// upper bound which would get exceeded by a factor of 10 if the
				// workers were not throttled.
				if got, limit := numResponses, 200; got > limit {
					t.Errorf("expected at most %d responses, got %d", limit, got)
				}
			},
		},
	)
	if err != nil {
		t.Fatalf("failed to visit all nodes: %v", err)
	}

	if numNodes != numVisited {
		t.Errorf("expected %d nodes, got %d", numNodes, numVisited)
	}
}

func TestVisit_Nodes_Failing_CannotOpenDir(t *testing.T) {
	for _, config := range allMptConfigs {
		config := config
		t.Run(config.Name, func(t *testing.T) {
			t.Parallel()

			dir := path.Join(t.TempDir(), "missing")
			if err := visitAll(dir, config, mpt.EmptyId(), nil, false); err == nil {
				t.Errorf("expected error, got nil")
			}
		})
	}
}

func TestVisit_Nodes_Failing_MissingDir(t *testing.T) {
	for _, config := range allMptConfigs {
		config := config
		t.Run(config.Name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			trie := createArchive(t, dir, config)
			root, err := trie.GetBlockRoot(0)
			if err != nil {
				t.Fatalf("failed to get block root: %v", err)
			}

			if err := trie.Close(); err != nil {
				t.Fatalf("failed to close archive: %v", err)
			}

			// corrupt the directory by removing the files
			if err := os.RemoveAll(path.Join(dir, "accounts")); err != nil {
				t.Fatalf("failed to remove directory: %v", err)
			}

			if err := visitAll(dir, config, root, nil, false); err == nil {
				t.Errorf("expected error, got nil")
			}
		})
	}
}

func TestVisit_Nodes_Failing_MissingData(t *testing.T) {
	for _, config := range allMptConfigs {
		config := config
		t.Run(config.Name, func(t *testing.T) {
			runTestWithArchive(t, config, func(trie *mpt.ArchiveTrie) {
				t.Parallel()

				// truncate file to simulate missing data
				file, err := os.OpenFile(path.Join(trie.Directory(), "accounts", "values.dat"), os.O_WRONLY|os.O_TRUNC, 0666)
				if err != nil {
					t.Fatalf("failed to open file: %v", err)
				}
				if err := file.Close(); err != nil {
					t.Fatalf("failed to close file: %v", err)
				}

				nodeId, err := trie.GetBlockRoot(0)
				if err != nil {
					t.Fatalf("failed to get block root: %v", err)
				}
				if err := visitAll(trie.Directory(), config, nodeId, nil, false); err == nil {
					t.Errorf("expected error, got nil")
				}
			})
		})
	}
}

func TestVisit_Nodes_CannotOpenFiles(t *testing.T) {
	injectedError := fmt.Errorf("injected error")

	ctrl := gomock.NewController(t)
	fc := NewMocknodeSourceFactory(ctrl)
	fc.EXPECT().open().Return(nil, injectedError).Times(16 + 1)

	if err := visitAllWithSources(fc, mpt.EmptyId(), nil, false); !errors.Is(err, injectedError) {
		t.Errorf("expected error %v, got %v", injectedError, err)
	}
}

func TestVisit_Nodes_CannotCloseSources(t *testing.T) {
	sourceFactories := map[string]struct {
		createData func(t *testing.T, dir string, config mpt.MptConfig) (root mpt.NodeId)
	}{
		"archive": {
			createData: func(t *testing.T, dir string, config mpt.MptConfig) (root mpt.NodeId) {
				trie := createArchive(t, dir, config)
				root, err := trie.GetBlockRoot(0)
				if err != nil {
					t.Fatalf("failed to get block root: %v", err)
				}
				if err := trie.Close(); err != nil {
					t.Fatalf("failed to close archive: %v", err)
				}
				return root
			}},
		"live": {
			createData: func(t *testing.T, dir string, config mpt.MptConfig) (root mpt.NodeId) {
				trie := createMptState(t, dir, config)
				rootId := trie.GetRootId()
				if err := trie.Close(); err != nil {
					t.Fatalf("failed to close live db: %v", err)
				}
				return rootId
			}},
	}

	injectedError := fmt.Errorf("injected error")

	for _, config := range allMptConfigs {
		config := config
		for name, factory := range sourceFactories {
			factory := factory
			t.Run(fmt.Sprintf("%s_%s", name, config.Name), func(t *testing.T) {
				t.Parallel()

				dir := t.TempDir()
				rootId := factory.createData(t, dir, config)
				parentFc := stockNodeSourceFactory{directory: dir, config: config}
				ctrl := gomock.NewController(t)

				mockFc := NewMocknodeSourceFactory(ctrl)
				mockFc.EXPECT().open().DoAndReturn(func() (nodeSource, error) {
					parentSource, err := parentFc.open()
					if err != nil {
						t.Fatalf("failed to open source: %v", err)
					}
					mockSource := NewMocknodeSource(ctrl)
					mockSource.EXPECT().get(gomock.Any()).DoAndReturn(parentSource.get).AnyTimes()
					mockSource.EXPECT().Close().Return(injectedError)
					return mockSource, nil
				}).Times(16 + 1)

				visitor := NewMocknoResponseNodeVisitor(ctrl)
				visitor.EXPECT().Visit(gomock.Any(), gomock.Any()).AnyTimes()

				if err := visitAllWithSources(mockFc, rootId, visitor, false); !errors.Is(err, injectedError) {
					t.Errorf("expected error %v, got %v", injectedError, err)
				}
			})
		}
	}
}

func TestVisit_Nodes_CannotGetNode_FailingSource(t *testing.T) {
	injectedError := fmt.Errorf("injected error")

	ctrl := gomock.NewController(t)
	mockFc := NewMocknodeSourceFactory(ctrl)
	mockFc.EXPECT().open().DoAndReturn(func() (nodeSource, error) {
		mockSource := NewMocknodeSource(ctrl)
		mockSource.EXPECT().get(gomock.Any()).Return(nil, injectedError).AnyTimes()
		mockSource.EXPECT().Close().Return(nil)
		return mockSource, nil
	}).Times(16 + 1)

	visitor := NewMocknoResponseNodeVisitor(ctrl)
	visitor.EXPECT().Visit(gomock.Any(), gomock.Any()).AnyTimes()

	var nodeId mpt.NodeId
	if err := visitAllWithSources(mockFc, nodeId, visitor, false); !errors.Is(err, injectedError) {
		t.Errorf("expected error %v, got %v", injectedError, err)
	}
}

func TestVisit_Nodes_Iterated_Deterministic(t *testing.T) {
	const N = 15
	for _, config := range allMptConfigs {
		t.Run(config.Name, func(t *testing.T) {
			runTestWithArchive(t, config, func(trie *mpt.ArchiveTrie) {
				// trie must be flushed before opening the parallel source
				if err := trie.Flush(); err != nil {
					t.Fatalf("failed to flush archive: %v", err)
				}

				blocks, _, err := trie.GetBlockHeight()
				if err != nil {
					t.Fatalf("failed to get block height: %v", err)
				}

				// iterate all nodes in the trie for all blocks
				for block := uint64(0); block <= blocks; block++ {
					var nodes []mpt.NodeId
					if err := trie.VisitTrie(block, mpt.MakeVisitor(
						func(node mpt.Node, info mpt.NodeInfo) mpt.VisitResponse {
							nodes = append(nodes, info.Id)
							return mpt.VisitResponseContinue
						})); err != nil {
						t.Fatalf("failed to visit trie: %v", err)
					}

					nodeId, err := trie.GetBlockRoot(block)
					if err != nil {
						t.Fatalf("failed to get block root: %v", err)
					}

					// visit all nodes in the trie and compare that the nodes are visited in the same order
					// as the nodes in the trie
					// run the experiment N times to ensure that the order is deterministic
					for i := 0; i < N; i++ {
						t.Run(fmt.Sprintf("block=%d,iteration=%d", block, i), func(t *testing.T) {
							t.Parallel()
							var position int
							ctrl := gomock.NewController(t)
							visitor := NewMocknoResponseNodeVisitor(ctrl)
							visitor.EXPECT().Visit(gomock.Any(), gomock.Any()).DoAndReturn(
								func(_ mpt.Node, info mpt.NodeInfo) error {
									if got, want := info.Id, nodes[position]; got != want {
										t.Errorf("expected node %v, got %v", want, got)
									}
									position++
									return nil
								}).Times(len(nodes))

							if err := visitAll(trie.Directory(), config, nodeId, visitor, false); err != nil {
								t.Fatalf("failed to visit nodes: %v", err)
							}
						})
					}
				}
			})
		})
	}
}

func TestSource_EmptyNodeId(t *testing.T) {
	source := stockNodeSource{}
	node, _ := source.get(mpt.EmptyId())
	if got, want := node, (mpt.EmptyNode{}); got != want {
		t.Errorf("expected empty node, got %v", got)
	}
}

func TestOpenSource_Failing_MissingFiles(t *testing.T) {
	tests := []string{"accounts", "extensions", "branches", "values"}

	for _, config := range allMptConfigs {
		config := config
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("%s %s", config.Name, test), func(t *testing.T) {
				t.Parallel()

				dir := t.TempDir()
				fmt.Printf("%s %s %s\n", config.Name, test, dir)
				aEnc, bEnc, eEnc, vEnc := config.GetEncoders()
				// create all stocks first
				stock1, err := file.OpenStock[uint64, mpt.AccountNode](aEnc, path.Join(dir, "accounts"))
				if err != nil {
					t.Fatalf("failed to open stock: %v", err)
				}
				if err := stock1.Close(); err != nil {
					t.Fatalf("failed to close stock: %v", err)
				}

				stock2, err := file.OpenStock[uint64, mpt.ExtensionNode](eEnc, path.Join(dir, "extensions"))
				if err != nil {
					t.Fatalf("failed to open stock: %v", err)
				}
				if err := stock2.Close(); err != nil {
					t.Fatalf("failed to close stock: %v", err)
				}

				stock3, err := file.OpenStock[uint64, mpt.BranchNode](bEnc, path.Join(dir, "branches"))
				if err != nil {
					t.Fatalf("failed to open stock: %v", err)
				}
				if err := stock3.Close(); err != nil {
					t.Fatalf("failed to close stock: %v", err)
				}

				stock4, err := file.OpenStock[uint64, mpt.ValueNode](vEnc, path.Join(dir, "values"))
				if err != nil {
					t.Fatalf("failed to open stock: %v", err)
				}
				if err := stock4.Close(); err != nil {
					t.Fatalf("failed to close stock: %v", err)
				}

				// delete one of the stocks
				if err := os.RemoveAll(path.Join(dir, test)); err != nil {
					t.Fatalf("failed to remove directory: %v", err)
				}

				factory := stockNodeSourceFactory{directory: dir, config: config}
				if _, err := factory.open(); err == nil {
					t.Errorf("expected error, got nil")
				}
			})
		}
	}
}

func TestNodeSourceFactoryForLiveDB_CanRead_Nodes(t *testing.T) {
	for _, config := range allMptConfigs {
		config := config
		t.Run(config.Name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			live := createMptState(t, dir, config)

			// collect all nodes from the live db
			nodes := make(map[mpt.NodeId]mpt.Node)
			// collect all nodes from the live db
			if err := live.Visit(mpt.MakeVisitor(func(node mpt.Node, info mpt.NodeInfo) mpt.VisitResponse {
				nodes[info.Id] = node
				return mpt.VisitResponseContinue
			})); err != nil {
				t.Fatalf("failed to visit trie: %v", err)
			}

			if err := live.Close(); err != nil {
				t.Fatalf("failed to close live db: %v", err)
			}

			// check that all nodes can be read from the source
			factory := stockNodeSourceFactory{directory: dir, config: config}
			source, err := factory.open()
			if err != nil {
				t.Fatalf("failed to open node source: %v", err)
			}
			defer func() {
				if err := source.Close(); err != nil {
					t.Fatalf("failed to close source: %v", err)
				}
			}()

			// compare that all nodes from the trie can be read from the source
			for id, node := range nodes {
				sourceNode, err := source.get(id)
				if err != nil {
					t.Fatalf("failed to get node from source: %v", err)
				}

				matchNodes(t, node, sourceNode)
			}
		})
	}
}

// matchNodes compares two nodes and fails the test if they are not equal.
func matchNodes(t *testing.T, a, b mpt.Node) {
	switch n := a.(type) {
	case *mpt.AccountNode:
		if got, want := n.Address(), b.(*mpt.AccountNode).Address(); got != want {
			t.Errorf("expected address %v, got %v", want, got)
		}
		if got, want := n.Info(), b.(*mpt.AccountNode).Info(); got != want {
			t.Errorf("expected info %v, got %v", want, got)
		}
		if got, want := n.GetStorage(), b.(*mpt.AccountNode).GetStorage(); got.Id() != want.Id() {
			t.Errorf("expected storage %v, got %v", want, got)
		}
	case *mpt.ExtensionNode:
		if got, want := n.Path(), b.(*mpt.ExtensionNode).Path(); got != want {
			t.Errorf("expected path %v, got %v", want, got)
		}
		if got, want := n.GetNext(), b.(*mpt.ExtensionNode).GetNext(); got.Id() != want.Id() {
			t.Errorf("expected next %v, got %v", want, got)
		}
	case *mpt.BranchNode:
		for i := 0; i < 16; i++ {
			if got, want := n.GetChildren()[i], b.(*mpt.BranchNode).GetChildren()[i]; got.Id() != want.Id() {
				t.Errorf("expected children %v, got %v", want, got)
			}
		}
	}
}

// runTestWithArchive runs a test with a new archive that is pre-populated data.
func runTestWithArchive(t *testing.T, config mpt.MptConfig, action func(trie *mpt.ArchiveTrie)) {
	trie := createArchive(t, t.TempDir(), config)
	defer func() {
		if err := trie.Close(); err != nil {
			t.Fatalf("failed to close archive archive: %v", err)
		}
	}()

	action(trie)
}

// createArchive creates a new archive with pre-populated data.
func createArchive(t *testing.T, dir string, config mpt.MptConfig) *mpt.ArchiveTrie {
	archive, err := mpt.OpenArchiveTrie(dir, config, mpt.NodeCacheConfig{Capacity: 1024}, mpt.ArchiveConfig{})
	if err != nil {
		t.Fatalf("failed to create archive: %v", err)
	}

	const (
		Blocks   = 10
		Accounts = 30
	)

	for i := 0; i < Blocks; i++ {
		code := []byte{1, 2, 3, byte(i)}
		u := uint64(i)
		update := common.Update{}
		for j := 0; j < Accounts; j++ {
			newAddr := common.AddressFromNumber(j)

			update.CreatedAccounts = append(update.CreatedAccounts, newAddr)
			update.Balances = append(update.Balances, common.BalanceUpdate{
				Account: newAddr, Balance: amount.New(u + 1)})
			update.Nonces = append(update.Nonces, common.NonceUpdate{
				Account: newAddr, Nonce: common.ToNonce(u + 1)})
			update.Codes = append(update.Codes, common.CodeUpdate{
				Account: newAddr, Code: code})
			update.Slots = append(update.Slots, common.SlotUpdate{
				Account: newAddr, Key: common.Key{byte(j)}, Value: common.Value{byte(i)}})
		}
		err = archive.Add(u, update, nil)
		if err != nil {
			t.Fatalf("failed to create block in archive: %v", err)
		}
	}

	return archive
}

// createMptState creates a new live db with pre-populated data.
func createMptState(t *testing.T, dir string, config mpt.MptConfig) *mpt.MptState {
	live, err := mpt.OpenGoFileState(dir, config, mpt.NodeCacheConfig{Capacity: 1024})
	if err != nil {
		t.Fatalf("failed to open live db: %v", err)
	}

	const (
		Accounts = 31
		Key      = 32
	)

	// populate the live db with some data
	for i := 0; i < Accounts; i++ {
		addr := common.AddressFromNumber(i)
		if err := live.SetNonce(addr, common.Nonce{1}); err != nil {
			t.Fatalf("failed to set account info: %v", err)
		}
		for j := 0; j < Key; j++ {
			if err := live.SetStorage(addr, common.Key{byte(i), byte(j)}, common.Value{1}); err != nil {
				t.Fatalf("failed to set value: %v", err)
			}
		}
	}

	return live
}

func TestBarrier_SyncsWorkers(t *testing.T) {
	const NumWorker = 30
	const NumIterations = 100

	data := []int{}
	dataLock := sync.Mutex{}

	// produces data in the form of [0, 0, 0, 1, 1, 1, 2, 2, 2, ...]
	var wg sync.WaitGroup
	wg.Add(NumWorker)
	barrier := newBarrier(NumWorker)
	for i := 0; i < NumWorker; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < NumIterations; j++ {
				barrier.wait()
				dataLock.Lock()
				data = append(data, j)
				dataLock.Unlock()
			}
		}()
	}

	wg.Wait()

	if len(data) != NumWorker*NumIterations {
		t.Errorf("expected %d, got %d", NumWorker*NumIterations, len(data))
	}

	sorted := slices.Clone(data)
	slices.Sort(sorted)

	if !slices.Equal(data, sorted) {
		t.Errorf("expected sorted data, got %v", data)
	}
}

func TestBarrier_CanBeReleased(t *testing.T) {
	const NumWorker = 3

	var wg sync.WaitGroup
	wg.Add(NumWorker)
	barrier := newBarrier(NumWorker)
	for i := 0; i < NumWorker; i++ {
		go func(i int) {
			defer wg.Done()
			if i != 0 {
				barrier.wait() // not all workers will reach the barrier
			}
			barrier.wait() // reached after releasing the barrier
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Errorf("should not have completed without releasing the barrier")
	case <-time.After(100 * time.Millisecond):
	}

	barrier.release()
	<-done
}

func TestBarrier_AReleasedBarrierDoesNotBlock(t *testing.T) {
	barrier := newBarrier(2)
	barrier.release()

	done := make(chan struct{})
	go func() {
		close(done)
		barrier.wait()
	}()

	select {
	case <-done:
		// all fine
	case <-time.After(time.Second):
		t.Errorf("the released barrier should not block")
	}
}

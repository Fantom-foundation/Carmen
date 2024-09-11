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
	"github.com/Fantom-foundation/Carmen/go/backend/stock/file"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"go.uber.org/mock/gomock"
)

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
	runTestWithArchive(t, func(trie *mpt.ArchiveTrie) {
		// trie must be flushed before opening the parallel source
		if err := trie.Flush(); err != nil {
			t.Fatalf("failed to flush archive: %v", err)
		}

		blocks, _, err := trie.GetBlockHeight()
		if err != nil {
			t.Fatalf("failed to get block height: %v", err)
		}

		factory := nodeSourceHashWithNodesFactory{directory: trie.Directory()}
		source, err := factory.open()
		if err != nil {
			t.Fatalf("failed to open node source: %v", err)
		}

		// iterate all nodes in the trie for all blocks
		for i := uint64(0); i <= blocks; i++ {
			if err := trie.VisitTrie(i, mpt.MakeVisitor(func(node mpt.Node, info mpt.NodeInfo) mpt.VisitResponse {
				nodeHash, dirty := node.GetHash()
				if dirty {
					t.Fatalf("node %v is dirty", info.Id)
				}

				sourceNode, err := source.get(info.Id)
				if err != nil {
					t.Fatalf("failed to get node from source: %v", err)
				}
				nodeSourceHash, dirty := sourceNode.GetHash()
				if dirty {
					t.Fatalf("node %v is dirty", info.Id)
				}

				if nodeHash != nodeSourceHash {
					t.Errorf("node %v hash mismatch, wanted %v, got %v", info.Id, nodeHash, nodeSourceHash)
				}

				return mpt.VisitResponseContinue
			})); err != nil {
				t.Fatalf("failed to visit trie: %v", err)
			}
		}
	})
}

func TestVisit_Nodes_Failing_CannotOpenDir(t *testing.T) {
	dir := path.Join(t.TempDir(), "missing")
	if err := visitAll(dir, mpt.EmptyId(), nil, false); err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestVisit_Nodes_Failing_MissingDir(t *testing.T) {
	dir := t.TempDir()
	trie := createArchive(t, dir)
	root, err := trie.GetBlockRoot(0)
	if err != nil {
		t.Fatalf("failed to get block root: %v", err)
	}

	if err := trie.Close(); err != nil {
		t.Fatalf("failed to close archive: %v", err)
	}

	// break the directory by removing the files
	if err := os.RemoveAll(path.Join(dir, "accounts")); err != nil {
		t.Fatalf("failed to remove directory: %v", err)
	}

	if err := visitAll(dir, root, nil, false); err == nil {
		t.Errorf("expected error, got nil")
	}

}

func TestVisit_Nodes_Failing_MissingData(t *testing.T) {
	runTestWithArchive(t, func(trie *mpt.ArchiveTrie) {
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
		if err := visitAll(trie.Directory(), nodeId, nil, false); err == nil {
			t.Errorf("expected error, got nil")
		}
	})
}

func TestVisit_Nodes_CannotOpenFiles(t *testing.T) {
	injectedError := fmt.Errorf("injected error")

	ctrl := gomock.NewController(t)
	fc := NewMocknodeSourceFactory(ctrl)
	fc.EXPECT().open().Return(nil, injectedError).Times(16)

	if err := visitAllWithSources(fc, mpt.EmptyId(), nil, false); !errors.Is(err, injectedError) {
		t.Errorf("expected error %v, got %v", injectedError, err)
	}
}

func TestVisit_Nodes_CannotCloseSources(t *testing.T) {
	sourceFactories := []struct {
		name       string
		factory    func(dir string) nodeSourceFactory
		createData func(t *testing.T, dir string) (root mpt.NodeId)
	}{
		{"hashWithNodes", func(dir string) nodeSourceFactory { return &nodeSourceHashWithNodesFactory{directory: dir} }, func(t *testing.T, dir string) (root mpt.NodeId) {
			trie := createArchive(t, dir)
			root, err := trie.GetBlockRoot(0)
			if err != nil {
				t.Fatalf("failed to get block root: %v", err)
			}
			if err := trie.Close(); err != nil {
				t.Fatalf("failed to close archive: %v", err)
			}
			return root
		}},
		{"hashWithChildNodes", func(dir string) nodeSourceFactory { return &nodeSourceHashWithChildNodesFactory{directory: dir} }, func(t *testing.T, dir string) (root mpt.NodeId) {
			trie := createMptState(t, dir)
			rootId := trie.GetRootId()
			if err := trie.Close(); err != nil {
				t.Fatalf("failed to close live db: %v", err)
			}
			return rootId
		}},
	}

	injectedError := fmt.Errorf("injected error")

	for _, factory := range sourceFactories {
		t.Run(factory.name, func(t *testing.T) {
			dir := t.TempDir()
			rootId := factory.createData(t, dir)
			parentFc := factory.factory(dir)
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
			}).Times(16)

			visitor := NewMocknoResponseNodeVisitor(ctrl)
			visitor.EXPECT().Visit(gomock.Any(), gomock.Any()).AnyTimes()

			if err := visitAllWithSources(mockFc, rootId, visitor, false); !errors.Is(err, injectedError) {
				t.Errorf("expected error %v, got %v", injectedError, err)
			}
		})
	}

}

func TestVisit_Nodes_CannotGetNode_FailingSource(t *testing.T) {
	injectedError := fmt.Errorf("injected error")

	trie := createArchive(t, t.TempDir())

	nodeId, err := trie.GetBlockRoot(0)
	if err != nil {
		t.Fatalf("failed to get block root: %v", err)
	}
	if err := trie.Close(); err != nil {
		t.Fatalf("failed to close archive: %v", err)
	}

	ctrl := gomock.NewController(t)

	mockFc := NewMocknodeSourceFactory(ctrl)
	mockFc.EXPECT().open().DoAndReturn(func() (nodeSource, error) {
		if err != nil {
			t.Fatalf("failed to open source: %v", err)
		}
		mockSource := NewMocknodeSource(ctrl)
		mockSource.EXPECT().get(gomock.Any()).Return(nil, injectedError).AnyTimes()
		mockSource.EXPECT().Close().Return(nil)
		return mockSource, nil
	}).Times(16)

	visitor := NewMocknoResponseNodeVisitor(ctrl)
	visitor.EXPECT().Visit(gomock.Any(), gomock.Any()).AnyTimes()

	if err := visitAllWithSources(mockFc, nodeId, visitor, false); !errors.Is(err, injectedError) {
		t.Errorf("expected error %v, got %v", injectedError, err)
	}
}

func TestVisit_Nodes_Iterated_Deterministic(t *testing.T) {
	const N = 15
	runTestWithArchive(t, func(trie *mpt.ArchiveTrie) {
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
			if err := trie.VisitTrie(block, mpt.MakeVisitor(func(node mpt.Node, info mpt.NodeInfo) mpt.VisitResponse {
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
					visitor.EXPECT().Visit(gomock.Any(), gomock.Any()).DoAndReturn(func(_ mpt.Node, info mpt.NodeInfo) error {
						if got, want := info.Id, nodes[position]; got != want {
							t.Errorf("expected node %v, got %v", want, got)
						}
						position++
						return nil
					}).Times(len(nodes))

					if err := visitAll(trie.Directory(), nodeId, visitor, false); err != nil {
						t.Fatalf("failed to visit nodes: %v", err)
					}
				})
			}
		}
	})
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

	sourceFactories := []struct {
		name    string
		factory func(dir string) nodeSourceFactory
	}{
		{"hashWithNodes", func(dir string) nodeSourceFactory { return &nodeSourceHashWithNodesFactory{directory: dir} }},
		{"hashWithChildNodes", func(dir string) nodeSourceFactory { return &nodeSourceHashWithChildNodesFactory{directory: dir} }},
	}

	for _, test := range tests {
		t.Run(test, func(t *testing.T) {
			dir := t.TempDir()

			// create all stocks first
			stock1, err := file.OpenStock[uint64, mpt.AccountNode](mpt.AccountNodeWithPathLengthEncoderWithNodeHash{}, path.Join(dir, "accounts"))
			if err != nil {
				t.Fatalf("failed to open stock: %v", err)
			}
			if err := stock1.Close(); err != nil {
				t.Fatalf("failed to close stock: %v", err)
			}

			stock2, err := file.OpenStock[uint64, mpt.ExtensionNode](mpt.ExtensionNodeEncoderWithNodeHash{}, path.Join(dir, "extensions"))
			if err != nil {
				t.Fatalf("failed to open stock: %v", err)
			}
			if err := stock2.Close(); err != nil {
				t.Fatalf("failed to close stock: %v", err)
			}

			stock3, err := file.OpenStock[uint64, mpt.BranchNode](mpt.BranchNodeEncoderWithNodeHash{}, path.Join(dir, "branches"))
			if err != nil {
				t.Fatalf("failed to open stock: %v", err)
			}
			if err := stock3.Close(); err != nil {
				t.Fatalf("failed to close stock: %v", err)
			}

			stock4, err := file.OpenStock[uint64, mpt.ValueNode](mpt.ValueNodeWithPathLengthEncoderWithNodeHash{}, path.Join(dir, "values"))
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

			for _, factory := range sourceFactories {
				t.Run(factory.name, func(t *testing.T) {
					source := factory.factory(dir)
					if _, err := source.open(); err == nil {
						t.Errorf("expected error, got nil")
					}
				})
			}
		})
	}
}

func TestNodeSourceFactoryForLiveDB_CanRead_Nodes(t *testing.T) {
	dir := t.TempDir()
	live := createMptState(t, dir)

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
	factory := nodeSourceHashWithChildNodesFactory{directory: dir}
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

		switch n := node.(type) {
		case *mpt.AccountNode:
			if got, want := n.Address(), sourceNode.(*mpt.AccountNode).Address(); got != want {
				t.Errorf("expected address %v, got %v", want, got)
			}
			if got, want := n.Info(), sourceNode.(*mpt.AccountNode).Info(); got != want {
				t.Errorf("expected info %v, got %v", want, got)
			}
			if got, want := n.GetStorage(), sourceNode.(*mpt.AccountNode).GetStorage(); got.Id() != want.Id() {
				t.Errorf("expected storage %v, got %v", want, got)
			}
		case *mpt.ExtensionNode:
			if got, want := n.Path(), sourceNode.(*mpt.ExtensionNode).Path(); got != want {
				t.Errorf("expected path %v, got %v", want, got)
			}
			if got, want := n.GetNext(), sourceNode.(*mpt.ExtensionNode).GetNext(); got.Id() != want.Id() {
				t.Errorf("expected next %v, got %v", want, got)
			}
		case *mpt.BranchNode:
			for i := 0; i < 16; i++ {
				if got, want := n.GetChildren()[i], sourceNode.(*mpt.BranchNode).GetChildren()[i]; got.Id() != want.Id() {
					t.Errorf("expected children %v, got %v", want, got)
				}
			}
		}
	}
}

// runTestWithArchive runs a test with a new archive that is pre-populated data.
func runTestWithArchive(t *testing.T, action func(trie *mpt.ArchiveTrie)) {
	trie := createArchive(t, t.TempDir())
	defer func() {
		if err := trie.Close(); err != nil {
			t.Fatalf("failed to close archive archive: %v", err)
		}
	}()

	action(trie)
}

// createArchive creates a new archive with pre-populated data.
func createArchive(t *testing.T, dir string) *mpt.ArchiveTrie {
	archive, err := mpt.OpenArchiveTrie(dir, mpt.S5ArchiveConfig, mpt.NodeCacheConfig{Capacity: 1024}, mpt.ArchiveConfig{})
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
			update.Balances = append(update.Balances, common.BalanceUpdate{Account: newAddr, Balance: amount.New(u + 1)})
			update.Nonces = append(update.Nonces, common.NonceUpdate{Account: newAddr, Nonce: common.ToNonce(u + 1)})
			update.Codes = append(update.Codes, common.CodeUpdate{Account: newAddr, Code: code})
			update.Slots = append(update.Slots, common.SlotUpdate{Account: newAddr, Key: common.Key{byte(j)}, Value: common.Value{byte(i)}})
		}
		err = archive.Add(u, update, nil)
		if err != nil {
			t.Fatalf("failed to create block in archive: %v", err)
		}
	}

	return archive
}

// createMptState creates a new live db with pre-populated data.
func createMptState(t *testing.T, dir string) *mpt.MptState {
	live, err := mpt.OpenGoFileState(dir, mpt.S5LiveConfig, mpt.NodeCacheConfig{Capacity: 1024})
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

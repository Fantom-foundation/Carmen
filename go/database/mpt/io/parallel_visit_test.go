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
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"go.uber.org/mock/gomock"
	"os"
	"path"
	"strings"
	"testing"
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
		pos = pos.Child(test.step)
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
		position = position.Child(byte(i))
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

		factory := stockNodeSourceFactory{directory: trie.Directory()}
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
	if err := visitAll(&stockNodeSourceFactory{dir}, mpt.EmptyId(), &noResponseMptNodeVisitor{}, false); err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestVisit_Nodes_Failing_MissingDir(t *testing.T) {
	trie := createArchive(t)
	dir := trie.Directory()
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

	if err := visitAll(&stockNodeSourceFactory{dir}, root, &noResponseMptNodeVisitor{}, false); err == nil {
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
		if err := visitAll(&stockNodeSourceFactory{trie.Directory()}, nodeId, &noResponseMptNodeVisitor{}, false); err == nil {
			t.Errorf("expected error, got nil")
		}
	})
}

func TestVisit_Nodes_CannotOpenFiles(t *testing.T) {
	injectedError := fmt.Errorf("injected error")

	ctrl := gomock.NewController(t)
	fc := NewMocknodeSourceFactory(ctrl)
	fc.EXPECT().open().Return(nil, injectedError).Times(16)

	if err := visitAll(fc, mpt.EmptyId(), &noResponseMptNodeVisitor{}, false); !errors.Is(err, injectedError) {
		t.Errorf("expected error %v, got %v", injectedError, err)
	}
}

func TestVisit_Nodes_CannotCloseSources(t *testing.T) {
	injectedError := fmt.Errorf("injected error")

	trie := createArchive(t)

	nodeId, err := trie.GetBlockRoot(0)
	if err != nil {
		t.Fatalf("failed to get block root: %v", err)
	}
	if err := trie.Close(); err != nil {
		t.Fatalf("failed to close archive: %v", err)
	}

	parentFc := &stockNodeSourceFactory{trie.Directory()}
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

	if err := visitAll(mockFc, nodeId, visitor, false); !errors.Is(err, injectedError) {
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
					visitor.EXPECT().Visit(gomock.Any(), gomock.Any()).DoAndReturn(func(_ mpt.Node, info mpt.NodeInfo) {
						if got, want := info.Id, nodes[position]; got != want {
							t.Errorf("expected node %v, got %v", want, got)
						}
						position++
					}).Times(len(nodes))

					if err := visitAll(&stockNodeSourceFactory{trie.Directory()}, nodeId, visitor, false); err != nil {
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
	tests := []struct {
		name string
	}{
		{"accounts"},
		{"extensions"},
		{"branches"},
		{"values"},
	}

	for _, test := range tests {
		dir := t.TempDir()

		// make sure all files exists except the one under test
		for _, subdir := range tests {
			if subdir.name != test.name {
				if err := os.MkdirAll(path.Join(dir, subdir.name), 0700); err != nil {
					t.Fatalf("failed to create directory: %v", err)
				}
				_, err := os.Create(path.Join(dir, subdir.name, "values.dat"))
				if err != nil {
					t.Fatalf("failed to create file: %v", err)
				}
			}
		}

		factory := stockNodeSourceFactory{directory: dir}
		if _, err := factory.open(); err == nil {
			t.Errorf("expected error, got nil")
		}
	}
}

// runTestWithArchive runs a test with a new archive that is pre-populated data.
func runTestWithArchive(t *testing.T, action func(trie *mpt.ArchiveTrie)) {
	trie := createArchive(t)
	defer func() {
		if err := trie.Close(); err != nil {
			t.Fatalf("failed to close archive archive: %v", err)
		}
	}()

	action(trie)
}

// createArchive creates a new archive with pre-populated data.
func createArchive(t *testing.T) *mpt.ArchiveTrie {
	archive, err := mpt.OpenArchiveTrie(t.TempDir(), mpt.S5ArchiveConfig, mpt.NodeCacheConfig{Capacity: 1024}, mpt.ArchiveConfig{})
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

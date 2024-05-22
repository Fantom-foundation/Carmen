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

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
	gomock "go.uber.org/mock/gomock"
)

// ----------------------------------------------------------------------------
//                        General Hasher Tests
// ----------------------------------------------------------------------------

var allHashAlgorithms = []hashAlgorithm{DirectHashing, EthereumLikeHashing}

func TestHasher_ExtensionNode_GetHash_DirtyHashesAreIgnored(t *testing.T) {
	for _, algorithm := range allHashAlgorithms {
		t.Run(algorithm.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctxt := newNodeContext(t, ctrl)

			ref, node := ctxt.Build(&Extension{
				path: []Nibble{0x8, 0xe, 0xf},
				next: &Branch{
					children: Children{
						0x7: &Account{},
						0xd: &Account{},
					},
				},
				hashDirty:     true,
				nextHashDirty: true,
			})

			hasher := algorithm.createHasher()
			_, err := hasher.getHash(&ref, ctxt)
			if err != nil {
				t.Fatalf("error computing hash: %v", err)
			}

			// The dirty flag is cleared.
			handle := node.GetReadHandle()
			defer handle.Release()
			if handle.Get().(*ExtensionNode).hasCleanHash() {
				t.Errorf("dirty hash flag should not be cleared")
			}
			if !handle.Get().(*ExtensionNode).nextHashDirty {
				t.Errorf("dirty hash flag should not be changed")
			}
		})
	}
}

func TestHasher_ExtensionNode_UpdateHash_DirtyHashesAreRefreshed(t *testing.T) {
	for _, algorithm := range allHashAlgorithms {
		t.Run(algorithm.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctxt := newNodeContext(t, ctrl)

			ref, node := ctxt.Build(&Extension{
				path: []Nibble{0x8, 0xe, 0xf},
				next: &Branch{
					children: Children{
						0x7: &Account{},
						0xd: &Account{},
					},
				},
				hashDirty:     true,
				nextHashDirty: true,
			})

			hasher := algorithm.createHasher()
			_, _, err := hasher.updateHashes(&ref, ctxt)
			if err != nil {
				t.Fatalf("error computing hash: %v", err)
			}

			// The dirty flag is cleared.
			handle := node.GetReadHandle()
			defer handle.Release()
			if !handle.Get().(*ExtensionNode).hasCleanHash() {
				t.Errorf("dirty hash flag should be cleared")
			}
			if handle.Get().(*ExtensionNode).nextHashDirty {
				t.Errorf("node still marked as dirty after updating the hashes")
			}
		})
	}
}

func TestHasher_BranchNode_GetHash_DirtyHashesAreIgnored(t *testing.T) {
	for _, algorithm := range allHashAlgorithms {
		t.Run(algorithm.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctxt := newNodeContext(t, ctrl)

			ref, node := ctxt.Build(&Branch{
				children: Children{
					0x7: &Account{},
					0xd: &Account{},
				},
				dirtyHash:        true,
				dirtyChildHashes: []int{0x7, 0xd},
			})

			hasher := algorithm.createHasher()
			_, err := hasher.getHash(&ref, ctxt)
			if err != nil {
				t.Fatalf("error computing hash: %v", err)
			}

			// The dirty flag is not touched.
			handle := node.GetReadHandle()
			defer handle.Release()
			if handle.Get().(*BranchNode).hasCleanHash() {
				t.Errorf("dirty hash flag should not be cleared")
			}
			if handle.Get().(*BranchNode).dirtyHashes != ((1 << 0x7) | (1 << 0xd)) {
				t.Errorf("dirty child hash flags should not be changed")
			}
		})
	}
}

func TestHasher_BranchNode_UpdateHash_DirtyHashesAreRefreshed(t *testing.T) {
	for _, algorithm := range allHashAlgorithms {
		t.Run(algorithm.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctxt := newNodeContext(t, ctrl)

			ref, node := ctxt.Build(&Branch{
				children: Children{
					0x7: &Account{},
					0xd: &Account{},
				},
				dirtyHash:        true,
				dirtyChildHashes: []int{0x7, 0xd},
			})

			ctxt.EXPECT().hashAddress(gomock.Any()).MaxTimes(2)

			hasher := algorithm.createHasher()
			_, _, err := hasher.updateHashes(&ref, ctxt)
			if err != nil {
				t.Fatalf("error computing hash: %v", err)
			}

			// The dirty flag is cleared.
			handle := node.GetReadHandle()
			defer handle.Release()
			if !handle.Get().(*BranchNode).hasCleanHash() {
				t.Errorf("dirty hash flag should be cleared")
			}
			if handle.Get().(*BranchNode).dirtyHashes != 0 {
				t.Errorf("dirty hash flags should be cleared")
			}
		})
	}
}

func TestHasher_BranchNode_UpdateHash_DirtyFlagsForEmptyChildrenAreClearedButNoUpdateIssued(t *testing.T) {
	for _, algorithm := range allHashAlgorithms {
		t.Run(algorithm.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctxt := newNodeContext(t, ctrl)

			ref, node := ctxt.Build(&Branch{
				children: Children{
					0x7: &Account{},
					0xd: &Account{},
				},
				dirtyHash:        true,
				dirtyChildHashes: []int{1, 2, 3}, // < all empty children
			})

			hasher := algorithm.createHasher()
			_, _, err := hasher.updateHashes(&ref, ctxt)
			if err != nil {
				t.Fatalf("error computing hash: %v", err)
			}

			// The dirty flag is cleared.
			handle := node.GetReadHandle()
			defer handle.Release()
			if !handle.Get().(*BranchNode).hasCleanHash() {
				t.Errorf("dirty hash flag should be cleared")
			}
			if handle.Get().(*BranchNode).dirtyHashes != 0 {
				t.Errorf("dirty children hash flags should be cleared")
			}
		})
	}
}

func TestHasher_AccountNode_GetHash_DirtyHashesAreIgnored(t *testing.T) {
	for _, algorithm := range allHashAlgorithms {
		t.Run(algorithm.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctxt := newNodeContext(t, ctrl)

			ref, node := ctxt.Build(&Account{
				dirtyHash:        true,
				storageHashDirty: true,
			})

			ctxt.EXPECT().hashAddress(gomock.Any()).MaxTimes(1)

			hasher := algorithm.createHasher()
			_, err := hasher.getHash(&ref, ctxt)
			if err != nil {
				t.Fatalf("error computing hash: %v", err)
			}

			// The dirty flag is not changed.
			handle := node.GetReadHandle()
			defer handle.Release()
			if handle.Get().(*AccountNode).hasCleanHash() {
				t.Errorf("dirty hash flags should not be changed")
			}
			if !handle.Get().(*AccountNode).storageHashDirty {
				t.Errorf("dirty storage hash flags should not be changed")
			}
		})
	}
}

func TestHasher_AccountNode_UpdateHash_DirtyHashesAreRefreshed(t *testing.T) {
	for _, algorithm := range allHashAlgorithms {
		t.Run(algorithm.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctxt := newNodeContext(t, ctrl)

			ref, node := ctxt.Build(&Account{
				dirtyHash:        true,
				storageHashDirty: true,
			})

			ctxt.EXPECT().hashAddress(gomock.Any()).MaxTimes(1)

			hasher := algorithm.createHasher()
			_, _, err := hasher.updateHashes(&ref, ctxt)
			if err != nil {
				t.Fatalf("error computing hash: %v", err)
			}

			// The dirty flag is cleared.
			handle := node.GetReadHandle()
			defer handle.Release()
			if !handle.Get().(*AccountNode).hasCleanHash() {
				t.Errorf("dirty hash flag should be cleared")
			}
			if handle.Get().(*AccountNode).storageHashDirty {
				t.Errorf("dirty storage hash flags should be cleared")
			}
		})
	}
}

// ----------------------------------------------------------------------------
//                          Ethereum Like Hasher
// ----------------------------------------------------------------------------

func TestEthereumLikeHasher_EmptyNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	nodes := NewMockNodeManager(ctrl)
	hasher := makeEthereumLikeHasher()

	ref := NewNodeReference(EmptyId())
	hash, err := hasher.getHash(&ref, nodes)
	if err != nil {
		t.Fatalf("failed to hash empty node: %v", err)
	}

	if got, want := hash, EmptyNodeEthereumHash; got != want {
		t.Errorf("invalid hash of empty node, wanted %v, got %v", got, want)
	}
}

func TestEthereumLikeHasher_ExtensionNode_KnownHash(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	// This test case reconstructs an issue encountered while hashing the
	// state tree of block 25399 of the Fantom main-net.

	hasher := makeEthereumLikeHasher()
	ref, node := ctxt.Build(&Extension{
		path: []Nibble{0x8, 0xe, 0xf},
		next: &Branch{
			children: Children{
				0x7: &Account{},
				0xd: &Account{},
			},
		},
	})

	handle := node.GetWriteHandle()
	ext := handle.Get().(*ExtensionNode)
	ext.nextHash = common.HashFromString("43085a287ea060fa9089bd4797d2471c6d57136b666a314e6a789735251317d4")
	ext.nextHashDirty = false
	handle.Release()

	hash, err := hasher.getHash(&ref, ctxt)
	if err != nil {
		t.Fatalf("error computing hash: %v", err)
	}
	want := "ebf7c28d351f2ec8a26d0e40049ddf406117e0468a49af0d261bb74d88e17560"
	got := fmt.Sprintf("%x", hash)
	if got != want {
		t.Fatalf("unexpected hash\nwanted %v\n   got %v", want, got)
	}
}

func TestEthereumLikeHasher_BranchNode_KnownHash_EmbeddedNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	ctxt.EXPECT().hashKey(gomock.Any()).AnyTimes().DoAndReturn(func(key common.Key) (common.Hash, error) {
		return common.Keccak256(key[:]), nil
	})

	ctxt.EXPECT().hashKey(gomock.Any()).AnyTimes().DoAndReturn(func(k common.Key) common.Hash {
		return common.Keccak256(k[:])
	})

	// This test case reconstructs an issue encountered while hashing the
	// state tree of block 652606 of the Fantom main-net.

	v31 := common.Value{}
	v31[len(v31)-1] = 31

	var key common.Key
	data, _ := hex.DecodeString("c1bb1e5ab6acf1bef1a125f3d60e0941b9a8624288ffd67282484c25519f9e65")
	copy(key[:], data)

	hasher := makeEthereumLikeHasher()

	ref, branch := ctxt.Build(&Branch{
		children: Children{
			0x7: &Value{length: 55, key: key, value: v31},
			0xc: &Value{length: 55, value: common.Value{255}},
		},
	})

	handle := branch.GetWriteHandle()
	node := handle.Get().(*BranchNode)
	node.setEmbedded(0x7, true)
	node.hashes[0xc] = common.HashFromString("e7f1b1dc5bd6a8aa153134ddae4d2bf64a80ad1205355f385c5879a622a73612")
	handle.Release()

	hash, err := hasher.getHash(&ref, ctxt)
	if err != nil {
		t.Fatalf("error computing hash: %v", err)
	}
	want := "0f284164ed2106b827a49f8298c2fedc8b726c1fff3b574fba83fda47aa1fe8e"
	got := fmt.Sprintf("%x", hash)
	if got != want {
		t.Fatalf("unexpected hash\nwanted %v\n   got %v", want, got)
	}
}

func TestEthereumLikeHasher_BranchNode_RecomputesEmbeddedFlagsForHashInNodeMode(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContextWithConfig(t, ctrl, S5ArchiveConfig)

	// This test case reconstructs an issue encountered while computing
	// hashes for archives which are not storing embedded flags on disk.
	// In those cases, embedded flags in branch nodes have not been updated
	// if the hashes of the child nodes have been valid.

	key1 := hexToKey("c76547ce3912f8c25a9943819c2992169865dfd500bed5213c8a92ceff5db5e3")
	key2 := hexToKey("2968f9295ca3ab4960ae553a18f47567e56f2777ad762ee1d639421728926a37")

	val1 := common.Value{}
	val1[len(val1)-1] = 1

	dirtyHash := hashStatusDirty
	ref, branch := ctxt.Build(&Branch{
		children: Children{
			0x2: &Value{length: 55, key: key1, value: val1}, // the node and its hash are clean
			0x4: &Value{length: 55, key: key2, value: val1}, // the node and its hash are clean
		},
		hashStatus:       &dirtyHash,
		dirtyChildHashes: []int{0x2, 0x4}, // the branch's hashes are outdated
	})

	// The embedded mask should contain no 1 bits right now.
	view := branch.GetViewHandle()
	embeddedMask := int(view.Get().(*BranchNode).embeddedChildren)
	view.Release()
	if want, got := 0, embeddedMask; want != got {
		t.Errorf("nested nodes not identified as embedded, wanted %016b, got %016b", want, got)
	}

	hasher := makeEthereumLikeHasher()
	_, _, err := hasher.updateHashes(&ref, ctxt)
	if err != nil {
		t.Fatalf("failed to compute hash for node: %v", err)
	}

	// The embedded mask should indicate both value nodes as embedded.
	view = branch.GetViewHandle()
	embeddedMask = int(view.Get().(*BranchNode).embeddedChildren)
	view.Release()
	if want, got := (1<<2)|(1<<4), embeddedMask; want != got {
		t.Errorf("nested nodes not identified as embedded, wanted %016b, got %016b", want, got)
	}
}

// The other node types are tested as part of the overall state hash tests.

func TestEthereumLikeHasher_GetLowerBoundForEmptyNode(t *testing.T) {
	size, err := getLowerBoundForEncodedSizeEmpty(EmptyNode{}, 0, nil)
	if err != nil {
		t.Fatalf("failed to get lower bound for encoding: %v", err)
	}
	encoded, _ := encodeEmpty()
	if got, want := size, len(encoded); got != want {
		t.Fatalf("empty code size prediction is off, want %d, got %d", want, got)
	}
}

func TestEthereumLikeHasher_GetLowerBoundForAccountNode(t *testing.T) {
	tests := []*AccountNode{
		(&AccountNode{}),
		(&AccountNode{storage: NewNodeReference(BranchId(12))}),
		(&AccountNode{info: AccountInfo{Nonce: common.Nonce{1, 2, 3}}}),
		(&AccountNode{info: AccountInfo{Balance: common.Balance{1, 2, 3}}}),
		(&AccountNode{info: AccountInfo{CodeHash: common.Hash{1, 2, 3, 4}}}),
	}

	ctrl := gomock.NewController(t)

	nodesSource := NewMockNodeSource(ctrl)
	nodesSource.EXPECT().getConfig().AnyTimes().Return(S5LiveConfig)
	nodesSource.EXPECT().hashAddress(gomock.Any()).AnyTimes().Return(common.Hash{})

	for _, test := range tests {
		size, err := getLowerBoundForEncodedSize(test, 10000, nil)
		if err != nil {
			t.Fatalf("failed to get lower bound for encoding: %v", err)
		}
		encoded, err := encode(test, nodesSource, nil)
		if err != nil {
			t.Fatalf("failed to encode test value: %v", err)
		}
		if got, want := size, len(encoded); got > want {
			t.Errorf("invalid lower bound, encoded size %d, bound %d", want, got)
		}
	}
}

func TestEthereumLikeHasher_GetLowerBoundForBranchNode(t *testing.T) {
	ctrl := gomock.NewController(t)

	smallChild := NewNodeReference(ValueId(12)) // A node that can be encoded in less than 32 bytes
	bigChild := NewNodeReference(ValueId(14))   // A node that can requires more than 32 bytes

	one := common.Value{}
	one[len(one)-1] = 1
	smallValue := shared.MakeShared[Node](&ValueNode{pathLength: 4, value: one})
	bigValue := shared.MakeShared[Node](&ValueNode{pathLength: 64, value: one})

	nodeManager := NewMockNodeManager(ctrl)
	nodeManager.EXPECT().getViewAccess(smallChild).AnyTimes().DoAndReturn(func(NodeId) (shared.ReadHandle[Node], error) {
		return smallValue.GetReadHandle(), nil
	})
	nodeManager.EXPECT().getViewAccess(bigChild).AnyTimes().DoAndReturn(func(NodeId) (shared.ReadHandle[Node], error) {
		return bigValue.GetReadHandle(), nil
	})
	nodeManager.EXPECT().hashKey(gomock.Any()).AnyTimes().Return(common.Hash{})

	tests := []*BranchNode{
		(&BranchNode{}),
		(&BranchNode{children: [16]NodeReference{smallChild}}),
		(&BranchNode{children: [16]NodeReference{smallChild, smallChild}}),
		(&BranchNode{children: [16]NodeReference{bigChild}}),
	}

	for _, test := range tests {
		size, err := getLowerBoundForEncodedSize(test, 10000, nodeManager)
		if err != nil {
			t.Fatalf("failed to get lower bound for encoding: %v", err)
		}
		encoded, err := encode(test, nodeManager, nil)
		if err != nil {
			t.Fatalf("failed to encode test value: %v", err)
		}
		if got, want := size, len(encoded); got > want {
			t.Errorf("invalid lower bound, encoded size %d, bound %d, node %v", want, got, test)
		}
	}
}

func TestEthereumLikeHasher_GetLowerBoundForExtensionNode(t *testing.T) {
	ctrl := gomock.NewController(t)

	smallChild := NewNodeReference(ValueId(12)) // A node that can be encoded in less than 32 bytes
	bigChild := NewNodeReference(ValueId(14))   // A node that can requires more than 32 bytes

	one := common.Value{}
	one[len(one)-1] = 1
	smallValue := shared.MakeShared[Node](&ValueNode{pathLength: 4, value: one})
	bigValue := shared.MakeShared[Node](&ValueNode{pathLength: 64, value: one})

	nodeManager := NewMockNodeManager(ctrl)
	nodeManager.EXPECT().getViewAccess(RefTo(smallChild.Id())).AnyTimes().DoAndReturn(func(*NodeReference) (shared.ViewHandle[Node], error) {
		return smallValue.GetViewHandle(), nil
	})
	nodeManager.EXPECT().getViewAccess(RefTo(bigChild.Id())).AnyTimes().DoAndReturn(func(*NodeReference) (shared.ViewHandle[Node], error) {
		return bigValue.GetViewHandle(), nil
	})

	nodeManager.EXPECT().hashKey(gomock.Any()).AnyTimes().Return(common.Hash{})

	tests := []*ExtensionNode{
		(&ExtensionNode{next: smallChild}),
		(&ExtensionNode{next: bigChild}),
		(&ExtensionNode{path: CreatePathFromNibbles([]Nibble{1}), next: smallChild}),
		(&ExtensionNode{path: CreatePathFromNibbles([]Nibble{1}), next: bigChild}),
		(&ExtensionNode{path: CreatePathFromNibbles([]Nibble{1, 2}), next: smallChild}),
		(&ExtensionNode{path: CreatePathFromNibbles([]Nibble{1, 2}), next: bigChild}),
		(&ExtensionNode{path: CreatePathFromNibbles([]Nibble{1, 2, 3}), next: smallChild}),
		(&ExtensionNode{path: CreatePathFromNibbles([]Nibble{1, 2, 3}), next: bigChild}),
	}

	for _, test := range tests {
		size, err := getLowerBoundForEncodedSize(test, 10000, nodeManager)
		if err != nil {
			t.Fatalf("failed to get lower bound for encoding: %v", err)
		}
		encoded, err := encode(test, nodeManager, nil)
		if err != nil {
			t.Fatalf("failed to encode test value: %v", err)
		}
		if got, want := size, len(encoded); got > want {
			t.Errorf("invalid lower bound, encoded size %d, bound %d, node %v", want, got, test)
		}
	}
}

func TestEthereumLikeHasher_GetLowerBoundForValueNode(t *testing.T) {
	one := common.Value{}
	one[len(one)-1] = 1

	tests := []*ValueNode{
		(&ValueNode{}),
		(&ValueNode{pathLength: 1}),
		(&ValueNode{pathLength: 2}),
		(&ValueNode{pathLength: 64}),
		(&ValueNode{pathLength: 1, value: one}),
		(&ValueNode{pathLength: 2, value: one}),
		(&ValueNode{pathLength: 3, value: one}),
		(&ValueNode{pathLength: 64, value: one}),
		(&ValueNode{pathLength: 1, value: common.Value{1}}),
		(&ValueNode{pathLength: 64, value: common.Value{255}}),
	}

	ctrl := gomock.NewController(t)
	nodeManager := NewMockNodeManager(ctrl)
	nodeManager.EXPECT().hashKey(gomock.Any()).AnyTimes().Return(common.Hash{})

	for _, test := range tests {
		size, err := getLowerBoundForEncodedSize(test, 10000, nil)
		if err != nil {
			t.Fatalf("failed to get lower bound for encoding: %v", err)
		}
		encoded, err := encode(test, nodeManager, nil)
		if err != nil {
			t.Fatalf("failed to encode test value: %v", err)
		}
		if got, want := size, len(encoded); got > want {
			t.Errorf("invalid lower bound, encoded size %d, bound %d, node %v", want, got, test)
		}
	}
}

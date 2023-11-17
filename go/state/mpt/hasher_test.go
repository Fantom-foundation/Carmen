package mpt

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt/shared"
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

			id, node := ctxt.Build(&Extension{
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
			_, err := hasher.getHash(id, ctxt)
			if err != nil {
				t.Fatalf("error computing hash: %v", err)
			}

			// The dirty flag is cleared.
			handle := node.GetReadHandle()
			defer handle.Release()
			if !handle.Get().(*ExtensionNode).hashDirty {
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

			id, node := ctxt.Build(&Extension{
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

			// The node is updated while being hashed.
			ctxt.EXPECT().updateHash(id, gomock.Any())

			hasher := algorithm.createHasher()
			_, _, err := hasher.updateHashes(id, ctxt)
			if err != nil {
				t.Fatalf("error computing hash: %v", err)
			}

			// The dirty flag is cleared.
			handle := node.GetReadHandle()
			defer handle.Release()
			if handle.Get().(*ExtensionNode).hashDirty {
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

			id, node := ctxt.Build(&Branch{
				children: Children{
					0x7: &Account{},
					0xd: &Account{},
				},
				hashDirty: true,
				dirty:     []int{0x7, 0xd},
			})

			hasher := algorithm.createHasher()
			_, err := hasher.getHash(id, ctxt)
			if err != nil {
				t.Fatalf("error computing hash: %v", err)
			}

			// The dirty flag is not touched.
			handle := node.GetReadHandle()
			defer handle.Release()
			if !handle.Get().(*BranchNode).hashDirty {
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

			id, node := ctxt.Build(&Branch{
				children: Children{
					0x7: &Account{},
					0xd: &Account{},
				},
				hashDirty: true,
				dirty:     []int{0x7, 0xd},
			})

			// The node is updated while being hashed.
			ctxt.EXPECT().updateHash(id, gomock.Any())
			ctxt.EXPECT().hashAddress(gomock.Any()).MaxTimes(2)

			hasher := algorithm.createHasher()
			_, _, err := hasher.updateHashes(id, ctxt)
			if err != nil {
				t.Fatalf("error computing hash: %v", err)
			}

			// The dirty flag is cleared.
			handle := node.GetReadHandle()
			defer handle.Release()
			if handle.Get().(*BranchNode).hashDirty {
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

			id, node := ctxt.Build(&Branch{
				children: Children{
					0x7: &Account{},
					0xd: &Account{},
				},
				hashDirty: true,
				dirty:     []int{1, 2, 3}, // < all empty children
			})

			// the node is not marked to be modified

			hasher := algorithm.createHasher()
			_, _, err := hasher.updateHashes(id, ctxt)
			if err != nil {
				t.Fatalf("error computing hash: %v", err)
			}

			// The dirty flag is cleared.
			handle := node.GetReadHandle()
			defer handle.Release()
			if handle.Get().(*BranchNode).hashDirty {
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

			id, node := ctxt.Build(&Account{
				hashDirty:        true,
				storageHashDirty: true,
			})

			ctxt.EXPECT().hashAddress(gomock.Any()).MaxTimes(1)

			hasher := algorithm.createHasher()
			_, err := hasher.getHash(id, ctxt)
			if err != nil {
				t.Fatalf("error computing hash: %v", err)
			}

			// The dirty flag is not changed.
			handle := node.GetReadHandle()
			defer handle.Release()
			if !handle.Get().(*AccountNode).hashDirty {
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

			id, node := ctxt.Build(&Account{
				hashDirty:        true,
				storageHashDirty: true,
			})

			// The node is updated while being hashed.
			ctxt.EXPECT().updateHash(id, gomock.Any())
			ctxt.EXPECT().hashAddress(gomock.Any()).MaxTimes(1)

			hasher := algorithm.createHasher()
			_, _, err := hasher.updateHashes(id, ctxt)
			if err != nil {
				t.Fatalf("error computing hash: %v", err)
			}

			// The dirty flag is cleared.
			handle := node.GetReadHandle()
			defer handle.Release()
			if handle.Get().(*AccountNode).hashDirty {
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

	hash, err := hasher.getHash(EmptyId(), nodes)
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
	id, node := ctxt.Build(&Extension{
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

	hash, err := hasher.getHash(id, ctxt)
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

	id, branch := ctxt.Build(&Branch{
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

	hash, err := hasher.getHash(id, ctxt)
	if err != nil {
		t.Fatalf("error computing hash: %v", err)
	}
	want := "0f284164ed2106b827a49f8298c2fedc8b726c1fff3b574fba83fda47aa1fe8e"
	got := fmt.Sprintf("%x", hash)
	if got != want {
		t.Fatalf("unexpected hash\nwanted %v\n   got %v", want, got)
	}
}

// The other node types are tested as part of the overall state hash tests.

func TestEthereumLikeHasher_GetLowerBoundForEmptyNode(t *testing.T) {
	size, err := getLowerBoundForEncodedSizeEmpty(EmptyNode{}, 0, nil)
	if err != nil {
		t.Fatalf("failed to get lower bound for encoding: %v", err)
	}
	hasher := makeEthereumLikeHasher().(*ethHasher)
	encoded, _ := hasher.encodeEmpty()
	if got, want := size, len(encoded); got != want {
		t.Fatalf("empty code size prediction is off, want %d, got %d", want, got)
	}
}

func TestEthereumLikeHasher_GetLowerBoundForAccountNode(t *testing.T) {
	tests := []*AccountNode{
		(&AccountNode{}),
		(&AccountNode{storage: BranchId(12)}),
		(&AccountNode{info: AccountInfo{Nonce: common.Nonce{1, 2, 3}}}),
		(&AccountNode{info: AccountInfo{Balance: common.Balance{1, 2, 3}}}),
		(&AccountNode{info: AccountInfo{CodeHash: common.Hash{1, 2, 3, 4}}}),
	}

	ctrl := gomock.NewController(t)

	nodesSource := NewMockNodeSource(ctrl)
	nodesSource.EXPECT().getConfig().AnyTimes().Return(S5LiveConfig)
	nodesSource.EXPECT().hashAddress(gomock.Any()).AnyTimes().Return(common.Hash{})

	hasher := makeEthereumLikeHasher().(*ethHasher)
	for _, test := range tests {
		size, err := getLowerBoundForEncodedSize(test, 10000, nil)
		if err != nil {
			t.Fatalf("failed to get lower bound for encoding: %v", err)
		}
		encoded, err := hasher.encode(AccountId(1), test, shared.HashHandle[Node]{}, nil, nodesSource, EmptyPath(), nil)
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

	smallChild := ValueId(12) // A node that can be encoded in less than 32 bytes
	bigChild := ValueId(14)   // A node that can requires more than 32 bytes

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
		(&BranchNode{children: [16]NodeId{smallChild}}),
		(&BranchNode{children: [16]NodeId{smallChild, smallChild}}),
		(&BranchNode{children: [16]NodeId{bigChild}}),
	}

	hasher := makeEthereumLikeHasher().(*ethHasher)
	for _, test := range tests {
		size, err := getLowerBoundForEncodedSize(test, 10000, nodeManager)
		if err != nil {
			t.Fatalf("failed to get lower bound for encoding: %v", err)
		}
		encoded, err := hasher.encode(BranchId(1), test, shared.HashHandle[Node]{}, nil, nodeManager, EmptyPath(), nil)
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

	smallChild := ValueId(12) // A node that can be encoded in less than 32 bytes
	bigChild := ValueId(14)   // A node that can requires more than 32 bytes

	one := common.Value{}
	one[len(one)-1] = 1
	smallValue := shared.MakeShared[Node](&ValueNode{pathLength: 4, value: one})
	bigValue := shared.MakeShared[Node](&ValueNode{pathLength: 64, value: one})

	nodeManager := NewMockNodeManager(ctrl)
	nodeManager.EXPECT().getViewAccess(smallChild).AnyTimes().DoAndReturn(func(NodeId) (shared.ViewHandle[Node], error) {
		return smallValue.GetViewHandle(), nil
	})
	nodeManager.EXPECT().getViewAccess(bigChild).AnyTimes().DoAndReturn(func(NodeId) (shared.ViewHandle[Node], error) {
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

	hasher := makeEthereumLikeHasher().(*ethHasher)
	for _, test := range tests {
		size, err := getLowerBoundForEncodedSize(test, 10000, nodeManager)
		if err != nil {
			t.Fatalf("failed to get lower bound for encoding: %v", err)
		}
		encoded, err := hasher.encode(ExtensionId(1), test, shared.HashHandle[Node]{}, nil, nodeManager, EmptyPath(), nil)
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

	hasher := makeEthereumLikeHasher().(*ethHasher)
	for _, test := range tests {
		size, err := getLowerBoundForEncodedSize(test, 10000, nil)
		if err != nil {
			t.Fatalf("failed to get lower bound for encoding: %v", err)
		}
		encoded, err := hasher.encode(ValueId(1), test, shared.HashHandle[Node]{}, nil, nodeManager, EmptyPath(), nil)
		if err != nil {
			t.Fatalf("failed to encode test value: %v", err)
		}
		if got, want := size, len(encoded); got > want {
			t.Errorf("invalid lower bound, encoded size %d, bound %d, node %v", want, got, test)
		}
	}
}

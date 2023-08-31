package mpt

/*
var emptyNodeHash = keccak256(rlp.Encode(rlp.String{}))

func TestMptHasher_EmptyNode(t *testing.T) {
	hasher := MptHasher{}
	hash, err := hasher.GetHash(EmptyNode{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to hash empty node: %v", err)
	}

	if got, want := hash, emptyNodeHash; got != want {
		t.Errorf("invalid hash of empty node, wanted %v, got %v", got, want)
	}
}

func TestMptHasher_ExtensionNode_KnownHash(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)

	// This test case reconstructs an issue encountered while hashing the
	// state tree of block 25399 of the Fantom main-net.

	nextId, _ := ctxt.Build(
		&Branch{
			0x7: &Account{},
			0xd: &Account{},
		},
	)

	hasher := MptHasher{}
	node := &ExtensionNode{
		path: CreatePathFromNibbles([]Nibble{0x8, 0xe, 0xf}),
		next: nextId,
	}

	hashSource := NewMockHashSource(ctrl)
	hashSource.EXPECT().getHashFor(nextId).Return(common.HashFromString("43085a287ea060fa9089bd4797d2471c6d57136b666a314e6a789735251317d4"), nil)

	hash, err := hasher.GetHash(node, ctxt, hashSource)
	if err != nil {
		t.Fatalf("error computing hash: %v", err)
	}
	want := "ebf7c28d351f2ec8a26d0e40049ddf406117e0468a49af0d261bb74d88e17560"
	got := fmt.Sprintf("%x", hash)
	if got != want {
		t.Fatalf("unexpected hash\nwanted %v\n   got %v", want, got)
	}
}

func TestMptHasher_BranchNode_KnownHash_EmbeddedNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctxt := newNodeContext(t, ctrl)
	ctxt.EXPECT().hashKey(gomock.Any()).AnyTimes().DoAndReturn(func(key common.Key) (common.Hash, error) {
		return keccak256(key[:]), nil
	})

	ctxt.EXPECT().hashKey(gomock.Any()).AnyTimes().DoAndReturn(func(k common.Key) common.Hash {
		return keccak256(k[:])
	})

	// This test case reconstructs an issue encountered while hashing the
	// state tree of block 652606 of the Fantom main-net.

	v31 := common.Value{}
	v31[len(v31)-1] = 31

	var key common.Key
	data, _ := hex.DecodeString("c1bb1e5ab6acf1bef1a125f3d60e0941b9a8624288ffd67282484c25519f9e65")
	copy(key[:], data)

	child1Id, _ := ctxt.Build(&ValueWithLength{length: 55, key: key, value: v31})
	child2Id, _ := ctxt.Build(&ValueWithLength{length: 55, value: common.Value{255}})

	hasher := MptHasher{}

	node := &BranchNode{}
	node.children[0x7] = child1Id
	node.children[0xc] = child2Id

	hashSource := NewMockHashSource(ctrl)
	hashSource.EXPECT().getHashFor(child2Id).Return(common.HashFromString("e7f1b1dc5bd6a8aa153134ddae4d2bf64a80ad1205355f385c5879a622a73612"), nil)

	hash, err := hasher.GetHash(node, ctxt, hashSource)
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

func TestMptHasher_GetLowerBoundForEmptyNode(t *testing.T) {
	size, err := getLowerBoundForEncodedSizeEmpty(EmptyNode{}, 0, nil)
	if err != nil {
		t.Fatalf("failed to get lower bound for encoding: %v", err)
	}
	encoded, _ := encodeEmpty(EmptyNode{}, nil, nil)
	if got, want := size, len(encoded); got != want {
		t.Fatalf("empty code size prediction is off, want %d, got %d", want, got)
	}
}

func TestMptHasher_GetLowerBoundForAccountNode(t *testing.T) {
	tests := []*AccountNode{
		(&AccountNode{}),
		(&AccountNode{storage: BranchId(12)}),
		(&AccountNode{info: AccountInfo{Nonce: common.Nonce{1, 2, 3}}}),
		(&AccountNode{info: AccountInfo{Balance: common.Balance{1, 2, 3}}}),
		(&AccountNode{info: AccountInfo{CodeHash: common.Hash{1, 2, 3, 4}}}),
	}

	ctrl := gomock.NewController(t)
	hashSource := NewMockHashSource(ctrl)
	hashSource.EXPECT().getHashFor(gomock.Any()).AnyTimes().Return(common.Hash{}, nil)

	nodesSource := NewMockNodeSource(ctrl)
	nodesSource.EXPECT().getConfig().AnyTimes().Return(S5Config)
	nodesSource.EXPECT().hashAddress(gomock.Any()).AnyTimes().Return(common.Hash{})

	for _, test := range tests {
		size, err := getLowerBoundForEncodedSize(test, 10000, nil)
		if err != nil {
			t.Fatalf("failed to get lower bound for encoding: %v", err)
		}
		encoded, err := encode(test, nodesSource, hashSource)
		if err != nil {
			t.Fatalf("failed to encode test value: %v", err)
		}
		if got, want := size, len(encoded); got > want {
			t.Errorf("invalid lower bound, encoded size %d, bound %d", want, got)
		}
	}
}

func TestMptHasher_GetLowerBoundForBranchNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	hashSource := NewMockHashSource(ctrl)
	hashSource.EXPECT().getHashFor(gomock.Any()).AnyTimes().Return(common.Hash{}, nil)

	smallChild := ValueId(12) // A node that can be encoded in less than 32 bytes
	bigChild := ValueId(14)   // A node that can requires more than 32 bytes

	one := common.Value{}
	one[len(one)-1] = 1
	smallValue := shared.MakeShared[Node](&ValueNode{pathLength: 4, value: one})
	bigValue := shared.MakeShared[Node](&ValueNode{pathLength: 64, value: one})

	nodeSource := NewMockNodeSource(ctrl)
	nodeSource.EXPECT().getNode(smallChild).AnyTimes().DoAndReturn(func(NodeId) (shared.ReadHandle[Node], error) {
		return smallValue.GetReadHandle(), nil
	})
	nodeSource.EXPECT().getNode(bigChild).AnyTimes().DoAndReturn(func(NodeId) (shared.ReadHandle[Node], error) {
		return bigValue.GetReadHandle(), nil
	})
	nodeSource.EXPECT().hashKey(gomock.Any()).AnyTimes().Return(common.Hash{})

	tests := []*BranchNode{
		(&BranchNode{}),
		(&BranchNode{children: [16]NodeId{smallChild}}),
		(&BranchNode{children: [16]NodeId{smallChild, smallChild}}),
		(&BranchNode{children: [16]NodeId{bigChild}}),
	}

	for _, test := range tests {
		size, err := getLowerBoundForEncodedSize(test, 10000, nodeSource)
		if err != nil {
			t.Fatalf("failed to get lower bound for encoding: %v", err)
		}
		encoded, err := encode(test, nodeSource, hashSource)
		if err != nil {
			t.Fatalf("failed to encode test value: %v", err)
		}
		if got, want := size, len(encoded); got > want {
			t.Errorf("invalid lower bound, encoded size %d, bound %d, node %v", want, got, test)
		}
	}
}

func TestMptHasher_GetLowerBoundForExtensionNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	hashSource := NewMockHashSource(ctrl)
	hashSource.EXPECT().getHashFor(gomock.Any()).AnyTimes().Return(common.Hash{}, nil)

	smallChild := ValueId(12) // A node that can be encoded in less than 32 bytes
	bigChild := ValueId(14)   // A node that can requires more than 32 bytes

	one := common.Value{}
	one[len(one)-1] = 1
	smallValue := shared.MakeShared[Node](&ValueNode{pathLength: 4, value: one})
	bigValue := shared.MakeShared[Node](&ValueNode{pathLength: 64, value: one})

	nodeSource := NewMockNodeSource(ctrl)
	nodeSource.EXPECT().getNode(smallChild).AnyTimes().DoAndReturn(func(NodeId) (shared.ReadHandle[Node], error) {
		return smallValue.GetReadHandle(), nil
	})
	nodeSource.EXPECT().getNode(bigChild).AnyTimes().DoAndReturn(func(NodeId) (shared.ReadHandle[Node], error) {
		return bigValue.GetReadHandle(), nil
	})

	nodeSource.EXPECT().hashKey(gomock.Any()).AnyTimes().Return(common.Hash{})

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
		size, err := getLowerBoundForEncodedSize(test, 10000, nodeSource)
		if err != nil {
			t.Fatalf("failed to get lower bound for encoding: %v", err)
		}
		encoded, err := encode(test, nodeSource, hashSource)
		if err != nil {
			t.Fatalf("failed to encode test value: %v", err)
		}
		if got, want := size, len(encoded); got > want {
			t.Errorf("invalid lower bound, encoded size %d, bound %d, node %v", want, got, test)
		}
	}
}

func TestMptHasher_GetLowerBoundForValueNode(t *testing.T) {
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
	nodeSource := NewMockNodeSource(ctrl)
	nodeSource.EXPECT().hashKey(gomock.Any()).AnyTimes().Return(common.Hash{})

	for _, test := range tests {
		size, err := getLowerBoundForEncodedSize(test, 10000, nil)
		if err != nil {
			t.Fatalf("failed to get lower bound for encoding: %v", err)
		}
		encoded, err := encode(test, nodeSource, nil)
		if err != nil {
			t.Fatalf("failed to encode test value: %v", err)
		}
		if got, want := size, len(encoded); got > want {
			t.Errorf("invalid lower bound, encoded size %d, bound %d, node %v", want, got, test)
		}
	}
}
*/
